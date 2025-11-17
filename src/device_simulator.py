"""
device_simulator.py
Simulates multiple devices sending KDD data to RabbitMQ
"""

import csv
import time
import pika
import threading
from datetime import datetime
from typing import Dict, Any, Callable
from src.config_manager import get_config


class DeviceSimulator:
    """Simulates a single device sending network connection data"""

    def __init__(
        self,
        device_id: str,
        device_name: str,
        dataset_path: str,
        on_message_sent: Callable = None,
    ):
        self.device_id = device_id
        self.device_name = device_name
        self.dataset_path = dataset_path
        self.config = get_config()
        self.rmq_config = self.config.get_rabbitmq_config()
        self.on_message_sent = on_message_sent

        self.running = False
        self.thread = None
        self.message_count = 0
        self.connection = None
        self.channel = None
        self.exchange_name = "network_intrusion_fanout"

    def connect(self) -> bool:
        """Connect to RabbitMQ"""
        try:
            credentials = pika.PlainCredentials(
                self.rmq_config["username"], self.rmq_config["password"]
            )
            parameters = pika.ConnectionParameters(
                host=self.rmq_config["host"],
                port=self.rmq_config["port"],
                virtual_host=self.rmq_config["vhost"],
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300,
            )
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()

            # print(f"[{self.device_name}] Connected to RabbitMQ")
            return True

        except Exception as e:
            print(f"[{self.device_name}] Connection error: {str(e)}")
            return False

    def disconnect(self):
        """Close RabbitMQ connection"""
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
        except Exception as e:
            print(f"[{self.device_name}] Disconnect error: {str(e)}")

    def generate_unique_id(self) -> int:
        """Generate unique ID based on timestamp and device"""
        timestamp = int(time.time() * 1000)
        device_num = int(self.device_id.split("-")[-1])
        return timestamp * 100 + device_num

    def publish_message(self, message: str) -> bool:
        """Publish message to fan-out exchange"""
        try:
            if not self.channel:
                if not self.connect():
                    return False

            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key="",
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                ),
            )

            self.message_count += 1
            return True

        except Exception as e:
            print(f"[{self.device_name}] Publish error: {str(e)}")
            self.disconnect()
            return False

    def run(self, messages_per_second: float, loop_dataset: bool = True):
        """Start sending messages"""
        self.running = True
        delay = 1.0 / messages_per_second if messages_per_second > 0 else 0.5

        print(
            f"[{self.device_name}] Starting simulation (rate: {messages_per_second:.2f} msg/sec)"
        )

        if not self.connect():
            print(f"[{self.device_name}] Failed to connect to RabbitMQ")
            return

        try:
            while self.running:
                with open(self.dataset_path, "r") as file:
                    reader = csv.DictReader(file)

                    for row in reader:
                        if not self.running:
                            break

                        unique_id = self.generate_unique_id()
                        message_parts = [str(unique_id)]

                        field_order = [
                            "duration",
                            "protocol_type",
                            "service",
                            "flag",
                            "src_bytes",
                            "dst_bytes",
                            "land",
                            "wrong_fragment",
                            "urgent",
                            "hot",
                            "num_failed_logins",
                            "logged_in",
                            "num_compromised",
                            "root_shell",
                            "su_attempted",
                            "num_root",
                            "num_file_creations",
                            "num_shells",
                            "num_access_files",
                            "num_outbound_cmds",
                            "is_host_login",
                            "is_guest_login",
                            "count",
                            "srv_count",
                            "serror_rate",
                            "srv_serror_rate",
                            "rerror_rate",
                            "srv_rerror_rate",
                            "same_srv_rate",
                            "diff_srv_rate",
                            "srv_diff_host_rate",
                            "dst_host_count",
                            "dst_host_srv_count",
                            "dst_host_same_srv_rate",
                            "dst_host_diff_srv_rate",
                            "dst_host_same_src_port_rate",
                            "dst_host_srv_diff_host_rate",
                            "dst_host_serror_rate",
                            "dst_host_srv_serror_rate",
                            "dst_host_rerror_rate",
                            "dst_host_srv_rerror_rate",
                            "intrusion_type",
                        ]

                        for field in field_order:
                            value = row.get(field, "")
                            message_parts.append(str(value))

                        message = ",".join(message_parts)
                        success = self.publish_message(message)

                        if success and self.on_message_sent:
                            self.on_message_sent(
                                {
                                    "device_id": self.device_id,
                                    "device_name": self.device_name,
                                    "message_count": self.message_count,
                                    "intrusion_type": row.get(
                                        "intrusion_type", "unknown"
                                    ),
                                    "timestamp": datetime.now().isoformat(),
                                }
                            )

                        time.sleep(delay)

                if not loop_dataset:
                    break

        except Exception as e:
            print(f"[{self.device_name}] Error during simulation: {str(e)}")

        finally:
            self.disconnect()
            print(
                f"[{self.device_name}] Simulation stopped (sent {self.message_count} messages)"
            )

    def start(self, messages_per_second: float, loop_dataset: bool = True):
        """Start simulation in separate thread"""
        if self.running:
            print(f"[{self.device_name}] Already running")
            return

        self.thread = threading.Thread(
            target=self.run, args=(messages_per_second, loop_dataset), daemon=True
        )
        self.thread.start()

    def stop(self):
        """Stop simulation"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)


class SimulationManager:
    """Manages multiple device simulators"""

    def __init__(self, on_message_sent: Callable = None):
        self.config = get_config()
        self.sim_config = self.config.get_simulation_config()
        self.dataset_path = self.config.get_simulation_dataset_path()
        self.on_message_sent = on_message_sent

        self.devices = []
        self.running = False

        device_names = self.sim_config["device_names"]
        num_devices = self.sim_config["num_devices"]

        for i in range(num_devices):
            device_id = f"device-{i}"
            device_name = device_names[i] if i < len(device_names) else f"Device-{i}"

            device = DeviceSimulator(
                device_id=device_id,
                device_name=device_name,
                dataset_path=self.dataset_path,
                on_message_sent=on_message_sent,
            )

            self.devices.append(device)

    def start(self):
        """Start all device simulators"""
        if self.running:
            print("Simulation already running")
            return

        self.running = True

        total_rate = self.sim_config["messages_per_second"]
        num_devices = len(self.devices)
        per_device_rate = total_rate / num_devices

        print(
            f"Starting {num_devices} devices at {total_rate} msg/sec total ({per_device_rate:.2f} each)"
        )

        for device in self.devices:
            device.start(
                messages_per_second=per_device_rate,
                loop_dataset=self.sim_config["loop_dataset"],
            )

    def stop(self):
        """Stop all device simulators"""
        if not self.running:
            return

        self.running = False

        print("Stopping all devices...")
        for device in self.devices:
            device.stop()

        print("All devices stopped")

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics from all devices"""
        total_messages = sum(d.message_count for d in self.devices)

        return {
            "total_messages": total_messages,
            "num_devices": len(self.devices),
            "devices": [
                {
                    "id": d.device_id,
                    "name": d.device_name,
                    "message_count": d.message_count,
                    "running": d.running,
                }
                for d in self.devices
            ],
        }
