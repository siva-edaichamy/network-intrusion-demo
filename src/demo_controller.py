"""
Demo Controller
Manages the lifecycle of the entire demo (simulators + consumer)
FIXED: Handle prediction result correctly
"""

import time
from typing import Dict, Any, Callable
from datetime import datetime
from src.device_simulator import SimulationManager
from src.consumer_service import ConsumerService


class DemoController:
    """Controls the entire demo lifecycle"""

    def __init__(self, on_event: Callable = None):
        """
        Initialize demo controller

        Args:
            on_event: Callback for demo events (message_sent, prediction, etc.)
        """
        self.on_event = on_event

        # Create simulation manager
        self.simulation_manager = SimulationManager(
            on_message_sent=self._handle_message_sent
        )

        # Create consumer service
        self.consumer_service = ConsumerService(on_prediction=self._handle_prediction)

        self.status = "stopped"  # stopped, starting, running, stopping
        self.start_time = None

        # Aggregated stats
        self.total_messages_sent = 0
        self.total_predictions = 0
        self.prediction_distribution = {}

    def _handle_message_sent(self, event: Dict[str, Any]):
        """Handle message sent event from devices"""
        self.total_messages_sent += 1

        if self.on_event:
            self.on_event("message_sent", event)

    def _handle_prediction(self, result: Dict[str, Any]):
        """Handle prediction result from consumer"""
        self.total_predictions += 1

        # Extract prediction value - it might be nested in the result dict
        prediction = result.get("prediction", "unknown")

        # Handle case where prediction is a dict (shouldn't happen, but be defensive)
        if isinstance(prediction, dict):
            prediction = prediction.get("value", "unknown")

        # Ensure prediction is a string
        prediction = str(prediction)

        # Track prediction distribution
        self.prediction_distribution[prediction] = (
            self.prediction_distribution.get(prediction, 0) + 1
        )

        if self.on_event:
            self.on_event("prediction", result)

    def start(self) -> Dict[str, Any]:
        """Start the demo"""
        try:
            if self.status == "running":
                return {"success": False, "message": "Demo already running"}

            self.status = "starting"
            self.start_time = datetime.now()

            # Reset stats
            self.total_messages_sent = 0
            self.total_predictions = 0
            self.prediction_distribution = {}

            # Start consumer first
            print("Starting demo: consumer service...")
            self.consumer_service.start()
            time.sleep(2)  # Give consumer time to initialize

            # Start simulators
            print("Starting device simulators...")
            self.simulation_manager.start()

            self.status = "running"

            print("Demo started successfully!")

            return {
                "success": True,
                "message": "Demo started successfully",
                "start_time": self.start_time.isoformat(),
            }

        except Exception as e:
            self.status = "stopped"
            return {"success": False, "message": f"Failed to start demo: {str(e)}"}

    def stop(self) -> Dict[str, Any]:
        """Stop the demo"""
        try:
            if self.status == "stopped":
                return {"success": False, "message": "Demo not running"}

            self.status = "stopping"

            print("Stopping demo...")

            # Stop simulators first
            print("Stopping device simulators...")
            self.simulation_manager.stop()

            # Give time for queue to drain
            time.sleep(2)

            # Stop consumer
            print("Stopping consumer service...")
            self.consumer_service.stop()

            self.status = "stopped"

            print("Demo stopped successfully!")

            return {"success": True, "message": "Demo stopped successfully"}

        except Exception as e:
            self.status = "stopped"
            return {"success": False, "message": f"Error stopping demo: {str(e)}"}

    def pause(self) -> Dict[str, Any]:
        """Pause the demo (stop simulators but keep consumer running)"""
        try:
            if self.status != "running":
                return {"success": False, "message": "Demo not running"}

            print("Pausing demo...")
            self.simulation_manager.stop()

            return {
                "success": True,
                "message": "Demo paused (simulators stopped, consumer still running)",
            }

        except Exception as e:
            return {"success": False, "message": f"Error pausing demo: {str(e)}"}

    def resume(self) -> Dict[str, Any]:
        """Resume the demo (restart simulators)"""
        try:
            if self.status != "running":
                return {"success": False, "message": "Demo not running"}

            print("Resuming demo...")
            self.simulation_manager.start()

            return {"success": True, "message": "Demo resumed"}

        except Exception as e:
            return {"success": False, "message": f"Error resuming demo: {str(e)}"}

    def get_status(self) -> Dict[str, Any]:
        """Get current status and statistics"""

        # Get device stats
        device_stats = self.simulation_manager.get_stats()

        # Get consumer stats
        consumer_stats = self.consumer_service.get_stats()

        # Calculate uptime
        uptime_seconds = 0
        if self.start_time:
            uptime_seconds = (datetime.now() - self.start_time).total_seconds()

        return {
            "status": self.status,
            "uptime_seconds": round(uptime_seconds, 2),
            "start_time": self.start_time.isoformat() if self.start_time else None,
            # Simulation stats
            "num_devices": device_stats["num_devices"],
            "total_messages_sent": device_stats["total_messages"],
            "devices": device_stats["devices"],
            # Consumer stats
            "total_predictions": consumer_stats.get("predictions_made", consumer_stats.get("prediction_count", 0)),
            "consumer_errors": consumer_stats.get("errors", consumer_stats.get("error_count", 0)),
            "avg_inference_time_ms": consumer_stats.get("avg_inference_time_ms", 0),
            "messages_per_sec": consumer_stats.get("messages_per_sec", 0),
            # Classification counters
            "normal_connections": consumer_stats.get("normal_connections", 0),
            "attack_connections": consumer_stats.get("attack_connections", 0),
            "unknown_predictions": consumer_stats.get("unknown_predictions", 0),
            "normal_percent": consumer_stats.get("normal_percent", 0),
            "attack_percent": consumer_stats.get("attack_percent", 0),
            # Prediction distribution
            "prediction_distribution": self.prediction_distribution,
        }

    def reset(self, hard: bool = False) -> Dict[str, Any]:
        """
        Reset the demo

        Args:
            hard: If True, also clear Greenplum data
        """
        try:
            # Stop if running
            if self.status == "running":
                self.stop()

            # Reset stats
            self.total_messages_sent = 0
            self.total_predictions = 0
            self.prediction_distribution = {}
            self.start_time = None
            
            # Reset consumer service counters
            if hasattr(self, 'consumer_service') and self.consumer_service:
                self.consumer_service.normal_count = 0
                self.consumer_service.attack_count = 0
                self.consumer_service.unknown_count = 0
                self.consumer_service.prediction_count = 0
                self.consumer_service.error_count = 0
                self.consumer_service.inference_times = []

            message = "Demo reset successfully (soft reset - stats cleared)"

            # Hard reset - clear Greenplum data
            if hard:
                try:
                    import psycopg2
                    from src.config_manager import get_config

                    config = get_config()
                    gp_config = config.get_greenplum_config()

                    conn = psycopg2.connect(
                        host=gp_config["host"],
                        port=gp_config["port"],
                        database=gp_config["database"],
                        user=gp_config["user"],
                        password=gp_config["password"],
                    )

                    cursor = conn.cursor()
                    cursor.execute(f"TRUNCATE TABLE {gp_config['table']}")
                    conn.commit()
                    cursor.close()
                    conn.close()

                    message = (
                        "Demo reset successfully (hard reset - Greenplum data cleared)"
                    )

                except Exception as e:
                    message = (
                        f"Demo reset (soft), but failed to clear Greenplum: {str(e)}"
                    )

            return {"success": True, "message": message}

        except Exception as e:
            return {"success": False, "message": f"Error resetting demo: {str(e)}"}
