"""
Flask Web Application - COMPLETE INTEGRATED VERSION
Provides Control Panel and Demo Dashboard interfaces
Includes new API endpoints for the rebuilt dashboard
FIXED: Status endpoint error handling and consumer graceful shutdown
"""

from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
from flask_socketio import SocketIO, emit
import os
import sys
import pika
import psycopg2
import requests
import urllib.parse
from requests.auth import HTTPBasicAuth


# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.config_manager import get_config
from src.preflight_checker import run_preflight_checks
from src.setup_manager import SetupManager
from src.demo_controller import DemoController
from src.gemfire_client import GemfireClient

# Initialize Flask app
app = Flask(__name__)
app.config["SECRET_KEY"] = "tanzu-demo-secret-key-change-in-production"
CORS(app)

# Initialize SocketIO
socketio = SocketIO(app, cors_allowed_origins="*")

# Initialize managers
config = get_config()
setup_manager = SetupManager()
demo_controller = None  # Will be initialized when starting demo


def broadcast_event(event_type: str, data: dict):
    """Broadcast event to all connected clients"""
    socketio.emit("demo_event", {"type": event_type, "data": data})


# ===== Routes =====


@app.route("/")
def index():
    """Redirect to control panel"""
    return render_template("control_panel.html")


@app.route("/control")
def control_panel():
    """Control Panel - Setup and management"""
    return render_template("control_panel.html")


@app.route("/demo")
def demo_dashboard():
    """Demo Dashboard - Presentation view"""
    return render_template("demo_dashboard.html")


# ===== API Endpoints =====


@app.route("/api/config", methods=["GET"])
def get_config_api():
    """Get current configuration"""
    try:
        is_valid, errors = config.validate()

        return jsonify(
            {
                "success": True,
                "config": {
                    "rabbitmq": {
                        "host": config.get("RABBITMQ_HOST"),
                        "port": config.get("RABBITMQ_PORT"),
                    },
                    "greenplum": {
                        "host": config.get("GREENPLUM_HOST"),
                        "database": config.get("GREENPLUM_DATABASE"),
                        "table": config.get("GREENPLUM_TABLE"),
                    },
                    "gemfire": {
                        "public_ip": config.get("GEMFIRE_PUBLIC_IP"),
                        "model_region": config.get("GEMFIRE_MODEL_REGION"),
                    },
                    "dataset_path": config.get_dataset_path(),
                },
                "valid": is_valid,
                "errors": errors,
            }
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/preflight/run", methods=["POST"])
def run_preflight_api():
    """Run all pre-flight checks"""
    try:
        summary = run_preflight_checks()
        return jsonify({"success": True, "summary": summary})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/setup/run", methods=["POST"])
def run_setup():
    """Run all setup operations"""
    try:
        results = setup_manager.run_all_setup()

        # Convert SetupResult objects to dictionaries
        serializable_results = {}
        for key, result in results.items():
            if hasattr(result, "to_dict"):
                serializable_results[key] = result.to_dict()
            else:
                serializable_results[key] = result

        return jsonify({"success": True, "results": serializable_results})
    except Exception as e:
        import traceback

        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/setup/status", methods=["GET"])
def get_setup_status():
    """Get current setup status"""
    try:
        status = setup_manager.get_setup_status()
        return jsonify({"success": True, "status": status})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ===== NEW DASHBOARD API ENDPOINTS =====


@app.route("/api/stats", methods=["GET"])
def get_stats():
    """
    Get current statistics for all components
    Used by the new dashboard for real-time updates
    """
    try:
        device_counters = get_device_counters()
        rabbitmq_stats = get_rabbitmq_stats()
        gemfire_stats = get_gemfire_stats()
        greenplum_count = get_greenplum_row_count()
        is_running = demo_controller.status == "running" if demo_controller else False
        
        # Get demo status which includes normal/attack connections
        demo_status = demo_controller.get_status() if demo_controller else {}
        

        stats = {
            "devices": device_counters,
            "rabbitmq": {
                "exchange_published": rabbitmq_stats.get(
                    "exchange_messages_published", 0
                ),
                "gpss_processed": rabbitmq_stats.get("gpss_messages_processed", 0),
                "gemfire_processed": rabbitmq_stats.get(
                    "gemfire_messages_processed", 0
                ),
            },
            "gemfire": {
                **gemfire_stats,
                "normal_connections": demo_status.get("normal_connections", 0),
                "attack_connections": demo_status.get("attack_connections", 0),
                "unknown_predictions": demo_status.get("unknown_predictions", 0),
            },
            "greenplum": greenplum_count,
            "is_running": is_running,
        }

        return jsonify(stats)

    except Exception as e:
        print(f"ERROR in /api/stats: {e}")
        import traceback

        traceback.print_exc()

        return (
            jsonify(
                {
                    "devices": {
                        "celltower": 0,
                        "mobile": 0,
                        "laptop": 0,
                        "iot": 0,
                        "c2": 0,
                    },
                    "rabbitmq": {
                        "exchange_published": 0,
                        "gpss_processed": 0,
                        "gemfire_processed": 0,
                    },
                    "gemfire": {
                        "messages_processed": 0,
                        "predictions_cached": 0,
                        "cache_hit_rate": 0,
                        "cache_hits": 0,
                        "cache_misses": 0,
                        "model_loaded": False,
                        "normal_connections": 0,
                        "attack_connections": 0,
                        "unknown_predictions": 0,
                    },
                    "greenplum": 0,
                    "is_running": False,
                }
            ),
            200,
        )


def get_device_counters():
    """Get message counts sent by each device"""
    if not demo_controller:
        return {"celltower": 0, "mobile": 0, "laptop": 0, "iot": 0, "c2": 0}

    if not hasattr(demo_controller, "simulation_manager"):
        return {"celltower": 0, "mobile": 0, "laptop": 0, "iot": 0, "c2": 0}

    try:
        stats = demo_controller.simulation_manager.get_stats()
        devices = stats.get("devices", [])

        # Map device names to counts
        counters = {}
        device_mapping = {
            "Cell Tower": "celltower",
            "Mobile Phone": "mobile",
            "Laptop": "laptop",
            "IoT Device": "iot",
            "C2 Center": "c2",
        }

        for device in devices:
            device_name = device.get("name", "")
            mapped_name = device_mapping.get(
                device_name, device_name.lower().replace(" ", "")
            )
            counters[mapped_name] = device.get("message_count", 0)

        # Ensure all devices are present
        for key in ["celltower", "mobile", "laptop", "iot", "c2"]:
            if key not in counters:
                counters[key] = 0

        return counters
    except Exception as e:
        print(f"ERROR getting device counters: {e}")
        return {"celltower": 0, "mobile": 0, "laptop": 0, "iot": 0, "c2": 0}


def get_rabbitmq_stats():
    """Get RabbitMQ exchange and queue statistics"""
    try:
        rmq_config = config.get_rabbitmq_config()
        management_port = rmq_config.get("management_port", 15672)
        vhost = rmq_config["vhost"].replace("/", "%2F")
        base_url = f"http://{rmq_config['host']}:{management_port}/api"
        auth = HTTPBasicAuth(rmq_config["username"], rmq_config["password"])

        exchange_name = "network_intrusion_fanout"
        exchange_url = f"{base_url}/exchanges/{vhost}/{exchange_name}"

        exchange_response = requests.get(exchange_url, auth=auth, timeout=5)
        exchange_response.raise_for_status()
        exchange_stats = exchange_response.json()

        messages_published = exchange_stats.get("message_stats", {}).get(
            "publish_in", 0
        )

        gpss_url = f"{base_url}/queues/{vhost}/gpss_queue"
        gpss_response = requests.get(gpss_url, auth=auth, timeout=5)
        gpss_response.raise_for_status()
        gpss_stats = gpss_response.json()
        gpss_processed = gpss_stats.get("message_stats", {}).get("ack", 0)

        gemfire_url = f"{base_url}/queues/{vhost}/gemfire_queue"
        gemfire_response = requests.get(gemfire_url, auth=auth, timeout=5)
        gemfire_response.raise_for_status()
        gemfire_stats = gemfire_response.json()
        gemfire_processed = gemfire_stats.get("message_stats", {}).get("ack", 0)

        stats = {
            "exchange_messages_published": messages_published,
            "gpss_messages_processed": gpss_processed,
            "gemfire_messages_processed": gemfire_processed,
        }

        return stats

    except Exception as e:
        print(f"Error getting RabbitMQ stats: {e}")
        return {
            "exchange_messages_published": 0,
            "gpss_messages_processed": 0,
            "gemfire_messages_processed": 0,
        }


def get_greenplum_row_count():
    """Get current row count in Greenplum table"""
    try:
        gp_config = config.get_greenplum_config()

        conn = psycopg2.connect(
            host=gp_config["host"],
            port=gp_config["port"],
            database=gp_config["database"],
            user=gp_config["user"],
            password=gp_config["password"],
            connect_timeout=5,
        )

        cursor = conn.cursor()
        table_name = f"{gp_config['schema']}.{gp_config['table']}"
        
        # Check if table exists, create if it doesn't
        cursor.execute(
            f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = '{gp_config['schema']}'
                AND table_name = '{gp_config['table']}'
            )
            """
        )
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print(f"Table {table_name} does not exist. Creating it...")
            from src.setup_manager import SetupManager
            setup_manager = SetupManager()
            result = setup_manager.setup_greenplum_table()
            if not result.success:
                print(f"Failed to create table: {result.message}")
                cursor.close()
                conn.close()
                return 0
            # Reconnect to get fresh connection
            conn.close()
            conn = psycopg2.connect(
                host=gp_config["host"],
                port=gp_config["port"],
                database=gp_config["database"],
                user=gp_config["user"],
                password=gp_config["password"],
                connect_timeout=5,
            )
            cursor = conn.cursor()
        
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return count

    except Exception as e:
        print(f"Error getting Greenplum row count: {e}")
        import traceback
        traceback.print_exc()
        return 0


def get_gemfire_stats():
    """Get Gemfire cache statistics"""
    try:
        if demo_controller and hasattr(demo_controller, "consumer_service"):
            consumer = demo_controller.consumer_service
            if hasattr(consumer, "gemfire_client"):
                cache_stats = consumer.gemfire_client.get_cache_stats()

                # Main counter = total messages processed through consumer
                total_processed = consumer.prediction_count

                return {
                    "messages_processed": total_processed,  # Main counter
                    "predictions_cached": cache_stats.get(
                        "predictions_cached", 0
                    ),  # Actual cached items
                    "cache_hit_rate": cache_stats.get("hit_rate", 0),
                    "cache_hits": cache_stats.get("cache_hits", 0),
                    "cache_misses": cache_stats.get("cache_misses", 0),
                    "model_loaded": cache_stats.get("model_loaded", False),
                }

        # Return zeros if not available
        return {
            "messages_processed": 0,
            "predictions_cached": 0,
            "cache_hit_rate": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "model_loaded": False,
        }

    except Exception as e:
        print(f"Error getting Gemfire stats: {e}")
        import traceback

        traceback.print_exc()
        return {
            "messages_processed": 0,
            "predictions_cached": 0,
            "cache_hit_rate": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "model_loaded": False,
        }


@app.route("/api/demo/start", methods=["POST"])
def start_demo():
    """Start the demo - creates devices with specific names"""
    global demo_controller

    try:
        if demo_controller and demo_controller.status == "running":
            return (
                jsonify({"success": False, "message": "Demo is already running"}),
                400,
            )

        # Create demo controller with broadcast callback
        demo_controller = DemoController(on_event=broadcast_event)

        # Start the demo
        result = demo_controller.start()

        if result.get("success", True):
            num_devices = len(demo_controller.simulation_manager.devices)
            return jsonify(
                {
                    "success": True,
                    "message": f"Demo started with {num_devices} devices",
                    "num_devices": num_devices,
                }
            )
        else:
            return (
                jsonify(
                    {
                        "success": False,
                        "message": result.get("message", "Failed to start demo"),
                    }
                ),
                500,
            )

    except Exception as e:
        import traceback

        traceback.print_exc()
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/api/demo/stop", methods=["POST"])
def stop_demo():
    """Stop the demo"""
    global demo_controller

    try:
        if not demo_controller or demo_controller.status != "running":
            return jsonify({"success": False, "message": "Demo is not running"}), 400

        result = demo_controller.stop()
        return jsonify({"success": True, "message": "Demo stopped"})

    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/api/demo/status", methods=["GET"])
def get_demo_status():
    """
    Get demo status
    FIXED: Returns safe defaults instead of 500 error
    """
    global demo_controller

    try:
        if not demo_controller:
            return jsonify(
                {
                    "success": True,
                    "status": {
                        "running": False,
                        "devices": 0,
                        "messages_sent": 0,
                        "predictions_made": 0,
                        "normal_connections": 0,
                        "attack_connections": 0,
                        "unknown_predictions": 0,
                        "normal_percent": 0,
                        "attack_percent": 0,
                    },
                }
            )

        # Try to get status, but handle any errors gracefully
        try:
            status = demo_controller.get_status()

            return jsonify(
                {
                    "success": True,
                    "status": {
                        "running": status.get("status") == "running",
                        "devices": status.get("num_devices", 0),
                        "messages_sent": status.get("total_messages_sent", 0),
                        "predictions_made": status.get("total_predictions", 0),
                        "normal_connections": status.get("normal_connections", 0),
                        "attack_connections": status.get("attack_connections", 0),
                        "unknown_predictions": status.get("unknown_predictions", 0),
                        "normal_percent": status.get("normal_percent", 0),
                        "attack_percent": status.get("attack_percent", 0),
                    },
                }
            )
        except:
            # If get_status() fails, return safe defaults
            return jsonify(
                {
                    "success": True,
                    "status": {
                        "running": False,
                        "devices": 0,
                        "messages_sent": 0,
                        "predictions_made": 0,
                        "normal_connections": 0,
                        "attack_connections": 0,
                        "unknown_predictions": 0,
                        "normal_percent": 0,
                        "attack_percent": 0,
                    },
                }
            )

    except Exception as e:
        # Return safe defaults instead of 500 error
        return jsonify(
            {
                "success": True,
                "status": {
                    "running": False,
                    "devices": 0,
                    "messages_sent": 0,
                    "predictions_made": 0,
                },
            }
        )


@app.route("/api/model/train", methods=["POST"])
def train_model():
    """Train model using MADlib on Greenplum and upload to Gemfire"""
    try:
        from src.model_trainer import ModelTrainer

        # This will be a long-running operation
        # In production, you'd use Celery or similar for async tasks
        # For demo, we'll run it synchronously with progress updates

        trainer = ModelTrainer()
        result = trainer.train_model()

        return jsonify(result)

    except Exception as e:
        import traceback

        traceback.print_exc()
        return (
            jsonify(
                {
                    "success": False,
                    "error": str(e),
                    "message": f"Training failed: {str(e)}",
                }
            ),
            500,
        )


@app.route("/api/model/comparison", methods=["GET"])
def get_model_comparison():
    """Get model comparison results from kdd_intrusion_detection.py"""
    try:
        import json
        from pathlib import Path
        
        comparison_file = Path("tools/model_comparison_results.json")
        
        if not comparison_file.exists():
            return jsonify({
                "success": False,
                "message": "No model comparison results found. Run kdd_intrusion_detection.py first to train models.",
                "comparison": None
            }), 404
        
        with open(comparison_file, 'r') as f:
            comparison_data = json.load(f)
        
        return jsonify({
            "success": True,
            "comparison": comparison_data,
            "message": "Model comparison results retrieved successfully"
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e),
            "message": f"Failed to read comparison results: {str(e)}"
        }), 500


@app.route("/api/demo/reset", methods=["POST"])
def reset_demo():
    """Reset demo state and optionally clear data"""
    global demo_controller

    try:
        # Stop demo if running
        if demo_controller:
            if demo_controller.status == "running":
                demo_controller.stop()
                demo_controller = None

        # Get reset options
        data = request.get_json() or {}
        clear_data = data.get("clear_data", True)

        message = "Demo reset successfully"
        reset_details = []

        # Clear RabbitMQ queue
        if clear_data:
            ssh_client = None
            job_name = None
            try:
                # Stop GPSS job before resetting queues (to avoid connection loss)
                try:
                    from src.ssh_client import get_ssh_client
                    ssh_client = get_ssh_client()
                    if ssh_client and ssh_client.connect():
                        gpss_config = config.get_gpss_config()
                        job_name = gpss_config["job_name"]
                        ssh_client.stop_gpss_job(job_name)
                except Exception as e:
                    print(f"Warning: Could not stop GPSS job: {e}")
                
                clear_rabbitmq_queue()
                reset_details.append("RabbitMQ queue cleared")
                    
            except Exception as e:
                print(f"ERROR: Failed to clear RabbitMQ: {e}")
                reset_details.append(f"RabbitMQ clear failed: {e}")

        # Clear Greenplum table
        if clear_data:
            try:
                clear_greenplum_table()
                reset_details.append("Greenplum table cleared")
            except Exception as e:
                print(f"ERROR: Failed to clear Greenplum: {e}")
                reset_details.append(f"Greenplum clear failed: {e}")

        # Clear Gemfire prediction cache (but NOT the model)
        if clear_data:
            try:
                gemfire_client = GemfireClient()
                cache_cleared = gemfire_client.clear_prediction_cache()
                if cache_cleared:
                    reset_details.append("Gemfire prediction cache cleared")
                else:
                    reset_details.append("Gemfire prediction cache clear failed")
            except Exception as e:
                print(f"ERROR: Failed to clear Gemfire prediction cache: {e}")
                reset_details.append(f"Gemfire prediction cache clear error: {e}")

        if clear_data:
            message += " (" + ", ".join(reset_details) + ")"

        return jsonify({
            "success": True, 
            "message": message,
            "requires_setup": clear_data  # Flag to indicate setup is needed
        })

    except Exception as e:
        print(f"ERROR during reset: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "message": str(e)}), 500


def clear_rabbitmq_queue():
    """Clear all messages from RabbitMQ queues and reset statistics"""
    rmq_config = config.get_rabbitmq_config()

    # Clear queues using pika
    queues_to_clear = ["gpss_queue", "gemfire_queue"]
    try:
        credentials = pika.PlainCredentials(rmq_config["username"], rmq_config["password"])
        parameters = pika.ConnectionParameters(
            host=rmq_config["host"],
            port=rmq_config["port"],
            virtual_host=rmq_config["vhost"],
            credentials=credentials,
        )

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # Clear both queues
        for queue_name in queues_to_clear:
            try:
                channel.queue_purge(queue=queue_name)
            except Exception as e:
                print(f"Warning: Could not clear queue '{queue_name}': {e}")
        
        connection.close()
    except Exception as e:
        print(f"ERROR during RabbitMQ queue purge: {e}")
    
    # Reset statistics via Management API
    try:
        management_port = rmq_config.get("management_port", 15672)
        vhost = rmq_config["vhost"].replace("/", "%2F")
        base_url = f"http://{rmq_config['host']}:{management_port}/api"
        auth = HTTPBasicAuth(rmq_config["username"], rmq_config["password"])
        
        # Reset exchange statistics
        exchange_name = "network_intrusion_fanout"
        exchange_url = f"{base_url}/exchanges/{vhost}/{exchange_name}"
        try:
            # Delete and recreate exchange to reset stats (if it exists)
            delete_response = requests.delete(exchange_url, auth=auth, timeout=5)
            if delete_response.status_code in [204, 404]:
                # Recreate exchange
                exchange_config = {
                    "type": "fanout",
                    "durable": True,
                    "auto_delete": False
                }
                requests.put(exchange_url, json=exchange_config, auth=auth, timeout=5)
        except Exception as e:
            print(f"Warning: Could not reset exchange stats: {e}")
        
        # Reset queue statistics by deleting and recreating queues
        for queue_name in queues_to_clear:
            try:
                queue_url = f"{base_url}/queues/{vhost}/{queue_name}"
                
                # Delete queue
                delete_response = requests.delete(queue_url, auth=auth, timeout=5)
                
                if delete_response.status_code in [204, 404]:
                    # Recreate queue
                    queue_config = {
                        "durable": True,
                        "arguments": {"x-queue-type": "quorum"}
                    }
                    requests.put(queue_url, json=queue_config, auth=auth, timeout=5)
            except Exception as e:
                print(f"Warning: Could not reset queue '{queue_name}' stats: {e}")
        
        # Rebind queues to exchange
        exchange_name_encoded = urllib.parse.quote(exchange_name, safe="")
        for queue_name in queues_to_clear:
            try:
                queue_name_encoded = urllib.parse.quote(queue_name, safe="")
                binding_url = f"{base_url}/bindings/{vhost}/e/{exchange_name_encoded}/q/{queue_name_encoded}"
                binding_config = {"routing_key": "", "arguments": {}}
                requests.post(binding_url, json=binding_config, auth=auth, timeout=5)
            except Exception as e:
                print(f"Warning: Could not rebind queue '{queue_name}': {e}")
                
    except Exception as e:
        print(f"Warning: Could not reset statistics via Management API: {e}")
        # Continue - at least queues were purged


def clear_greenplum_table():
    """Clear all rows from Greenplum table"""
    gp_config = config.get_greenplum_config()
    table_name = f"{gp_config['schema']}.{gp_config['table']}"
    
    # Step 1: Stop GPSS job if running to avoid lock conflicts
    try:
        from src.ssh_client import get_ssh_client
        ssh_client = get_ssh_client()
        if ssh_client and ssh_client.connect():
            gpss_config = config.get_gpss_config()
            job_name = gpss_config["job_name"]
            
            # Check job status
            status_output = ssh_client.get_gpss_job_status(job_name)
            
            # Try to stop the job if it's running
            if "running" in status_output.lower() or "active" in status_output.lower():
                success, message = ssh_client.stop_gpss_job(job_name)
                if success:
                    # Wait a moment for transactions to complete
                    import time
                    time.sleep(2)
    except Exception as e:
        print(f"Warning: Could not stop GPSS job before truncate: {e}")
    
    # Step 2: Connect to Greenplum and truncate
    try:
        conn = psycopg2.connect(
            host=gp_config["host"],
            port=gp_config["port"],
            database=gp_config["database"],
            user=gp_config["user"],
            password=gp_config["password"],
            connect_timeout=10
        )

        # Set statement timeout to avoid hanging
        cursor = conn.cursor()
        cursor.execute("SET statement_timeout = '30s'")
        
        # Check for active locks
        lock_check_query = f"""
            SELECT COUNT(*) 
            FROM pg_locks l
            JOIN pg_class c ON l.relation = c.oid
            WHERE c.relname = '{gp_config['table']}'
            AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{gp_config['schema']}')
        """
        cursor.execute(lock_check_query)
        lock_count = cursor.fetchone()[0]
        
        if lock_count > 0:
            print(f"Warning: {lock_count} active lock(s) detected. TRUNCATE may wait or timeout.")
        
        # Truncate table
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()

        cursor.close()
        conn.close()

    except psycopg2.OperationalError as e:
        print(f"ERROR: Greenplum connection error: {e}")
        raise
    except Exception as e:
        error_str = str(e).lower()
        if "lock" in error_str or "timeout" in error_str or "waiting" in error_str:
            raise Exception(f"Table is locked or operation timed out. Please stop GPSS job first: {str(e)}")
        else:
            print(f"ERROR during TRUNCATE: {e}")
            import traceback
            traceback.print_exc()
            raise


@app.route("/api/greenplum/count", methods=["GET"])
def get_greenplum_count():
    """Get current row count from Greenplum table"""
    try:
        count = get_greenplum_row_count()
        return jsonify({"success": True, "count": count})
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "count": 0})


# ===== WebSocket Events =====


@socketio.on("connect")
def handle_connect():
    """Handle client connection"""
    print("Client connected")
    emit("connection_response", {"status": "connected"})


@socketio.on("disconnect")
def handle_disconnect():
    """Handle client disconnection"""
    print("Client disconnected")


@socketio.on("request_status")
def handle_status_request():
    """Handle status request from client"""
    try:
        if demo_controller:
            status = demo_controller.get_status()
            emit("status_update", status)
    except Exception as e:
        emit("error", {"message": str(e)})


# ===== Server Startup =====


def run_server():
    """Start the Flask server"""
    web_config = config.get_web_config()

    print(f"\n{'='*60}")
    print(f"Tanzu Data Intelligence Demo - Starting Server")
    print(f"{'='*60}")
    print(f"Control Panel: http://localhost:{web_config['port']}/control")
    print(f"Demo Dashboard: http://localhost:{web_config['port']}/demo")
    print(f"{'='*60}\n")

    socketio.run(
        app,
        host=web_config["host"],
        port=web_config["port"],
        debug=False,
        allow_unsafe_werkzeug=True,
    )


if __name__ == "__main__":
    run_server()
