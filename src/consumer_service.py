"""
Consumer Service
Consumes messages from RabbitMQ, runs predictions with ONNX, handles GemFire caching
"""

import pika
import json
import time
import base64
import threading
import numpy as np
from typing import Dict, Any, Optional, Callable
from src.config_manager import get_config
from src.gemfire_client import GemfireClient


class ConsumerService:
    """Handles consuming messages from RabbitMQ and running predictions"""

    def __init__(self, on_prediction: Optional[Callable] = None):
        self.config = get_config()
        self.rabbitmq_config = self.config.get_rabbitmq_config()
        self.gemfire_client = GemfireClient()
        self.on_prediction = on_prediction

        self.prediction_count = 0
        self.error_count = 0
        self.inference_times = []
        
        # Counters for normal vs attack connections
        self.normal_count = 0
        self.attack_count = 0
        self.unknown_count = 0  # For predictions we can't classify

        self.consumer_thread = None
        self.connection = None
        self.channel = None
        self.consuming = False
        self.queue_name = "gemfire_queue"

        self.model = None
        self.model_available = False
        self.model_format = None
        self.model_classes = None  # Store model class labels for decoding
        self.target_encoder = None  # Store target encoder for decoding predictions
        self.label_encoders = None  # Store label encoders for categorical feature encoding
        self._load_model()

    def _load_model(self):
        """Load model from disk (from models/ folder)"""
        import os
        import joblib
        from pathlib import Path
        
        # Use models/ folder for all model files
        models_dir = Path("models")
        models_dir.mkdir(exist_ok=True)  # Create directory if it doesn't exist
        
        model_name = "kdd_random_forest_model.pkl"
        target_encoder_name = "kdd_target_encoder.pkl"
        label_encoders_name = "kdd_label_encoders.pkl"
        
        def find_file(filename):
            """Find file in models directory"""
            file_path = models_dir / filename
            if file_path.exists():
                return str(file_path)
            return None
        
        try:
            # Find model file
            model_path = find_file(model_name)
            if not model_path:
                # Try to find any kdd_*_model.pkl file in models directory
                model_path = None
                if models_dir.exists():
                    for file in models_dir.glob("kdd_*_model.pkl"):
                        model_path = str(file)
                        print(f"[Consumer] Found model file: {model_path}")
                        break
                
                if not model_path:
                    self.model_available = False
                    print(f"[Consumer] ⚠️  Model file not found: {model_name}")
                    print(f"[Consumer]   Searched in: {models_dir.absolute()}")
                    print(f"[Consumer]   Make sure you've trained the model first!")
                    return
            
            # Load the model
            print(f"[Consumer] Loading model from disk: {model_path}")
            self.model = joblib.load(model_path)
            self.model_format = "pickle"
            self.model_available = True
            
            # Extract model classes for prediction decoding
            if hasattr(self.model, 'classes_'):
                self.model_classes = self.model.classes_
                print(f"[Consumer] ✓ Model loaded with {len(self.model_classes)} classes")
                print(f"[Consumer]   Sample classes: {list(self.model_classes[:5])}")
            else:
                print("[Consumer] ⚠ Model doesn't have classes_ attribute - predictions will be numeric")
            
            # Find and load target encoder
            target_encoder_path = find_file(target_encoder_name)
            if target_encoder_path:
                print(f"[Consumer] Loading target encoder from: {target_encoder_path}")
                self.target_encoder = joblib.load(target_encoder_path)
                print(f"[Consumer] ✓ Target encoder loaded - can decode predictions to class names")
                print(f"[Consumer]   Encoder classes: {list(self.target_encoder.classes_[:10])}...")
            else:
                print(f"[Consumer] ⚠ Target encoder not found: {target_encoder_name}")
                print(f"[Consumer]   Predictions will use numeric values instead of class names")
            
            # Find and load label encoders
            label_encoders_path = find_file(label_encoders_name)
            if label_encoders_path:
                print(f"[Consumer] Loading label encoders from: {label_encoders_path}")
                self.label_encoders = joblib.load(label_encoders_path)
                print(f"[Consumer] ✓ Label encoders loaded: {list(self.label_encoders.keys())}")
            else:
                print(f"[Consumer] ⚠ Label encoders not found: {label_encoders_name}")
                print(f"[Consumer]   Categorical features will use hash encoding (may cause incorrect predictions)")
            
            print("[Consumer] ✓ Model loaded and ready for inference!")
            print(f"[Consumer]   Model type: {type(self.model)}")
            print(f"[Consumer]   Has predict method: {hasattr(self.model, 'predict')}")

        except Exception as e:
            self.model_available = False
            print(f"✗ Error loading model from disk: {e}")
            import traceback
            traceback.print_exc()

    def start(self):
        """Start consuming messages in background thread"""
        if self.consuming:
            return {"success": False, "message": "Consumer already running"}

        try:
            credentials = pika.PlainCredentials(
                self.rabbitmq_config["username"], self.rabbitmq_config["password"]
            )
            parameters = pika.ConnectionParameters(
                host=self.rabbitmq_config["host"],
                port=self.rabbitmq_config["port"],
                virtual_host=self.rabbitmq_config["vhost"],
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300,
            )

            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()

            print(f"✓ Gemfire Consumer connected to queue: {self.queue_name}")

            self.channel.basic_qos(prefetch_count=10)

            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self._callback,
                auto_ack=False,
            )

            self.gemfire_client.ensure_prediction_cache_region()

            self.consuming = True
            self.consumer_thread = threading.Thread(
                target=self._consume_loop, daemon=True
            )
            self.consumer_thread.start()

            print(f"[Consumer] Started in background thread")

            return {"success": True, "message": "Consumer started"}

        except Exception as e:
            import traceback

            traceback.print_exc()
            return {"success": False, "message": f"Failed to start consumer: {str(e)}"}

    def _consume_loop(self):
        """Main consumer loop (runs in background thread)"""
        try:
            print(f"[Consumer] Starting to consume from queue: {self.queue_name}")
            self.channel.start_consuming()
        except Exception as e:
            print(f"[Consumer] Error in consume loop: {e}")
            self.consuming = False

    def _callback(self, ch, method, properties, body):
        """Handle incoming message from RabbitMQ"""
        try:
            # Step 1: Decode and validate message
            message_str = body.decode("utf-8")
            
            # Step 2: Parse and validate message structure
            features = self._parse_message(message_str)

            if features is None:
                print(f"[Consumer] ⚠ Message validation failed - invalid format")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.error_count += 1
                return
            
            # Step 3: Validate required features are present
            required_features = ["duration", "protocol_type", "service", "flag"]
            missing_features = [f for f in required_features if f not in features]
            if missing_features:
                print(f"[Consumer] ⚠ Message validation failed - missing features: {missing_features}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.error_count += 1
                return

            # Step 4: Run inference using the uploaded model
            result = self._run_inference(features)

            # Step 5: Update counters based on prediction
            self.prediction_count += 1
            self.inference_times.append(result["inference_time_ms"])
            
            # Classify prediction as normal, attack, or unknown
            prediction_value = result.get("prediction", "unknown")
            self._update_classification_counters(prediction_value)

            if len(self.inference_times) > 100:
                self.inference_times = self.inference_times[-100:]

            if self.prediction_count % 20 == 0:
                cache_stats = self.gemfire_client.get_cache_stats()
                if self.model_available:
                    print(
                        f"[Consumer] Processed {self.prediction_count} predictions | "
                        f"Normal: {self.normal_count} | Attacks: {self.attack_count} | "
                        f"Cache hit rate: {cache_stats['hit_rate']:.1f}%"
                    )
                else:
                    print(
                        f"[Consumer] Processed {self.prediction_count} predictions, cache hit rate: 0.0% (no model)"
                    )

            if self.on_prediction:
                self.on_prediction(
                    {
                        **result,
                        "prediction_count": self.prediction_count,
                        "features": features,
                    }
                )

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            print(f"[Consumer] ✗ Error processing message: {e}")
            import traceback

            traceback.print_exc()
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            self.error_count += 1
    
    def _update_classification_counters(self, prediction_value: Any):
        """
        Update counters based on prediction value
        Classifies as normal, attack, or unknown
        """
        # Normalize prediction string: lowercase, strip whitespace, and remove trailing punctuation
        prediction_str = str(prediction_value).lower().strip()
        # Remove trailing periods, commas, and other punctuation that might come from CSV parsing
        prediction_str = prediction_str.rstrip('.,;:!?')
        
        # Check if it's an error/unknown state first
        if prediction_str in ["error", "n/a", "not available", "unknown", ""]:
            self.unknown_count += 1
            return
        
        # Check if it's "normal" (case-insensitive, after normalization)
        if prediction_str == "normal":
            self.normal_count += 1
            return
        
        # Check if it's a numeric value that might need decoding
        try:
            # Try to parse as numeric
            pred_num = int(float(prediction_str))
            
            # If it's numeric, try to decode using target encoder (preferred)
            if self.target_encoder is not None:
                try:
                    decoded_array = self.target_encoder.inverse_transform([pred_num])
                    decoded_class = str(decoded_array[0]).lower().strip().rstrip('.,;:!?')
                    if decoded_class == "normal":
                        self.normal_count += 1
                    else:
                        self.attack_count += 1
                    return
                except (ValueError, IndexError, Exception):
                    pass  # Fall through to next decoding method
            
            # Fallback: try to decode using model.classes_ if available
            if self.model_classes is not None and 0 <= pred_num < len(self.model_classes):
                decoded_class = str(self.model_classes[pred_num]).lower().strip().rstrip('.,;:!?')
                if decoded_class == "normal":
                    self.normal_count += 1
                else:
                    self.attack_count += 1
                return
            
            # Numeric but can't decode - assume attack
            self.attack_count += 1
            return
            
        except (ValueError, TypeError):
            # Not numeric - it's a string class name
            # If it's "normal", we already checked above, so everything else is an attack
            if "normal" in prediction_str and prediction_str != "normal":
                # This is a weird case - classify as attack
                self.attack_count += 1
            else:
                # Any other class name (neptune, smurf, etc.) is an attack
                self.attack_count += 1

    def _parse_message(self, message_str: str) -> Optional[Dict[str, Any]]:
        """Parse message - handles both CSV and JSON formats"""
        try:
            message = json.loads(message_str)

            if isinstance(message, dict):
                features = {
                    k: v
                    for k, v in message.items()
                    if k
                    not in [
                        "timestamp",
                        "device_id",
                        "device_name",
                        "message_id",
                        "intrusion_type",
                    ]
                }
                return features

        except json.JSONDecodeError:
            try:
                values = message_str.strip().split(",")
                
                # Skip first field (ID) and last field (intrusion_type) if present
                # Format: id,feature1,feature2,...,feature41,intrusion_type
                if len(values) > 42:
                    # Has ID and intrusion_type, skip both
                    values = values[1:-1]  # Skip first (ID) and last (intrusion_type)
                elif len(values) > 41:
                    # Has either ID or intrusion_type, skip last
                    values = values[:-1]  # Skip last (intrusion_type)
                elif len(values) == 42:
                    # Exactly 42 fields: id + 41 features
                    values = values[1:]  # Skip first (ID)

                feature_names = [
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
                ]

                if len(values) > 41 and values[-1].endswith("."):
                    values[-1] = values[-1][:-1]

                features = {}
                for i, name in enumerate(feature_names):
                    if i < len(values):
                        val = values[i]
                        try:
                            if (
                                "." in val
                                or val.isdigit()
                                or (
                                    val.startswith("-")
                                    and val[1:].replace(".", "").isdigit()
                                )
                            ):
                                features[name] = float(val)
                            else:
                                features[name] = val
                        except ValueError:
                            features[name] = val
                return features

            except Exception as e:
                print(f"[Consumer] CSV parse error: {e}")
                return None

        except Exception as e:
            print(f"[Consumer] Unexpected error parsing message: {e}")
            return None

    def _run_inference(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """Run inference with Gemfire caching"""
        start_time = time.time()

        if not self.model_available:
            prediction = "not available"
            inference_time_ms = 0.1
            cache_hit = False
        else:
            try:
                # Check cache first
                cached_prediction = self.gemfire_client.get_prediction(features)

                if cached_prediction is not None:
                    # Cache HIT - use cached prediction, but decode it if it's numeric
                    prediction_raw = cached_prediction
                    
                    # Decode cached prediction if it's numeric
                    if isinstance(prediction_raw, (int, np.integer)) or (isinstance(prediction_raw, str) and prediction_raw.isdigit()):
                        try:
                            pred_num = int(prediction_raw) if isinstance(prediction_raw, str) else int(prediction_raw)
                            
                            # Try to decode using target encoder first (preferred)
                            if self.target_encoder is not None:
                                decoded_array = self.target_encoder.inverse_transform([pred_num])
                                prediction = str(decoded_array[0]).strip().rstrip('.,;:!?')
                            # Fallback to model.classes_ if target encoder not available
                            elif self.model_classes is not None and 0 <= pred_num < len(self.model_classes):
                                prediction = str(self.model_classes[pred_num]).strip().rstrip('.,;:!?')
                            else:
                                # Can't decode, use as-is but normalize
                                prediction = str(prediction_raw).strip().rstrip('.,;:!?')
                        except Exception as e:
                            # Decoding failed, use as-is but normalize
                            prediction = str(prediction_raw).strip().rstrip('.,;:!?')
                    else:
                        # Already decoded or string, normalize it
                        prediction = str(prediction_raw).strip().rstrip('.,;:!?')
                    
                    inference_time_ms = (time.time() - start_time) * 1000
                    cache_hit = True
                else:
                    # Cache MISS - run model inference
                    prediction = self._predict_with_model(features)
                    inference_time_ms = (time.time() - start_time) * 1000
                    cache_hit = False
                    if prediction != "n/a" and prediction != "error":
                        self.gemfire_client.store_prediction(features, prediction)

            except Exception as e:
                print(f"[Consumer] Prediction error: {e}")
                prediction = "error"
                inference_time_ms = (time.time() - start_time) * 1000
                cache_hit = False

        return {
            "prediction": prediction,
            "inference_time_ms": round(inference_time_ms, 2),
            "cache_hit": cache_hit,
            "model_available": self.model_available,
        }

    def _predict_with_model(self, features: Dict[str, Any]) -> str:
        """Run actual model prediction"""
        try:
            # Explicit check: ensure model is loaded and available
            if self.model is None:
                print("[Consumer] ERROR: Model object is None but model_available is True!")
                return "error"
            
            if self.model_format == "onnx":
                return self._predict_onnx(features)
            elif hasattr(self.model, "predict"):
                # Extract feature values in the correct order
                categorical_features = ["protocol_type", "service", "flag"]
                
                feature_values = []
                feature_names = [
                    "duration", "protocol_type", "service", "flag", "src_bytes", "dst_bytes",
                    "land", "wrong_fragment", "urgent", "hot", "num_failed_logins", "logged_in",
                    "num_compromised", "root_shell", "su_attempted", "num_root", "num_file_creations",
                    "num_shells", "num_access_files", "num_outbound_cmds", "is_host_login",
                    "is_guest_login", "count", "srv_count", "serror_rate", "srv_serror_rate",
                    "rerror_rate", "srv_rerror_rate", "same_srv_rate", "diff_srv_rate",
                    "srv_diff_host_rate", "dst_host_count", "dst_host_srv_count",
                    "dst_host_same_srv_rate", "dst_host_diff_srv_rate", "dst_host_same_src_port_rate",
                    "dst_host_srv_diff_host_rate", "dst_host_serror_rate", "dst_host_srv_serror_rate",
                    "dst_host_rerror_rate", "dst_host_srv_rerror_rate",
                ]
                
                for col in feature_names:
                    value = features.get(col, 0)
                    # Encode categorical features using LabelEncoder (if available) or hash encoding (fallback)
                    if col in categorical_features and isinstance(value, str):
                        if self.label_encoders is not None and col in self.label_encoders:
                            # Use the same LabelEncoder that was used during training
                            try:
                                encoded_val = float(self.label_encoders[col].transform([value])[0])
                                feature_values.append(encoded_val)
                            except (ValueError, KeyError):
                                # Value not seen during training, use hash as fallback
                                encoded_val = float(hash(value) % 1000)
                                feature_values.append(encoded_val)
                        else:
                            # No label encoder available, use hash encoding (may cause incorrect predictions)
                            encoded_val = float(hash(value) % 1000)
                            feature_values.append(encoded_val)
                    else:
                        try:
                            num_val = float(value) if value is not None else 0.0
                            feature_values.append(num_val)
                        except (ValueError, TypeError):
                            feature_values.append(0.0)
                
                # Convert to pandas DataFrame with column names to match training format
                # This eliminates the sklearn warning and ensures feature alignment
                import pandas as pd
                feature_df = pd.DataFrame([feature_values], columns=feature_names)
                
                # Execute the model with DataFrame (matches training format)
                prediction_numeric = self.model.predict(feature_df)[0]
                
                # Decode prediction to class name
                prediction = None
                if self.target_encoder is not None and isinstance(prediction_numeric, (int, np.integer)):
                    try:
                        decoded_array = self.target_encoder.inverse_transform([prediction_numeric])
                        prediction = str(decoded_array[0]).strip().rstrip('.,;:!?')
                    except (ValueError, IndexError, Exception):
                        prediction = None
                
                if prediction is None:
                    if self.model_classes is not None and isinstance(prediction_numeric, (int, np.integer)):
                        try:
                            pred_idx = int(prediction_numeric)
                            if 0 <= pred_idx < len(self.model_classes):
                                prediction = str(self.model_classes[pred_idx]).strip().rstrip('.,;:!?')
                            else:
                                prediction = str(prediction_numeric)
                        except (ValueError, IndexError):
                            prediction = str(prediction_numeric)
                    else:
                        prediction = str(prediction_numeric)
                
                return prediction
            else:
                print(f"[Consumer] Model object doesn't have 'predict' method. Type: {type(self.model)}")
                return "n/a"

        except Exception as e:
            print(f"[Consumer] Model prediction failed: {e}")
            import traceback
            traceback.print_exc()
            return "error"

    def _predict_onnx(self, features: Dict[str, Any]) -> str:
        """Run ONNX model prediction"""
        try:
            # Explicit check: ensure ONNX model is loaded
            if self.model is None:
                print("[Consumer] ERROR: ONNX model object is None!")
                return "error"
            
            feature_columns = [
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
            ]

            feature_array = []
            for col in feature_columns:
                value = features.get(col, 0)
                if isinstance(value, str):
                    feature_array.append(float(hash(value) % 1000))
                else:
                    feature_array.append(float(value) if value is not None else 0.0)

            input_data = np.array([feature_array], dtype=np.float32)

            input_name = self.model.get_inputs()[0].name
            output_name = self.model.get_outputs()[0].name

            result = self.model.run([output_name], {input_name: input_data})
            prediction = result[0][0]

            return str(prediction)

        except Exception as e:
            print(f"[Consumer] ONNX prediction failed: {e}")
            return "error"

    def stop(self):
        """Stop consuming messages"""
        if not self.consuming:
            return {"success": False, "message": "Consumer not running"}

        try:
            self.consuming = False

            import time

            time.sleep(0.5)

            if self.channel and self.channel.is_open:
                try:
                    self.channel.stop_consuming()
                except Exception as e:
                    print(f"[Consumer] Note: stop_consuming error (expected): {e}")

            if self.connection and self.connection.is_open:
                try:
                    self.connection.close()
                except Exception as e:
                    print(f"[Consumer] Note: connection close error (expected): {e}")

            print("[Consumer] Stopped")

            return {"success": True, "message": "Consumer stopped"}

        except Exception as e:
            print(f"[Consumer] Stop error: {e}")
            return {"success": False, "message": f"Error stopping consumer: {str(e)}"}

    def get_stats(self) -> Dict[str, Any]:
        """Get consumer statistics"""
        avg_inference_time = (
            sum(self.inference_times) / len(self.inference_times)
            if self.inference_times
            else 0
        )

        cache_stats = self.gemfire_client.get_cache_stats()
        
        # Calculate percentages
        total_classified = self.normal_count + self.attack_count
        normal_percent = (self.normal_count / total_classified * 100) if total_classified > 0 else 0
        attack_percent = (self.attack_count / total_classified * 100) if total_classified > 0 else 0

        return {
            "running": self.consuming,
            "predictions_made": self.prediction_count,
            "errors": self.error_count,
            "avg_inference_time_ms": round(avg_inference_time, 2),
            "model_available": self.model_available,
            "cache_stats": cache_stats,
            # Classification counters
            "normal_connections": self.normal_count,
            "attack_connections": self.attack_count,
            "unknown_predictions": self.unknown_count,
            "normal_percent": round(normal_percent, 2),
            "attack_percent": round(attack_percent, 2),
        }
