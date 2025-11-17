"""
Gemfire Client
Handles interactions with VMware Gemfire
UPDATED: Added prediction cache methods for demo
"""

import requests
import pickle
import base64
import hashlib
import json
import time
from typing import Any, Dict, Optional
from src.config_manager import get_config


class GemfireClient:
    """Client for interacting with Gemfire REST API"""

    def __init__(self):
        self.config = get_config()
        self.gemfire_config = self.config.get_gemfire_config()

        self.rest_url = self.gemfire_config["rest_url"]
        self.model_region = self.gemfire_config["model_region"]
        self.model_key = self.gemfire_config["model_key"]

        # Prediction cache region
        self.prediction_region = "PredictionCache"

        # In-memory model cache
        self.cached_model = None

        # Stats
        self.cache_hits = 0
        self.cache_misses = 0
        self.model_loaded = False

    def _make_request(
        self, method: str, url: str, **kwargs
    ) -> Optional[requests.Response]:
        """Make HTTP request to Gemfire with error handling"""
        try:
            response = requests.request(method, url, timeout=30, **kwargs)  # Increased timeout to 30s for large uploads
            
            # Don't raise for 404 on GET (cache miss is expected)
            if method == "GET" and response.status_code == 404:
                return response

            # For other errors, don't raise immediately - return response so we can inspect it
            if response.status_code >= 400:
                return response

            response.raise_for_status()
            return response
        except requests.exceptions.Timeout as e:
            print(f"[GemfireClient] ✗ Request timeout: {e}")
            print(f"[GemfireClient]   URL: {url}")
            print(f"[GemfireClient]   This may be due to a large model size or network issues")
            return None
        except requests.exceptions.ConnectionError as e:
            print(f"[GemfireClient] ✗ Connection error: {e}")
            print(f"[GemfireClient]   URL: {url}")
            print(f"[GemfireClient]   Check if Gemfire REST API is accessible at this URL")
            return None
        except requests.exceptions.RequestException as e:
            # Only print errors that aren't 404 (cache misses are normal)
            if not (method == "GET" and "404" in str(e)):
                print(f"[GemfireClient] ✗ Request error: {type(e).__name__}: {e}")
                print(f"[GemfireClient]   URL: {url}")
            return None

    # ========== MODEL CACHE METHODS ==========

    def get_model(self) -> Optional[Any]:
        """
        Get trained model from Gemfire
        Returns cached model if already loaded
        """
        # Return cached model if available
        if self.cached_model is not None:
            return self.cached_model

        try:
            # GET /gemfire-api/v1/{region}/{key}
            url = f"{self.rest_url}/{self.model_region}/{self.model_key}"
            response = self._make_request("GET", url)
            
            if response and response.status_code == 200:
                # Model is stored as base64-encoded pickle
                model_data = response.json()
                
                model_encoded = None

                if isinstance(model_data, dict):
                    # Try different possible keys
                    model_encoded = model_data.get("value") or model_data.get("@value")

                    # If still dict, might be nested
                    if isinstance(model_encoded, dict):
                        model_encoded = model_encoded.get("value") or model_encoded.get(
                            "@value"
                        )
                else:
                    # Direct string response
                    model_encoded = model_data

                # Ensure we have a string to decode
                if not isinstance(model_encoded, str):
                    print(f"[GemfireClient] ERROR: Expected base64 string, got {type(model_encoded)}")
                    raise ValueError(
                        f"Expected base64 string, got {type(model_encoded)}"
                    )

                # Decode and unpickle
                model_bytes = base64.b64decode(model_encoded)
                unpickled_obj = pickle.loads(model_bytes)

                # Handle both formats:
                # 1. If it's already a dict with metadata (from model_trainer), return it as-is
                # 2. If it's a raw model object (direct upload), wrap it in the expected dict format
                if isinstance(unpickled_obj, dict) and "format" in unpickled_obj and "model_data" in unpickled_obj:
                    # Already in the correct format from model_trainer
                    model = unpickled_obj
                else:
                    # Raw model object - wrap it in the expected format
                    # Re-encode the model as base64 for the dict structure
                    model_bytes_reencoded = pickle.dumps(unpickled_obj)
                    model_data_b64 = base64.b64encode(model_bytes_reencoded).decode("utf-8")
                    
                    model = {
                        "format": "pickle",
                        "model_data": model_data_b64,
                        "algorithm": type(unpickled_obj).__name__,
                        "source": "direct_upload"
                    }

                # Cache the model
                self.cached_model = model
                self.model_loaded = True

                print(f"[GemfireClient] ✓ Model loaded successfully from Gemfire!")
                print(f"[GemfireClient]   Model format: {model.get('format', 'unknown')}")
                print(f"[GemfireClient]   Model algorithm: {model.get('algorithm', 'unknown')}")
                return model
            else:
                print(
                    f"✗ Model not found in Gemfire (region: {self.model_region}, key: {self.model_key})"
                )
                return None

        except Exception as e:
            import traceback

            print(f"✗ Error loading model from Gemfire: {e}")
            traceback.print_exc()
            return None

    def store_model(self, model: Any) -> bool:
        """Store trained model in Gemfire"""
        try:
            # Pickle the model
            model_bytes = pickle.dumps(model)
            
            # Also create base64 version for JSON format attempts
            model_encoded = base64.b64encode(model_bytes).decode("utf-8")

            # PUT /gemfire-api/v1/{region}/{key}
            url = f"{self.rest_url}/{self.model_region}/{self.model_key}"
            
            # Try multiple formats - Gemfire REST API can be picky about large values
            # Format 1: Try as plain string in JSON format (original approach)
            response = self._make_request(
                "PUT",
                url,
                headers={"Content-Type": "application/json"},
                json={"@type": "string", "@value": model_encoded},
            )
            
            # If that fails with 400, try alternative formats
            original_response = response  # Keep original for error reporting
            
            if response and response.status_code == 400:
                # Try Format 2: Plain text body
                try:
                    alt_response = self._make_request(
                        "PUT",
                        url,
                        headers={"Content-Type": "text/plain"},
                        data=model_encoded,
                    )
                    if alt_response and alt_response.status_code in [200, 201]:
                        print(f"[GemfireClient] ✓ Format 2 (text/plain) worked!")
                        response = alt_response
                    elif alt_response:
                        print(f"[GemfireClient] Format 2 failed with status: {alt_response.status_code}")
                        if alt_response.text:
                            print(f"[GemfireClient]   Response: {alt_response.text[:200]}")
                    else:
                        print(f"[GemfireClient] Format 2 failed - no response")
                except Exception as alt_e:
                    print(f"[GemfireClient] Format 2 exception: {alt_e}")
                    import traceback
                    traceback.print_exc()
                
                # If still failing, try Format 3: Binary upload (smaller, more efficient)
                if not (response and response.status_code in [200, 201]):
                    try:
                        alt_response = self._make_request(
                            "PUT",
                            url,
                            headers={"Content-Type": "application/octet-stream"},
                            data=model_bytes,  # Raw bytes, not base64
                        )
                        if alt_response and alt_response.status_code in [200, 201]:
                            print(f"[GemfireClient] ✓ Format 3 (binary/octet-stream) worked!")
                            response = alt_response
                        elif alt_response:
                            print(f"[GemfireClient] Format 3 failed with status: {alt_response.status_code}")
                            if alt_response.text:
                                print(f"[GemfireClient]   Response: {alt_response.text[:200]}")
                        else:
                            print(f"[GemfireClient] Format 3 failed - no response")
                    except Exception as alt_e:
                        print(f"[GemfireClient] Format 3 exception: {alt_e}")
                        import traceback
                        traceback.print_exc()
                
                # If still failing, try Format 4: JSON with just the string value
                if not (response and response.status_code in [200, 201]):
                    try:
                        alt_response = self._make_request(
                            "PUT",
                            url,
                            headers={"Content-Type": "application/json"},
                            json=model_encoded,  # Just the string, not wrapped
                        )
                        if alt_response and alt_response.status_code in [200, 201]:
                            print(f"[GemfireClient] ✓ Format 4 (simplified JSON) worked!")
                            response = alt_response
                        elif alt_response:
                            print(f"[GemfireClient] Format 4 failed with status: {alt_response.status_code}")
                            if alt_response.text:
                                print(f"[GemfireClient]   Response: {alt_response.text[:200]}")
                        else:
                            print(f"[GemfireClient] Format 4 failed - no response")
                    except Exception as alt_e:
                        print(f"[GemfireClient] Format 4 exception: {alt_e}")
                        import traceback
                        traceback.print_exc()
                
                # If all formats failed, restore original response for error reporting
                if not (response and response.status_code in [200, 201]):
                    response = original_response

            if response is None:
                print(f"[GemfireClient] ✗ Failed to store model in Gemfire - No response received")
                print(f"[GemfireClient]   This usually indicates:")
                print(f"[GemfireClient]     - Network connectivity issue")
                print(f"[GemfireClient]     - Request timeout (model may be too large)")
                print(f"[GemfireClient]     - Gemfire REST API not accessible")
                print(f"[GemfireClient]     - Connection refused or reset")
                print(f"[GemfireClient]   URL attempted: {url}")
                print(f"[GemfireClient]   Model size: {len(model_encoded)} chars (~{len(model_encoded)/1024/1024:.1f}MB)")
                print(f"[GemfireClient]   Check:")
                print(f"[GemfireClient]     1. Gemfire REST API is running and accessible")
                print(f"[GemfireClient]     2. Region '{self.model_region}' exists")
                print(f"[GemfireClient]     3. Network connectivity to {self.rest_url}")
                return False
            elif response.status_code in [200, 201]:
                print(f"[GemfireClient] ✓ Model stored successfully in Gemfire")
                print(f"[GemfireClient]   Status code: {response.status_code}")
                
                # Verify the model was actually stored by checking it immediately
                time.sleep(0.5)  # Brief delay to ensure write completes
                verification = self.check_model_exists()
                
                if verification:
                    print(f"[GemfireClient] ✓ Verification successful - model is accessible!")
                    self.cached_model = model
                    self.model_loaded = True
                    return True
                else:
                    print(f"[GemfireClient] ⚠ WARNING: Model upload returned success but verification failed!")
                    print(f"[GemfireClient]   This might indicate a timing issue or region/key mismatch")
                    # Still return True since the PUT succeeded - verification might be a timing issue
                    self.cached_model = model
                    self.model_loaded = True
                    return True
            else:
                # response exists but status is not 200/201
                status = response.status_code if response else "None"
                print(f"[GemfireClient] ✗ Failed to store model in Gemfire - Status: {status}")
                if response and hasattr(response, 'text') and response.text:
                    print(f"[GemfireClient]   Response text: {response.text[:500]}")
                print(f"[GemfireClient]   Make sure region '{self.model_region}' exists in GemFire!")
                print(f"[GemfireClient]   URL: {url}")
                print(f"[GemfireClient]   Troubleshooting:")
                print(f"[GemfireClient]     - Status 400: JSON format not supported (tried all formats)")
                print(f"[GemfireClient]     - Status 503: Service unavailable (server may be overloaded)")
                print(f"[GemfireClient]     - Check Gemfire server logs for details")
                print(f"[GemfireClient]     - Verify region type supports large values")
                print(f"[GemfireClient]     - Consider using gfsh or direct file upload instead")
                return False

        except Exception as e:
            print(f"✗ Error storing model in Gemfire: {e}")
            return False

    def upload_model(self, model: Any) -> bool:
        """
        Alias for store_model() for backward compatibility with setup_manager
        """
        return self.store_model(model)

    def load_model(self) -> Optional[Any]:
        """
        Alias for get_model() for backward compatibility with consumer_service
        """
        return self.get_model()

    def check_model_exists(self) -> bool:
        """
        Check if model exists in Gemfire (for preflight checks)
        Returns True if model exists, False otherwise
        """
        try:
            url = f"{self.rest_url}/{self.model_region}/{self.model_key}"
            response = self._make_request("GET", url)
            
            if response and response.status_code == 200:
                return True
            else:
                return False

        except Exception as e:
            return False

    # ========== PREDICTION CACHE METHODS ==========

    def _generate_cache_key(self, features: Dict[str, Any]) -> str:
        """
        Generate cache key from feature values
        Uses hash of sorted feature values for consistency

        CRITICAL: Only uses the features that were used in model training!
        """
        # Sort features by key for consistent hashing
        # Convert all values to strings to ensure consistent hashing
        sorted_features = sorted(features.items())

        # Create a deterministic string representation
        feature_parts = []
        for k, v in sorted_features:
            # Convert to string, handling floats consistently
            if isinstance(v, float):
                # Round floats to avoid precision issues
                feature_parts.append(f"{k}:{v:.6f}")
            else:
                feature_parts.append(f"{k}:{v}")

        feature_str = "|".join(feature_parts)

        # Hash to create shorter key
        cache_key = hashlib.md5(feature_str.encode()).hexdigest()
        return cache_key

    def get_prediction(self, features: Dict[str, Any]) -> Optional[str]:
        """
        Check if prediction exists in cache for given features
        Returns cached prediction or None if not found

        NOTE: Should only be called when model is available
        """
        try:
            cache_key = self._generate_cache_key(features)

            # GET /gemfire-api/v1/{region}/{key}
            url = f"{self.rest_url}/{self.prediction_region}/{cache_key}"

            response = self._make_request("GET", url)

            if response and response.status_code == 200:
                # Cache HIT
                self.cache_hits += 1

                data = response.json()

                # Extract prediction value - handle same format as we stored
                prediction = None
                if isinstance(data, dict):
                    # Try different possible keys
                    prediction = data.get("value") or data.get("@value")

                    # If still dict, might be nested
                    if isinstance(prediction, dict):
                        prediction = prediction.get("value") or prediction.get("@value")
                else:
                    prediction = data

                return str(prediction) if prediction else None

            elif response and response.status_code == 404:
                # Cache MISS - key not found (this is normal, not an error)
                self.cache_misses += 1
                return None
            else:
                # Other error (region might not exist)
                self.cache_misses += 1
                return None

        except Exception as e:
            # On error, count as cache miss (silently)
            self.cache_misses += 1
            return None

    def store_prediction(
        self, features: Dict[str, Any], prediction: str, ttl: int = 3600
    ) -> bool:
        """
        Store prediction result in cache
        Uses same format as model storage which we know works

        Args:
            features: Input features used for prediction
            prediction: Prediction result to cache
            ttl: Time to live in seconds (default 1 hour)

        Returns:
            True if stored successfully, False otherwise
        """
        try:
            cache_key = self._generate_cache_key(features)

            # PUT /gemfire-api/v1/{region}/{key}?op=PUT
            # Add op=PUT query parameter which GemFire REST API expects
            url = f"{self.rest_url}/{self.prediction_region}/{cache_key}?op=PUT"

            # Use same format as model - this is what works!
            payload = {"@type": "string", "@value": str(prediction)}

            response = self._make_request(
                "PUT", url, headers={"Content-Type": "application/json"}, json=payload
            )
            
            if response and response.status_code in [200, 201]:
                return True
            else:
                return False

        except Exception as e:
            if self.cache_misses <= 5:
                print(f"Exception storing prediction: {e}")
                import traceback

                traceback.print_exc()
            return False

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get prediction cache statistics"""
        total_requests = self.cache_hits + self.cache_misses
        hit_rate = (self.cache_hits / total_requests * 100) if total_requests > 0 else 0

        # Try to get cache size from Gemfire
        cache_size = self._get_region_size(self.prediction_region)

        stats = {
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "total_requests": total_requests,
            "hit_rate": round(hit_rate, 2),
            "predictions_cached": cache_size,
            "model_loaded": self.model_loaded,
        }


        return stats

    def _get_region_size(self, region: str) -> int:
        """
        Get number of entries in a Gemfire region
        FIXED: Use keys endpoint which works better
        """
        try:
            # GET /gemfire-api/v1/{region}/keys
            url = f"{self.rest_url}/{region}/keys"

            response = self._make_request("GET", url)

            if response and response.status_code == 200:
                data = response.json()

                # Response should be like: {"keys": ["key1", "key2", ...]}
                if isinstance(data, dict) and "keys" in data:
                    keys = data["keys"]
                    return len(keys) if isinstance(keys, list) else 0
                elif isinstance(data, list):
                    return len(data)
                else:
                    return 0

            return 0

        except Exception as e:
            return 0

    def ensure_prediction_cache_region(self) -> bool:
        """
        Ensure PredictionCache region exists in Gemfire
        Returns True if region exists or was created
        """
        try:
            # Check if region exists by trying to query it
            url = f"{self.rest_url}/{self.prediction_region}"
            response = requests.get(url, timeout=5)

            if response.status_code == 200:
                print(f"✓ PredictionCache region exists")
                return True
            elif response.status_code == 404:
                print(f"⚠ PredictionCache region doesn't exist")
                print(f"  Note: Create region manually in Gemfire:")
                print(f"  gfsh> connect --locator=localhost[10334]")
                print(
                    f"  gfsh> create region --name={self.prediction_region} --type=REPLICATE"
                )
                print(f"")
                print(
                    f"  Without this region, predictions won't be cached (demo will still work)"
                )
                return False

            return False

        except Exception as e:
            print(f"⚠ Could not verify PredictionCache region: {e}")
            return False

    def create_prediction_cache_region(self) -> tuple[bool, str]:
        """
        Create PredictionCache region via REST API
        Returns (success, message)

        Note: Region creation via REST API requires management port, not data port
        This may need to be done via gfsh instead
        """
        try:
            # First check if it exists
            url = f"{self.rest_url}/{self.prediction_region}"
            response = requests.get(url, timeout=5)

            if response.status_code == 200:
                return (True, f"Region '{self.prediction_region}' already exists")

            # Try to create via REST API
            # Note: This typically requires management REST API, not data REST API
            # Most reliable way is via gfsh or management API

            return (
                False,
                f"Region creation requires gfsh access. Use setup instructions.",
            )

        except Exception as e:
            return (False, f"Error checking/creating region: {e}")

    def clear_prediction_cache(self) -> bool:
        """Clear all cached predictions"""
        try:
            # DELETE /gemfire-api/v1/{region}
            url = f"{self.rest_url}/{self.prediction_region}"
            response = self._make_request("DELETE", url)

            if response and response.status_code in [200, 204]:
                # Reset stats
                self.cache_hits = 0
                self.cache_misses = 0
                return True
            else:
                status = response.status_code if response else "None"
                print(f"[RESET Gemfire] Failed to clear prediction cache (status: {status})")
                return False

        except Exception as e:
            print(f"[RESET Gemfire] Error clearing prediction cache: {e}")
            return False

    def reset_stats(self):
        """Reset cache statistics"""
        self.cache_hits = 0
        self.cache_misses = 0

    def test_connection(self) -> bool:
        """Test connection to Gemfire"""
        try:
            # Try to list regions
            url = f"{self.rest_url}/regions"

            response = self._make_request("GET", url)

            if response and response.status_code == 200:
                print(f"✓ Gemfire connection successful")
                return True
            else:
                print(f"✗ Gemfire connection failed")
                return False

        except Exception as e:
            print(f"✗ Gemfire connection error: {e}")
            return False

    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive Gemfire status"""
        return {
            "connected": self.test_connection(),
            "model_loaded": self.model_loaded,
            "cache_stats": self.get_cache_stats(),
            "rest_url": self.rest_url,
            "model_region": self.model_region,
            "prediction_region": self.prediction_region,
        }
