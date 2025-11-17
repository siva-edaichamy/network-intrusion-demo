"""
setup_manager.py
Handles setup and teardown of demo environment
UPDATED: Creates both model region and prediction cache region
"""

import os
import time
import psycopg2
import requests
from requests.auth import HTTPBasicAuth
import urllib.parse
from typing import Dict, Any, Optional
from pathlib import Path
from dataclasses import dataclass
from src.config_manager import get_config
from src.gemfire_client import GemfireClient
from src.ssh_client import get_ssh_client


@dataclass
class SetupResult:
    name: str
    success: bool
    message: str
    status: str  # 'created', 'exists', 'updated', 'skipped', 'warning', 'error'

    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return {
            "name": self.name,
            "success": self.success,
            "message": self.message,
            "status": self.status,
        }


class SetupManager:
    """Manages demo environment setup and teardown"""

    def __init__(self):
        self.config = get_config()
        self.gemfire_client = GemfireClient()
        try:
            self.ssh_client = get_ssh_client()
        except:
            self.ssh_client = None
            print("Note: SSH client not available (GPSS setup will be skipped)")

    def _create_gemfire_region_via_ssh(self, region_name: str) -> tuple[bool, str]:
        """Helper method to create a GemFire region via SSH"""
        try:
            gemfire_config = self.config.get_gemfire_config()
            gemfire_ssh_host = os.getenv("GEMFIRE_SSH_HOST") or gemfire_config.get(
                "public_ip"
            )
            gemfire_ssh_user = os.getenv("GEMFIRE_SSH_USER", "ec2-user")
            gemfire_ssh_key = os.getenv("GEMFIRE_SSH_KEY_PATH")

            if not gemfire_ssh_host:
                return (False, f"GEMFIRE_SSH_HOST not configured. Set GEMFIRE_SSH_HOST environment variable or configure public_ip in config")

            locator = gemfire_config.get("locator", "localhost[10334]")

            print(f"[Setup]   SSH Host: {gemfire_ssh_host}")
            print(f"[Setup]   SSH User: {gemfire_ssh_user}")
            print(f"[Setup]   Locator: {locator}")
            print(f"[Setup]   Creating GemFire region '{region_name}' via SSH...")

            import paramiko

            gemfire_ssh = paramiko.SSHClient()
            gemfire_ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            try:
                connect_params = {
                    "hostname": gemfire_ssh_host,
                    "port": 22,
                    "username": gemfire_ssh_user,
                    "timeout": 10,
                }

                if gemfire_ssh_key and os.path.exists(gemfire_ssh_key):
                    connect_params["key_filename"] = gemfire_ssh_key
                    print(f"[Setup]   Using SSH key: {gemfire_ssh_key}")
                else:
                    print(f"[Setup]   ⚠ No SSH key found - will try password authentication")
                    if gemfire_ssh_key:
                        print(f"[Setup]   Specified key path does not exist: {gemfire_ssh_key}")

                print(f"[Setup]   Connecting to SSH host...")
                gemfire_ssh.connect(**connect_params)
                print(f"[Setup]   ✓ SSH connection established")

                # First check if region already exists
                check_cmd = f'bash -l -c "gfsh -e \\"connect --locator={locator}\\" -e \\"list regions\\""'
                print(f"[Setup]   Checking if region already exists...")
                stdin, stdout, stderr = gemfire_ssh.exec_command(check_cmd, timeout=30)
                check_output = stdout.read().decode("utf-8", errors="ignore") + stderr.read().decode("utf-8", errors="ignore")
                
                if region_name in check_output:
                    print(f"[Setup]   ✓ Region '{region_name}' already exists")
                    return (True, f"Region '{region_name}' already exists")

                # Create the region
                gfsh_cmd = f'bash -l -c "gfsh -e \\"connect --locator={locator}\\" -e \\"create region --name={region_name} --type=REPLICATE\\""'
                print(f"[Setup]   Executing gfsh command to create region...")
                stdin, stdout, stderr = gemfire_ssh.exec_command(gfsh_cmd, timeout=60)
                exit_code = stdout.channel.recv_exit_status()

                output = stdout.read().decode("utf-8", errors="ignore") + stderr.read().decode("utf-8", errors="ignore")

                print(f"[Setup]   gfsh command exit code: {exit_code}")
                print(f"[Setup]   gfsh output:")
                print(f"[Setup]   {output[:1000]}")

                if exit_code == 0 or "successfully" in output.lower() or "created" in output.lower():
                    return (True, f"Region '{region_name}' created successfully")
                elif "already exists" in output.lower():
                    return (True, f"Region '{region_name}' already exists")
                else:
                    return (
                        False,
                        f"Creation failed (exit code: {exit_code}). Output: {output[:200]}",
                    )

            except paramiko.AuthenticationException as e:
                return (False, f"SSH authentication failed: {e}. Check SSH credentials.")
            except paramiko.SSHException as e:
                return (False, f"SSH connection error: {e}")
            except Exception as e:
                return (False, f"SSH error: {e}")
            finally:
                try:
                    gemfire_ssh.close()
                except:
                    pass

        except ImportError:
            return (False, "paramiko library not installed. Install with: pip install paramiko")
        except Exception as e:
            print(f"[Setup] GemFire region creation error: {e}")
            import traceback
            traceback.print_exc()
            return (False, str(e))

    def setup_rabbitmq_infrastructure(self) -> SetupResult:
        """Setup RabbitMQ fanout exchange and dual queues"""
        try:
            rabbitmq_config = self.config.get_rabbitmq_config()

            host = rabbitmq_config["host"]
            vhost = rabbitmq_config["vhost"]
            exchange_name = "network_intrusion_fanout"

            admin_username = os.getenv("RABBITMQ_USERNAME")
            admin_password = os.getenv("RABBITMQ_PASSWORD")
            mgmt_port = int(os.getenv("RABBITMQ_MGMT_PORT", "15672"))
            vhost_encoded = urllib.parse.quote(vhost, safe="")
            base_url = f"http://{host}:{mgmt_port}/api"
            auth = HTTPBasicAuth(admin_username, admin_password)

            print(f"Setting up RabbitMQ infrastructure...")

            # Check if exchange exists
            exchange_encoded = urllib.parse.quote(exchange_name, safe="")
            exchange_url = f"{base_url}/exchanges/{vhost_encoded}/{exchange_encoded}"

            check_response = requests.get(exchange_url, auth=auth, timeout=5)
            exchange_exists = check_response.status_code == 200

            # Create or verify exchange
            if not exchange_exists:
                exchange_config = {
                    "type": "fanout",
                    "auto_delete": False,
                    "durable": True,
                    "internal": False,
                    "arguments": {},
                }

                create_response = requests.put(
                    exchange_url, json=exchange_config, auth=auth, timeout=10
                )

                if create_response.status_code not in [201, 204]:
                    return SetupResult(
                        "RabbitMQ Infrastructure",
                        False,
                        f"Failed to create exchange (status {create_response.status_code})",
                        "error",
                    )
                print(f"Created exchange '{exchange_name}'")
            else:
                print(f"Exchange '{exchange_name}' already exists")

            # Setup queues
            queue_config = {
                "auto_delete": False,
                "durable": True,
                "arguments": {"x-queue-type": "quorum"},
            }

            for queue_name in ["gpss_queue", "gemfire_queue"]:
                queue_encoded = urllib.parse.quote(queue_name, safe="")
                queue_url = f"{base_url}/queues/{vhost_encoded}/{queue_encoded}"

                # Check if queue exists
                check_response = requests.get(queue_url, auth=auth, timeout=5)
                queue_exists = check_response.status_code == 200

                if not queue_exists:
                    create_response = requests.put(
                        queue_url, json=queue_config, auth=auth, timeout=10
                    )

                    if create_response.status_code not in [201, 204]:
                        return SetupResult(
                            "RabbitMQ Infrastructure",
                            False,
                            f"Failed to create queue '{queue_name}'",
                            "error",
                        )
                    print(f"Created queue '{queue_name}'")
                else:
                    print(f"Queue '{queue_name}' already exists")

                # Ensure binding
                binding_url = f"{base_url}/bindings/{vhost_encoded}/e/{exchange_encoded}/q/{queue_encoded}"
                binding_config = {"routing_key": "", "arguments": {}}

                bind_response = requests.post(
                    binding_url, json=binding_config, auth=auth, timeout=10
                )

                if bind_response.status_code not in [201, 204]:
                    print(f"Note: Binding for '{queue_name}' may already exist")

            status_msg = "Exchange 'network_intrusion_fanout' and queues (gpss_queue, gemfire_queue) ready"
            return SetupResult(
                "RabbitMQ Infrastructure",
                True,
                status_msg,
                "exists" if exchange_exists else "created",
            )

        except Exception as e:
            print(f"RabbitMQ infrastructure setup error: {e}")
            import traceback

            traceback.print_exc()

            return SetupResult(
                "RabbitMQ Infrastructure",
                False,
                f"Failed to setup: {str(e)}",
                "error",
            )

    def setup_gemfire_model_region(self) -> SetupResult:
        """Check/create GemFire model region"""
        try:
            region_name = self.gemfire_client.model_region

            # Check if region exists via REST API
            print(f"\n[Setup] Checking GemFire model region '{region_name}'...")
            url = f"{self.gemfire_client.rest_url}/{region_name}"
            print(f"[Setup]   Checking URL: {url}")

            try:
                response = requests.get(url, timeout=5)
                print(f"[Setup]   Response status: {response.status_code}")

                if response.status_code == 200:
                    print(f"[Setup] ✓ Model region '{region_name}' exists and is accessible")
                    return SetupResult(
                        "GemFire Model Region",
                        True,
                        f"Region '{region_name}' exists and is accessible",
                        "exists",
                    )
                elif response.status_code == 404:
                    print(f"[Setup] ✗ Region '{region_name}' does not exist - will attempt to create")
                else:
                    print(f"[Setup] ⚠ Unexpected status code: {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"[Setup] ⚠ Could not verify region via REST API: {e}")
                print(f"[Setup]   This might indicate a connection issue or the region doesn't exist")

            # Region doesn't exist, try to create via SSH
            print(f"[Setup] Attempting to create region '{region_name}' via SSH...")
            success, message = self._create_gemfire_region_via_ssh(region_name)

            if success:
                print(f"[Setup] ✓ Region creation reported success: {message}")
                # Verify it was created
                print(f"[Setup] Waiting 2 seconds for region to be available...")
                time.sleep(2)  # Give GemFire a moment
                
                try:
                    verify_response = requests.get(url, timeout=5)
                    print(f"[Setup]   Verification response status: {verify_response.status_code}")
                    
                    if verify_response.status_code == 200:
                        print(f"[Setup] ✓ Region '{region_name}' created and verified successfully!")
                        return SetupResult(
                            "GemFire Model Region",
                            True,
                            f"Region '{region_name}' created successfully",
                            "created",
                        )
                    else:
                        print(f"[Setup] ⚠ Region created but verification returned status {verify_response.status_code}")
                        print(f"[Setup]   This might be a timing issue - region may be available shortly")
                except Exception as verify_error:
                    print(f"[Setup] ⚠ Could not verify region after creation: {verify_error}")
                    print(f"[Setup]   Region may have been created but is not yet accessible via REST API")

                # Created but couldn't verify via REST - still consider it success
                print(f"[Setup] ⚠ Created but verification failed - you may need to verify manually")
                return SetupResult(
                    "GemFire Model Region",
                    True,
                    f"Region '{region_name}' created (verify manually: gfsh> list regions)",
                    "created",
                )
            else:
                print(f"[Setup] ✗ Failed to create region: {message}")
                print(f"[Setup]   Manual creation required:")
                print(f"[Setup]   gfsh> connect --locator=<locator>")
                print(f"[Setup]   gfsh> create region --name={region_name} --type=REPLICATE")
                return SetupResult(
                    "GemFire Model Region",
                    False,
                    f"Auto-creation failed: {message}. Create manually: gfsh> create region --name={region_name} --type=REPLICATE",
                    "error",
                )

        except Exception as e:
            print(f"[Setup] GemFire model region setup error: {e}")
            import traceback
            traceback.print_exc()

            return SetupResult(
                "GemFire Model Region",
                False,
                f"Setup error: {str(e)}. Create manually: gfsh> create region --name={region_name} --type=REPLICATE",
                "error",
            )

    def setup_gemfire_prediction_cache(self) -> SetupResult:
        """Check/create GemFire prediction cache region"""
        try:
            region_name = self.gemfire_client.prediction_region

            # Check if region exists via REST API
            print(f"\n[Setup] Checking GemFire prediction cache region '{region_name}'...")
            url = f"{self.gemfire_client.rest_url}/{region_name}"
            print(f"[Setup]   Checking URL: {url}")

            try:
                response = requests.get(url, timeout=5)
                print(f"[Setup]   Response status: {response.status_code}")

                if response.status_code == 200:
                    print(f"[Setup] ✓ Prediction cache region '{region_name}' exists and is accessible")
                    return SetupResult(
                        "GemFire Prediction Cache",
                        True,
                        f"Region '{region_name}' exists and is accessible",
                        "exists",
                    )
                elif response.status_code == 404:
                    print(f"[Setup] ✗ Region '{region_name}' does not exist - will attempt to create")
                else:
                    print(f"[Setup] ⚠ Unexpected status code: {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"[Setup] ⚠ Could not verify region via REST API: {e}")
                print(f"[Setup]   This might indicate a connection issue or the region doesn't exist")

            # Region doesn't exist, try to create via SSH
            print(f"[Setup] Attempting to create region '{region_name}' via SSH...")
            success, message = self._create_gemfire_region_via_ssh(region_name)

            if success:
                print(f"[Setup] ✓ Region creation reported success: {message}")
                # Verify it was created
                print(f"[Setup] Waiting 2 seconds for region to be available...")
                time.sleep(2)
                
                try:
                    verify_response = requests.get(url, timeout=5)
                    print(f"[Setup]   Verification response status: {verify_response.status_code}")
                    
                    if verify_response.status_code == 200:
                        print(f"[Setup] ✓ Region '{region_name}' created and verified successfully!")
                        return SetupResult(
                            "GemFire Prediction Cache",
                            True,
                            f"Region '{region_name}' created successfully",
                            "created",
                        )
                    else:
                        print(f"[Setup] ⚠ Region created but verification returned status {verify_response.status_code}")
                        print(f"[Setup]   This might be a timing issue - region may be available shortly")
                except Exception as verify_error:
                    print(f"[Setup] ⚠ Could not verify region after creation: {verify_error}")
                    print(f"[Setup]   Region may have been created but is not yet accessible via REST API")

                # Created but couldn't verify via REST - still consider it success
                print(f"[Setup] ⚠ Created but verification failed - you may need to verify manually")
                return SetupResult(
                    "GemFire Prediction Cache",
                    True,
                    f"Region '{region_name}' created (verify manually: gfsh> list regions)",
                    "created",
                )
            else:
                print(f"[Setup] ✗ Failed to create region: {message}")
                print(f"[Setup]   Manual creation required:")
                print(f"[Setup]   gfsh> connect --locator=<locator>")
                print(f"[Setup]   gfsh> create region --name={region_name} --type=REPLICATE")
                return SetupResult(
                    "GemFire Prediction Cache",
                    False,
                    f"Auto-creation failed: {message}. Create manually: gfsh> create region --name={region_name} --type=REPLICATE",
                    "error",
                )

        except Exception as e:
            print(f"[Setup] GemFire prediction cache setup error: {e}")
            import traceback
            traceback.print_exc()

            return SetupResult(
                "GemFire Prediction Cache",
                False,
                f"Setup error: {str(e)}. Create manually: gfsh> create region --name={region_name} --type=REPLICATE",
                "error",
            )

    def check_gemfire_model(self) -> SetupResult:
        """Check if model exists in GemFire (warning if not present)"""
        try:
            model_exists = self.gemfire_client.check_model_exists()

            if model_exists:
                return SetupResult(
                    "GemFire Model",
                    True,
                    f"Model found in GemFire (region: {self.gemfire_client.model_region}, key: {self.gemfire_client.model_key})",
                    "exists",
                )
            else:
                return SetupResult(
                    "GemFire Model",
                    True,
                    f"Model not found in GemFire. Upload best model using 'Upload Best KDD Model' step",
                    "warning",
                )
        except Exception as e:
            return SetupResult(
                "GemFire Model",
                True,
                f"Could not check model: {str(e)}. Model can be uploaded later",
                "warning",
            )

    def setup_greenplum_table(self) -> SetupResult:
        """Check/create Greenplum table"""
        try:
            gp_config = self.config.get_greenplum_config()

            conn = psycopg2.connect(
                host=gp_config["host"],
                port=gp_config["port"],
                database=gp_config["database"],
                user=gp_config["user"],
                password=gp_config["password"],
            )

            cursor = conn.cursor()

            # Check if table exists
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

            if table_exists:
                cursor.close()
                conn.close()
                return SetupResult(
                    "Greenplum Table",
                    True,
                    f"Table {gp_config['schema']}.{gp_config['table']} already exists",
                    "exists",
                )

            # Create table from DDL file
            ddl_path = Path("gpss/create_table.sql")

            if not ddl_path.exists():
                cursor.close()
                conn.close()
                return SetupResult(
                    "Greenplum Table", False, f"DDL file not found: {ddl_path}", "error"
                )

            with open(ddl_path, "r") as f:
                ddl = f.read()

            cursor.execute(ddl)
            conn.commit()
            cursor.close()
            conn.close()

            return SetupResult(
                "Greenplum Table",
                True,
                f"Table {gp_config['schema']}.{gp_config['table']} created successfully",
                "created",
            )

        except Exception as e:
            return SetupResult("Greenplum Table", False, f"Error: {str(e)}", "error")

    def generate_gpss_yaml(self) -> SetupResult:
        """Generate GPSS YAML with updated queue name (gpss_queue)"""
        try:
            template_path = Path("config/load_kdd.yaml.template")
            output_path = Path("config/load_kdd.yaml")

            if not template_path.exists():
                return SetupResult(
                    "GPSS YAML",
                    False,
                    f"Template file not found: {template_path}",
                    "error",
                )

            with open(template_path, "r") as f:
                yaml_content = f.read()

            # Replace placeholders
            greenplum_config = self.config.get_greenplum_config()
            rabbitmq_config = self.config.get_rabbitmq_config()

            yaml_content = yaml_content.replace(
                "{{GREENPLUM_HOST}}", greenplum_config["host"]
            )
            yaml_content = yaml_content.replace(
                "{{GREENPLUM_PORT}}", str(greenplum_config["port"])
            )
            yaml_content = yaml_content.replace(
                "{{GREENPLUM_DATABASE}}", greenplum_config["database"]
            )
            yaml_content = yaml_content.replace(
                "{{GREENPLUM_USER}}", greenplum_config["user"]
            )
            yaml_content = yaml_content.replace(
                "{{GREENPLUM_PASSWORD}}", greenplum_config["password"]
            )
            yaml_content = yaml_content.replace(
                "{{GREENPLUM_SCHEMA}}", greenplum_config["schema"]
            )
            yaml_content = yaml_content.replace(
                "{{GREENPLUM_TABLE}}", greenplum_config["table"]
            )

            yaml_content = yaml_content.replace(
                "{{RABBITMQ_HOST}}", rabbitmq_config["host"]
            )
            yaml_content = yaml_content.replace(
                "{{RABBITMQ_PORT}}", str(rabbitmq_config["port"])
            )
            yaml_content = yaml_content.replace(
                "{{RABBITMQ_USERNAME}}", rabbitmq_config["username"]
            )
            yaml_content = yaml_content.replace(
                "{{RABBITMQ_PASSWORD}}", rabbitmq_config["password"]
            )
            yaml_content = yaml_content.replace(
                "{{RABBITMQ_VHOST}}", rabbitmq_config["vhost"]
            )

            # Use gpss_queue with underscore
            yaml_content = yaml_content.replace("{{RABBITMQ_QUEUE}}", "gpss_queue")

            with open(output_path, "w") as f:
                f.write(yaml_content)

            return SetupResult(
                "GPSS YAML",
                True,
                f"GPSS configuration generated at {output_path}",
                "created",
            )

        except Exception as e:
            return SetupResult("GPSS YAML", False, f"Error: {str(e)}", "error")

    def setup_gpss_job(self) -> SetupResult:
        """Setup GPSS job (check/start GPSS server, then submit job)"""
        try:
            if not self.ssh_client:
                return SetupResult(
                    "GPSS Job",
                    False,
                    "SSH client not configured. Set GREENPLUM_SSH_HOST, GREENPLUM_SSH_USER, and GREENPLUM_SSH_KEY_PATH in .env",
                    "error",
                )

            # Ensure SSH connection is established
            print(f"Establishing SSH connection to Greenplum host...")
            if not self.ssh_client.connect():
                return SetupResult(
                    "GPSS Job",
                    False,
                    f"Failed to connect to Greenplum host via SSH. Check SSH credentials in .env",
                    "error",
                )
            print(f"✓ SSH connection established")

            gpss_config = self.config.get_gpss_config()
            job_name = gpss_config["job_name"]
            gpss_remote_dir = gpss_config["gpss_dir"]
            gpss_remote_config = f"{gpss_remote_dir}/gpss_config.json"

            # Step 1: Check if GPSS is installed
            print(f"Checking if GPSS is installed on remote host...")
            is_installed, install_msg = self.ssh_client.check_gpss_installed()
            if not is_installed:
                return SetupResult(
                    "GPSS Job",
                    False,
                    f"GPSS is not installed on the Greenplum host: {install_msg}",
                    "error",
                )
            print(f"✓ GPSS is installed: {install_msg}")

            # Step 2: Check if GPSS is running
            print(f"Checking if GPSS server is running on remote host...")
            is_running, run_msg = self.ssh_client.check_gpss_running()
            
            if not is_running:
                print(f"GPSS server is not running. Starting GPSS on Greenplum host...")
                
                # Ensure remote directory exists
                if not self.ssh_client.directory_exists(gpss_remote_dir):
                    print(f"Creating remote directory: {gpss_remote_dir}")
                    success, msg = self.ssh_client.create_directory(gpss_remote_dir, as_gpadmin=True)
                    if not success:
                        print(f"Warning: Could not create directory: {msg}")
                
                # Upload GPSS config file if it exists locally
                local_config = Path("gpss/gpss_config.json")
                if local_config.exists():
                    print(f"Uploading GPSS config file to {gpss_remote_config}...")
                    config_uploaded = self.ssh_client.upload_file(
                        str(local_config), gpss_remote_config, as_gpadmin=True
                    )
                    if not config_uploaded:
                        return SetupResult(
                            "GPSS Job",
                            False,
                            f"Failed to upload GPSS config file to remote host. Check SSH permissions.",
                            "error",
                        )
                    print(f"✓ GPSS config uploaded")
                else:
                    print(f"Warning: Local GPSS config not found at {local_config}, will use existing remote config")
                
                # Start GPSS server
                print(f"Starting GPSS server with config: {gpss_remote_config}")
                success, message = self.ssh_client.start_gpss(gpss_remote_config)
                if not success:
                    return SetupResult(
                        "GPSS Job",
                        False,
                        f"Failed to start GPSS server on Greenplum host: {message}\n"
                        f"Check GPSS logs on remote host: /tmp/gpss.log",
                        "error",
                    )
                print(f"✓ GPSS server started successfully")
            else:
                print(f"✓ GPSS server is already running: {run_msg}")

            # Step 2: Upload YAML file
            local_yaml = Path("config/load_kdd.yaml")
            if not local_yaml.exists():
                return SetupResult(
                    "GPSS Job",
                    False,
                    "GPSS YAML not found. Run 'Generate GPSS YAML' first",
                    "error",
                )

            print(f"Uploading GPSS YAML to remote host...")
            success = self.ssh_client.upload_file(
                str(local_yaml), f"{gpss_remote_dir}/load_kdd.yaml"
            )

            if not success:
                return SetupResult(
                    "GPSS Job", False, "Failed to upload GPSS YAML", "error"
                )

            # Step 3: Stop and remove existing job if present
            print(f"Stopping GPSS job '{job_name}' if running...")
            try:
                self.ssh_client.stop_gpss_job(job_name)
            except Exception as e:
                print(f"Note: Could not stop job (may not be running): {e}")

            print(f"Removing existing job '{job_name}'...")
            try:
                self.ssh_client.remove_gpss_job(job_name)
            except Exception as e:
                print(f"Note: Could not remove job (may not exist): {e}")

            # Step 4: Submit fresh job
            print(f"Submitting fresh GPSS job '{job_name}'...")
            yaml_path = f"{gpss_remote_dir}/load_kdd.yaml"
            success, message = self.ssh_client.submit_gpss_job(job_name, yaml_path)

            if success:
                return SetupResult(
                    "GPSS Job",
                    True,
                    f"Job '{job_name}' submitted successfully",
                    "created",
                )
            else:
                return SetupResult(
                    "GPSS Job", False, f"Failed to submit job: {message}", "error"
                )

        except Exception as e:
            return SetupResult("GPSS Job", False, f"Error: {str(e)}", "error")

    def start_gpss_job(self) -> SetupResult:
        """Start GPSS job"""
        try:
            if not self.ssh_client:
                return SetupResult(
                    "GPSS Job Start", False, "SSH client not configured", "error"
                )

            gpss_config = self.config.get_gpss_config()
            job_name = gpss_config["job_name"]

            # Check current status
            status = self.ssh_client.get_gpss_job_status(job_name)

            if "RUNNING" in status:
                return SetupResult(
                    "GPSS Job Start",
                    True,
                    f"Job '{job_name}' is already running",
                    "exists",
                )

            # Start the job
            success, message = self.ssh_client.start_gpss_job(job_name)

            if success:
                return SetupResult(
                    "GPSS Job Start",
                    True,
                    f"Job '{job_name}' started successfully 🎉",
                    "created",
                )
            else:
                return SetupResult(
                    "GPSS Job Start", False, f"Failed to start job: {message}", "error"
                )

        except Exception as e:
            return SetupResult("GPSS Job Start", False, f"Error: {str(e)}", "error")

    def run_all_setup(self) -> Dict[str, SetupResult]:
        """Run all setup steps in sequence"""
        results = {}

        # 1. RabbitMQ Infrastructure (exchange + queues)
        print("\n=== Step 1: RabbitMQ Infrastructure ===")
        results["rabbitmq_infrastructure"] = self.setup_rabbitmq_infrastructure()

        # 2. GemFire Prediction Cache (for caching prediction results)
        print("\n=== Step 2: GemFire Prediction Cache ===")
        results["gemfire_prediction_cache"] = self.setup_gemfire_prediction_cache()

        # 3. Greenplum Table
        print("\n=== Step 3: Greenplum Table ===")
        results["greenplum_table"] = self.setup_greenplum_table()

        # 4. GPSS YAML Generation
        print("\n=== Step 4: GPSS YAML ===")
        results["gpss_yaml"] = self.generate_gpss_yaml()

        # 5-6. GPSS Job Setup and Start (only if YAML generated successfully)
        if results["gpss_yaml"].success:
            print("\n=== Step 5: GPSS Job ===")
            results["gpss_job"] = self.setup_gpss_job()

            if results["gpss_job"].success:
                print("\n=== Step 6: GPSS Job Start ===")
                results["gpss_job_start"] = self.start_gpss_job()

        return results

    def get_setup_status(self) -> Dict[str, bool]:
        """Get current setup status"""
        return {
            "rabbitmq_infrastructure": True,
            "gemfire_prediction_cache": True,
            "greenplum_table": True,
            "gpss": True,
        }
