"""
SSH Client for Remote GPSS Operations
Handles SSH connections to Greenplum host where GPSS runs
FIXED: Now handles both RSA and OpenSSH key formats
"""

import paramiko
import os
from typing import Tuple, Optional
from src.config_manager import get_config


class SSHClient:
    """SSH client for executing GPSS commands on remote Greenplum host"""

    def __init__(self):
        self.config = get_config()
        self.ssh_config = self._get_ssh_config()
        self.client = None

    def _get_ssh_config(self) -> dict:
        """Get SSH configuration from environment"""
        return {
            "host": self.config.get("GREENPLUM_SSH_HOST")
            or self.config.get("GREENPLUM_HOST"),
            "port": int(self.config.get("GREENPLUM_SSH_PORT", 22)),
            "username": self.config.get("GREENPLUM_SSH_USER")
            or self.config.get("GREENPLUM_USER"),
            "key_path": self.config.get("GREENPLUM_SSH_KEY_PATH"),
            "password": self.config.get("GREENPLUM_SSH_PASSWORD"),
            "gpss_remote_dir": self.config.get("GPSS_REMOTE_DIR", "/home/gpadmin/gpss"),
            "gpss_remote_config": self.config.get(
                "GPSS_REMOTE_CONFIG", "/home/gpadmin/gpss/gpss_config.json"
            ),
        }

    def _load_private_key(self, key_path: str):
        """
        Load private key with support for multiple formats
        Tries RSA, DSS, ECDSA, and Ed25519 formats
        """
        # Try different key types
        key_types = [
            ("RSA", paramiko.RSAKey),
            ("DSS", paramiko.DSSKey),
            ("ECDSA", paramiko.ECDSAKey),
            ("Ed25519", paramiko.Ed25519Key),
        ]

        last_error = None
        for key_name, key_class in key_types:
            try:
                key = key_class.from_private_key_file(key_path)
                print(f"  Successfully loaded {key_name} key")
                return key
            except paramiko.ssh_exception.SSHException as e:
                last_error = e
                continue
            except Exception as e:
                last_error = e
                continue

        # If all failed, raise the last error
        if last_error:
            raise last_error
        else:
            raise Exception(f"Could not load key from {key_path}")

    def connect(self) -> bool:
        """Establish SSH connection to Greenplum host"""
        try:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            connect_params = {
                "hostname": self.ssh_config["host"],
                "port": self.ssh_config["port"],
                "username": self.ssh_config["username"],
                "timeout": 10,
                "allow_agent": False,  # Don't use SSH agent
                "look_for_keys": False,  # Don't look in default locations
            }

            # Use key or password
            if self.ssh_config["key_path"] and os.path.exists(
                self.ssh_config["key_path"]
            ):
                try:
                    # Load the key explicitly to handle different formats
                    pkey = self._load_private_key(self.ssh_config["key_path"])
                    connect_params["pkey"] = pkey
                except Exception as e:
                    print(f"Error loading SSH key: {str(e)}")
                    print(f"Falling back to key_filename method...")
                    # Fall back to letting paramiko handle it
                    connect_params["key_filename"] = self.ssh_config["key_path"]

            elif self.ssh_config["password"]:
                connect_params["password"] = self.ssh_config["password"]
            else:
                raise ValueError("No SSH authentication configured")

            self.client.connect(**connect_params)
            return True

        except paramiko.AuthenticationException as e:
            print(f"SSH authentication failed: {str(e)}")
            return False
        except paramiko.SSHException as e:
            print(f"SSH connection error: {str(e)}")
            return False
        except Exception as e:
            print(f"Error connecting via SSH: {str(e)}")
            return False

    def disconnect(self):
        """Close SSH connection"""
        if self.client:
            self.client.close()
            self.client = None

    def execute_command(
        self, command: str, timeout: int = 30, as_gpadmin: bool = False
    ) -> Tuple[int, str, str]:
        """
        Execute command on remote host

        Args:
            command: Command to execute
            timeout: Command timeout in seconds
            as_gpadmin: If True, execute as gpadmin with Greenplum environment

        Returns:
            Tuple of (exit_code, stdout, stderr)
        """
        if not self.client:
            if not self.connect():
                return (-1, "", "SSH connection failed")

        try:
            # If as_gpadmin is True, wrap command to run as gpadmin with GP environment
            if as_gpadmin:
                # Change to gpadmin's home directory first to avoid permission issues
                command = f"sudo -u gpadmin bash -c 'cd /home/gpadmin && source /usr/local/greenplum-db/greenplum_path.sh && {command}'"

            stdin, stdout, stderr = self.client.exec_command(command, timeout=timeout)
            exit_code = stdout.channel.recv_exit_status()

            stdout_text = stdout.read().decode("utf-8", errors="ignore")
            stderr_text = stderr.read().decode("utf-8", errors="ignore")

            return (exit_code, stdout_text, stderr_text)

        except paramiko.SSHException as e:
            return (-1, "", f"SSH error: {str(e)}")
        except Exception as e:
            return (-1, "", f"Error executing command: {str(e)}")

    def create_directory(
        self, remote_path: str, as_gpadmin: bool = True
    ) -> Tuple[bool, str]:
        """Create directory on remote host"""
        if as_gpadmin:
            # Create as gpadmin user with sudo - don't source GP environment for mkdir
            command = f"sudo -u gpadmin mkdir -p {remote_path}"
        else:
            command = f"mkdir -p {remote_path}"

        # Execute command directly (not as_gpadmin because we already have sudo in command)
        exit_code, stdout, stderr = self.execute_command(command, as_gpadmin=False)

        if exit_code == 0:
            return True, f"Directory created: {remote_path}"
        else:
            return False, f"Failed to create directory: {stderr}"

    def directory_exists(self, remote_path: str, as_gpadmin: bool = True) -> bool:
        """Check if directory exists on remote host"""
        command = f"test -d {remote_path} && echo 'exists' || echo 'not found'"
        exit_code, stdout, stderr = self.execute_command(command, as_gpadmin=as_gpadmin)

        return "exists" in stdout.lower()

    def file_exists(self, remote_path: str, as_gpadmin: bool = True) -> bool:
        """Check if file exists on remote host"""
        command = f"test -f {remote_path} && echo 'exists' || echo 'not found'"
        exit_code, stdout, stderr = self.execute_command(command, as_gpadmin=as_gpadmin)

        return "exists" in stdout.lower()

    def upload_file(
        self, local_path: str, remote_path: str, as_gpadmin: bool = True
    ) -> Tuple[bool, str]:
        """Upload file to remote host via SFTP"""
        if not self.client:
            if not self.connect():
                return False, "SSH connection failed"

        try:
            sftp = self.client.open_sftp()

            # Ensure remote directory exists
            remote_dir = os.path.dirname(remote_path)
            if not self.directory_exists(remote_dir, as_gpadmin=as_gpadmin):
                success, message = self.create_directory(
                    remote_dir, as_gpadmin=as_gpadmin
                )
                if not success:
                    return False, f"Failed to create remote directory: {message}"

            # Upload to temp location first (as ec2-user)
            temp_path = f"/tmp/{os.path.basename(remote_path)}"
            sftp.put(local_path, temp_path)
            sftp.close()

            # Move to final location
            if as_gpadmin:
                # Move as root (has permission to move any file)
                move_cmd = f"sudo mv {temp_path} {remote_path}"
                exit_code, stdout, stderr = self.execute_command(
                    move_cmd, as_gpadmin=False
                )
                if exit_code != 0:
                    return False, f"Failed to move file to {remote_path}: {stderr}"

                # Set ownership to gpadmin
                chown_cmd = f"sudo chown gpadmin:gpadmin {remote_path}"
                exit_code, stdout, stderr = self.execute_command(
                    chown_cmd, as_gpadmin=False
                )
                if exit_code != 0:
                    return False, f"Failed to set ownership: {stderr}"
            else:
                move_cmd = f"mv {temp_path} {remote_path}"
                exit_code, stdout, stderr = self.execute_command(
                    move_cmd, as_gpadmin=False
                )
                if exit_code != 0:
                    return False, f"Failed to move file: {stderr}"

            return True, f"Uploaded {local_path} to {remote_path}"

        except Exception as e:
            return False, f"Failed to upload file: {str(e)}"

    def check_gpss_installed(self) -> Tuple[bool, str]:
        """Check if GPSS is installed on remote host"""
        exit_code, stdout, stderr = self.execute_command(
            "which gpsscli", as_gpadmin=True
        )

        if exit_code == 0 and stdout.strip():
            return True, f"GPSS installed at: {stdout.strip()}"
        else:
            return (
                False,
                "gpsscli command not found (checked as gpadmin with Greenplum environment)",
            )

    def check_gpss_running(self) -> Tuple[bool, str]:
        """Check if GPSS server is running on remote host"""
        exit_code, stdout, stderr = self.execute_command(
            "gpsscli list", timeout=10, as_gpadmin=True
        )

        if exit_code == 0:
            return True, "GPSS is running"

        # Parse error message
        error_output = stderr + stdout

        if "connection refused" in error_output.lower():
            return False, "GPSS server not running (connection refused on port 5000)"
        elif (
            "connection error" in error_output.lower()
            or "dial tcp" in error_output.lower()
        ):
            return False, "GPSS server not responding"
        elif "command not found" in error_output.lower():
            return False, "gpsscli command not found"
        else:
            return False, f"GPSS error: {error_output[:100]}"

    def start_gpss(self, config_path: str = None) -> Tuple[bool, str]:
        """Start GPSS server on remote host"""
        if config_path is None:
            config_path = self.ssh_config["gpss_remote_config"]

        # Check if GPSS is already running
        is_running, _ = self.check_gpss_running()
        if is_running:
            return True, "GPSS server is already running"

        # Check if config file exists
        test_cmd = f"test -f {config_path} && echo 'exists' || echo 'not found'"
        test_exit, test_out, test_err = self.execute_command(test_cmd, as_gpadmin=True)
        if "not found" in test_out.lower():
            return False, f"GPSS config file not found at {config_path}"

        # Command to start GPSS in background (as gpadmin with proper environment)
        # Use source to load Greenplum environment if available
        command = (
            f"source $GPHOME/greenplum_path.sh 2>/dev/null || true; "
            f"nohup gpss -c {config_path} > /tmp/gpss.log 2>&1 &"
        )

        exit_code, stdout, stderr = self.execute_command(command, as_gpadmin=True)

        if exit_code == 0:
            # Wait a moment for GPSS to start
            import time
            time.sleep(5)  # Give GPSS more time to start

            # Check if it started successfully
            is_running, message = self.check_gpss_running()
            if is_running:
                return True, "GPSS server started successfully"
            else:
                # Try to get error from log file
                log_cmd = "tail -20 /tmp/gpss.log 2>/dev/null || echo 'Log file not accessible'"
                log_exit, log_out, _ = self.execute_command(log_cmd, as_gpadmin=False)
                log_info = log_out.strip() if log_out else "No log information available"
                return False, f"GPSS started but not responding: {message}\nLast log entries:\n{log_info}"
        else:
            # Try to get error from log file
            log_cmd = "tail -20 /tmp/gpss.log 2>/dev/null || echo 'Log file not accessible'"
            log_exit, log_out, _ = self.execute_command(log_cmd, as_gpadmin=False)
            log_info = log_out.strip() if log_out else "No log information available"
            error_msg = stderr.strip() if stderr.strip() else stdout.strip()
            return False, f"Failed to start GPSS: {error_msg}\nLast log entries:\n{log_info}"

    def stop_gpss(self) -> Tuple[bool, str]:
        """Stop GPSS server on remote host"""
        command = "pkill -f 'gpss -c'"
        exit_code, stdout, stderr = self.execute_command(command, as_gpadmin=True)

        if exit_code == 0 or exit_code == 1:  # 1 means no process found
            return True, "GPSS server stopped"
        else:
            return False, f"Failed to stop GPSS: {stderr}"

    def submit_gpss_job(self, job_name: str, yaml_path: str) -> Tuple[bool, str]:
        """Submit GPSS streaming job"""
        # First check if YAML file is readable
        test_cmd = f"test -r {yaml_path} && echo 'readable' || echo 'not readable'"
        test_exit, test_out, test_err = self.execute_command(test_cmd, as_gpadmin=True)

        if "not readable" in test_out.lower():
            # Try to get more info
            ls_cmd = f"ls -la {yaml_path}"
            ls_exit, ls_out, ls_err = self.execute_command(ls_cmd, as_gpadmin=True)
            return (
                False,
                f"YAML file not readable. File info: {ls_out if ls_out else ls_err}",
            )

        command = f"gpsscli submit --name {job_name} {yaml_path}"
        exit_code, stdout, stderr = self.execute_command(command, as_gpadmin=True)

        if exit_code == 0:
            return True, f"Job '{job_name}' submitted successfully"
        else:
            # Combine stdout and stderr for better error diagnosis
            error_output = stderr.strip() if stderr.strip() else stdout.strip()
            if not error_output:
                error_output = (
                    f"Command exited with code {exit_code} but no error message"
                )
            return False, f"Failed to submit job: {error_output}"

    def list_gpss_jobs(self) -> Tuple[bool, str]:
        """List all GPSS jobs"""
        exit_code, stdout, stderr = self.execute_command(
            "gpsscli list", as_gpadmin=True
        )

        if exit_code == 0:
            return True, stdout
        else:
            return False, f"Failed to list jobs: {stderr}"

    def get_gpss_job_status(self, job_name: str) -> str:
        """Get status of a GPSS job"""
        command = f"gpsscli status {job_name}"
        exit_code, stdout, stderr = self.execute_command(command, as_gpadmin=True)

        if exit_code == 0:
            return stdout
        else:
            return f"Error getting status: {stderr}"

    def start_gpss_job(self, job_name: str) -> Tuple[bool, str]:
        """Start a GPSS job"""
        command = f"gpsscli start {job_name}"
        exit_code, stdout, stderr = self.execute_command(command, as_gpadmin=True)

        if exit_code == 0:
            return True, f"Job '{job_name}' started successfully"
        else:
            return False, f"Failed to start job: {stderr}"

    def stop_gpss_job(self, job_name: str) -> Tuple[bool, str]:
        """Stop a GPSS job"""
        command = f"gpsscli stop {job_name}"
        exit_code, stdout, stderr = self.execute_command(command, as_gpadmin=True)

        if exit_code == 0:
            return True, f"Job '{job_name}' stopped successfully"
        else:
            return False, f"Failed to stop job: {stderr}"

    def remove_gpss_job(self, job_name: str) -> Tuple[bool, str]:
        """Remove GPSS job"""
        command = f"gpsscli remove {job_name}"
        exit_code, stdout, stderr = self.execute_command(command, as_gpadmin=True)

        if exit_code == 0:
            return True, f"Job '{job_name}' removed successfully"
        else:
            return False, f"Failed to remove job: {stderr}"


# Singleton instance
_ssh_client = None


def get_ssh_client() -> SSHClient:
    """Get singleton SSHClient instance"""
    global _ssh_client
    if _ssh_client is None:
        _ssh_client = SSHClient()
    return _ssh_client
