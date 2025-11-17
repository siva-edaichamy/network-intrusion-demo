"""
Pre-flight Checker
Tests connectivity to all required services before starting the demo
Checks: RabbitMQ, Greenplum, GemFire, SSH connectivity, GPSS availability, and datasets (simulation & training)
"""

import os
import socket
import requests
import pika
import psycopg2
from typing import Dict, Any, List
from src.config_manager import get_config
from src.ssh_client import SSHClient, get_ssh_client


class CheckResult:
    """Result of a pre-flight check"""

    def __init__(self, name: str, success: bool, message: str, details: Dict = None):
        self.name = name
        self.success = success
        self.message = message
        self.details = details or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "success": self.success,
            "message": self.message,
            "details": self.details,
        }


class PreflightChecker:
    """Runs pre-flight checks on all required services"""

    def __init__(self):
        self.config = get_config()
        self.results: List[CheckResult] = []

    def run_all_checks(self) -> Dict[str, Any]:
        """Run all pre-flight checks and return summary"""
        print("Running pre-flight checks...")
        print("=" * 60)

        # Core service checks
        self.check_rabbitmq()
        self.check_greenplum()
        self.check_gemfire()

        # SSH and GPSS checks
        self.check_ssh_connectivity()
        self.check_gpss_availability()

        # File system checks
        self.check_dataset()

        # Print summary
        self._print_summary()

        # Return summary dictionary
        return self.get_summary()

    def check_rabbitmq(self) -> CheckResult:
        """Test RabbitMQ connectivity"""
        print("\n[1/6] Checking RabbitMQ connectivity...")

        try:
            config = self.config.get_rabbitmq_config()

            # Test connection
            credentials = pika.PlainCredentials(config["username"], config["password"])
            parameters = pika.ConnectionParameters(
                host=config["host"],
                port=config["port"],
                virtual_host=config["vhost"],
                credentials=credentials,
                connection_attempts=3,
                retry_delay=2,
            )

            connection = pika.BlockingConnection(parameters)
            connection.close()

            result = CheckResult(
                name="RabbitMQ",
                success=True,
                message=f"Connected to {config['host']}:{config['port']}",
                details={"host": config["host"], "port": config["port"]},
            )
            print(f"  ✓ {result.message}")

        except Exception as e:
            result = CheckResult(
                name="RabbitMQ",
                success=False,
                message=f"Failed to connect: {str(e)}",
                details={"error": str(e)},
            )
            print(f"  ✗ {result.message}")

        self.results.append(result)
        return result

    def check_greenplum(self) -> CheckResult:
        """Test Greenplum connectivity"""
        print("\n[2/6] Checking Greenplum connectivity...")

        try:
            config = self.config.get_greenplum_config()

            # Test database connection
            conn = psycopg2.connect(
                host=config["host"],
                port=config["port"],
                database=config["database"],
                user=config["user"],
                password=config["password"],
                connect_timeout=10,
            )

            # Test query
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            cursor.close()
            conn.close()

            result = CheckResult(
                name="Greenplum",
                success=True,
                message=f"Connected to {config['host']}:{config['port']}/{config['database']}",
                details={
                    "host": config["host"],
                    "port": config["port"],
                    "version_info": version[:50],
                },
            )
            print(f"  ✓ {result.message}")

        except Exception as e:
            result = CheckResult(
                name="Greenplum",
                success=False,
                message=f"Failed to connect: {str(e)}",
                details={"error": str(e)},
            )
            print(f"  ✗ {result.message}")

        self.results.append(result)
        return result

    def check_gemfire(self) -> CheckResult:
        """Test GemFire connectivity"""
        print("\n[3/6] Checking GemFire connectivity...")

        try:
            config = self.config.get_gemfire_config()
            rest_url = config.get("rest_url")

            if not rest_url:
                raise ValueError("GemFire rest_url not configured")

            # Test REST API endpoint
            response = requests.get(f"{rest_url}/ping", timeout=10)

            if response.status_code == 200:
                result = CheckResult(
                    name="GemFire",
                    success=True,
                    message=f"Connected to {rest_url}",
                    details={"url": rest_url},
                )
                print(f"  ✓ {result.message}")
            else:
                result = CheckResult(
                    name="GemFire",
                    success=False,
                    message=f"Unexpected status code: {response.status_code}",
                    details={"status_code": response.status_code},
                )
                print(f"  ✗ {result.message}")

        except Exception as e:
            result = CheckResult(
                name="GemFire",
                success=False,
                message=f"Failed to connect: {str(e)}",
                details={"error": str(e)},
            )
            print(f"  ✗ {result.message}")

        self.results.append(result)
        return result

    def check_ssh_connectivity(self) -> CheckResult:
        """Test SSH connectivity to Greenplum host"""
        print("\n[4/6] Checking SSH connectivity to Greenplum host...")

        try:
            # First validate SSH configuration
            ssh_host = self.config.get("GREENPLUM_SSH_HOST") or self.config.get(
                "GREENPLUM_HOST"
            )
            ssh_user = self.config.get("GREENPLUM_SSH_USER") or self.config.get(
                "GREENPLUM_USER"
            )
            ssh_key_path = self.config.get("GREENPLUM_SSH_KEY_PATH")
            ssh_password = self.config.get("GREENPLUM_SSH_PASSWORD")

            print(f"  SSH Host: {ssh_host}")
            print(f"  SSH User: {ssh_user}")
            print(f"  SSH Key: {ssh_key_path if ssh_key_path else 'Not configured'}")
            print(
                f"  SSH Password: {'Configured' if ssh_password else 'Not configured'}"
            )

            # Validate credentials are configured
            if not ssh_key_path and not ssh_password:
                raise ValueError(
                    "No SSH authentication configured. Set either GREENPLUM_SSH_KEY_PATH or GREENPLUM_SSH_PASSWORD in .env"
                )

            # If using key, validate file exists and has proper permissions
            if ssh_key_path:
                if not os.path.exists(ssh_key_path):
                    raise FileNotFoundError(f"SSH key file not found: {ssh_key_path}")

                # Check permissions (should be 600 or 400)
                file_stat = os.stat(ssh_key_path)
                file_perms = oct(file_stat.st_mode)[-3:]
                if file_perms not in ["600", "400"]:
                    print(
                        f"  ⚠ Warning: SSH key has permissions {file_perms}, should be 600 or 400"
                    )
                    print(f"  Run: chmod 600 {ssh_key_path}")

            # Get SSH client
            ssh_client = get_ssh_client()

            # Test connection
            print(f"  Attempting SSH connection to {ssh_host}...")
            if not ssh_client.connect():
                raise Exception("Failed to establish SSH connection")

            # Test command execution
            exit_code, stdout, stderr = ssh_client.execute_command(
                "echo 'SSH test successful'"
            )

            if exit_code != 0:
                raise Exception(f"SSH command failed: {stderr}")

            # Get remote host info
            exit_code, hostname, _ = ssh_client.execute_command("hostname")
            exit_code, whoami, _ = ssh_client.execute_command("whoami")

            result = CheckResult(
                name="SSH Connectivity",
                success=True,
                message=f"SSH connection established to {hostname.strip()} as {whoami.strip()}",
                details={
                    "hostname": hostname.strip(),
                    "user": whoami.strip(),
                    "test_output": stdout.strip(),
                },
            )
            print(f"  ✓ {result.message}")

        except Exception as e:
            result = CheckResult(
                name="SSH Connectivity",
                success=False,
                message=f"SSH connection failed: {str(e)}",
                details={"error": str(e)},
            )
            print(f"  ✗ {result.message}")

        self.results.append(result)
        return result

    def check_gpss_availability(self) -> CheckResult:
        """Test GPSS availability on remote host"""
        print("\n[5/6] Checking GPSS availability...")

        try:
            ssh_client = get_ssh_client()

            # Ensure SSH is connected
            if (
                not ssh_client.client
                or not ssh_client.client.get_transport()
                or not ssh_client.client.get_transport().is_active()
            ):
                if not ssh_client.connect():
                    raise Exception("SSH connection required for GPSS check")

            # Check if GPSS is installed
            is_installed, install_msg = ssh_client.check_gpss_installed()

            if not is_installed:
                result = CheckResult(
                    name="GPSS Availability",
                    success=False,
                    message="GPSS not installed on remote host",
                    details={"error": install_msg},
                )
                print(f"  ✗ {result.message}")
                self.results.append(result)
                return result

            # Check if GPSS is running
            is_running, run_msg = ssh_client.check_gpss_running()

            # Check if remote directory exists
            gpss_dir = self.config.get("GPSS_REMOTE_DIR", "/home/gpadmin/gpss")
            dir_exists = ssh_client.directory_exists(gpss_dir)

            result = CheckResult(
                name="GPSS Availability",
                success=True,
                message=f"GPSS installed. Status: {'Running' if is_running else 'Not running'}",
                details={
                    "installed": True,
                    "running": is_running,
                    "remote_dir": gpss_dir,
                    "remote_dir_exists": dir_exists,
                    "install_path": install_msg,
                },
            )

            if is_running:
                print(f"  ✓ {result.message}")
            else:
                print(f"  ⚠ {result.message} (will start during setup)")

        except Exception as e:
            result = CheckResult(
                name="GPSS Availability",
                success=False,
                message=f"GPSS check failed: {str(e)}",
                details={"error": str(e)},
            )
            print(f"  ✗ {result.message}")

        self.results.append(result)
        return result

    def check_dataset(self) -> CheckResult:
        """Test dataset file availability"""
        print("\n[6/6] Checking dataset availability...")

        try:
            # Check both simulation and training datasets
            simulation_dataset = self.config.get_simulation_dataset_path()
            training_dataset = self.config.get_training_dataset_path()
            
            missing_datasets = []
            
            if not os.path.exists(simulation_dataset):
                missing_datasets.append(f"Simulation dataset: {simulation_dataset}")
            
            if not os.path.exists(training_dataset):
                missing_datasets.append(f"Training dataset: {training_dataset}")
            
            if missing_datasets:
                raise FileNotFoundError("; ".join(missing_datasets))

            # Get file sizes
            sim_size = os.path.getsize(simulation_dataset) / (1024 * 1024)
            train_size = os.path.getsize(training_dataset) / (1024 * 1024)

            result = CheckResult(
                name="Datasets",
                success=True,
                message=f"Both datasets found (Simulation: {sim_size:.1f} MB, Training: {train_size:.1f} MB)",
                details={
                    "simulation_path": simulation_dataset,
                    "simulation_size_mb": sim_size,
                    "training_path": training_dataset,
                    "training_size_mb": train_size,
                },
            )
            print(f"  ✓ Simulation dataset found: {simulation_dataset} ({sim_size:.1f} MB)")
            print(f"  ✓ Training dataset found: {training_dataset} ({train_size:.1f} MB)")

        except Exception as e:
            result = CheckResult(
                name="Datasets",
                success=False,
                message=f"Dataset(s) not found: {str(e)}",
                details={"error": str(e)},
            )
            print(f"  ✗ {result.message}")

        self.results.append(result)
        return result

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all checks as a dictionary"""
        passed = sum(1 for r in self.results if r.success)
        failed = len(self.results) - passed

        # Debug: Check if all results are CheckResult objects
        results_list = []
        for i, r in enumerate(self.results):
            if not isinstance(r, CheckResult):
                print(
                    f"WARNING: Item at index {i} is not a CheckResult: {type(r)} = {r}"
                )
                continue
            results_list.append(r.to_dict())

        return {
            "total": len(self.results),
            "passed": passed,
            "failed": failed,
            "all_passed": all(
                r.success for r in self.results if isinstance(r, CheckResult)
            ),
            "results": results_list,
        }

    def _print_summary(self):
        """Print summary of all checks"""
        print("\n" + "=" * 60)
        print("PRE-FLIGHT CHECK SUMMARY")
        print("=" * 60)

        passed = sum(1 for r in self.results if r.success)
        failed = len(self.results) - passed

        for result in self.results:
            status = "✓ PASS" if result.success else "✗ FAIL"
            print(f"{status:10} | {result.name:20} | {result.message}")

        print("=" * 60)
        print(
            f"Total: {len(self.results)} checks | Passed: {passed} | Failed: {failed}"
        )
        print("=" * 60)


def run_preflight_checks() -> Dict[str, Any]:
    """Convenience function to run all checks"""
    checker = PreflightChecker()
    return checker.run_all_checks()


if __name__ == "__main__":
    # Run checks if executed directly
    summary = run_preflight_checks()

    if summary["all_passed"]:
        print("\n✓ All pre-flight checks passed!")
        exit(0)
    else:
        print(
            f"\n✗ {summary['failed']} pre-flight check(s) failed. Please resolve issues before proceeding."
        )
        exit(1)
