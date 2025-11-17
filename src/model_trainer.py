"""
Model Trainer
Trains ML models using kdd_intrusion_detection.py and uploads to Gemfire
"""

import os
import sys
import subprocess
import time
from typing import Dict, Any, Callable, Optional
from src.config_manager import get_config


class ModelTrainer:
    """Handles model training and deployment using kdd_intrusion_detection.py"""

    def __init__(self, progress_callback: Callable = None):
        self.config = get_config()
        self.progress_callback = progress_callback

    def _report_progress(self, step: str, progress: int, message: str):
        """Report progress to callback"""
        if self.progress_callback:
            self.progress_callback(
                {"step": step, "progress": progress, "message": message}
            )

    def train_model(self) -> Dict[str, Any]:
        """
        Train ML models using kdd_intrusion_detection.py
        Returns: Dict with success status and details
        """
        try:
            self._report_progress("prep", 10, "Preparing model training...")
            
            # Get the path to kdd_intrusion_detection.py
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)
            training_script = os.path.join(project_root, "tools", "kdd_intrusion_detection.py")
            
            if not os.path.exists(training_script):
                raise FileNotFoundError(f"Training script not found: {training_script}")
            
            # Check training dataset path
            dataset_path = self.config.get_training_dataset_path()
            if not os.path.exists(dataset_path):
                raise FileNotFoundError(f"Training dataset not found: {dataset_path}. Expected: ./data/kdd_cup_data_history.csv")
            
            self._report_progress("prep", 20, f"Dataset found: {dataset_path}")
            self._report_progress("train", 30, "Starting model training (this may take several minutes)...")
            
            # Run the training script as a subprocess
            # Use Python from the same environment
            python_executable = sys.executable
            
            print(f"Running training script: {training_script}")
            print(f"Using Python: {python_executable}")
            
            # Run the script with real-time output streaming
            print(f"\n{'='*60}")
            print("STARTING MODEL TRAINING")
            print(f"{'='*60}")
            print(f"Script: {training_script}")
            print(f"Python: {python_executable}")
            print(f"Dataset: {dataset_path}")
            print(f"{'='*60}\n")
            
            process = subprocess.Popen(
                [python_executable, training_script],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # Merge stderr into stdout
                text=True,
                bufsize=1,  # Line buffered
                universal_newlines=True,
                cwd=project_root
            )
            
            # Stream output in real-time
            stdout_lines = []
            line_count = 0
            last_progress_update = time.time()
            
            print("Streaming training output (this may take several minutes)...")
            print("-" * 60)
            
            # Read output line by line
            for line in iter(process.stdout.readline, ''):
                if not line:
                    break
                    
                line = line.rstrip()
                stdout_lines.append(line)
                line_count += 1
                
                # Print every line to console for debugging
                print(f"[TRAIN] {line}")
                
                # Parse output for progress indicators
                line_lower = line.lower()
                
                # Update progress based on output content
                if "loading dataset" in line_lower or "detected csv" in line_lower:
                    self._report_progress("train", 35, "Loading dataset...")
                elif "preprocessing" in line_lower or "encoding" in line_lower:
                    self._report_progress("train", 40, "Preprocessing data...")
                elif "training random forest" in line_lower:
                    self._report_progress("train", 50, "Training Random Forest...")
                elif "random forest training completed" in line_lower:
                    self._report_progress("train", 70, "Random Forest training completed!")
                elif "model training completed" in line_lower or "model:" in line_lower:
                    self._report_progress("train", 80, "Model training completed!")
                elif "saving model" in line_lower or "model saved" in line_lower:
                    self._report_progress("train", 85, "Saving model files...")
                elif "complete" in line_lower and "model" in line_lower:
                    self._report_progress("train", 95, "Model training completed!")
                
                # Periodic progress update every 30 seconds
                current_time = time.time()
                if current_time - last_progress_update > 30:
                    self._report_progress("train", 50, f"Training in progress... (processed {line_count} output lines)")
                    last_progress_update = current_time
                
                # Flush to ensure immediate output
                sys.stdout.flush()
            
            # Wait for process to complete
            process.wait()
            stdout = '\n'.join(stdout_lines)
            stderr = ""

            print("-" * 60)
            print(f"Training process completed with return code: {process.returncode}")
            print(f"Total output lines: {line_count}")
            print(f"{'='*60}\n")

            if process.returncode != 0:
                # Extract error from output
                error_lines = [line for line in stdout_lines if any(keyword in line.lower() for keyword in ['error', 'exception', 'failed', 'traceback'])]
                error_msg = '\n'.join(error_lines[-10:]) if error_lines else stdout[-500:] if stdout else "Training script failed"
                
                print(f"\n✗ TRAINING FAILED")
                print(f"Return code: {process.returncode}")
                print(f"Error output (last 10 error lines):")
                print("-" * 60)
                print(error_msg)
                print("-" * 60)
                
                final_message = f"Model training failed. Return code: {process.returncode}"
                
                self._report_progress("error", 0, f"✗ {final_message}")
                return {
                    "success": False,
                    "error": error_msg[:500],  # Limit error message length
                    "message": final_message,
                    "stdout_lines": line_count,
                    "return_code": process.returncode,
                }
            
            # Process completed successfully - check if model files were created
            model_files_created = False
            model_file_path = None
            
            # Check for model file in models/ directory
            models_dir = os.path.join(project_root, "models")
            if os.path.exists(models_dir):
                for filename in os.listdir(models_dir):
                    if filename.startswith("kdd_") and filename.endswith("_model.pkl"):
                        model_file_path = os.path.join(models_dir, filename)
                        model_files_created = True
                        break
            
            if not model_files_created:
                print(f"\n⚠ WARNING: Training completed but model file not found")
                print(f"  Expected model file: kdd_*_model.pkl in models/ directory")
                self._report_progress("error", 0, "⚠ Training completed but model file not found")
                return {
                    "success": False,
                    "error": "Model file not found after training",
                    "message": "Training completed but model file was not created. Check logs for details.",
                    "stdout_lines": line_count,
                    "return_code": process.returncode,
                }
            
            # Check if model comparison results were created
            comparison_file = os.path.join(project_root, "tools", "model_comparison_results.json")
            if os.path.exists(comparison_file):
                import json
                with open(comparison_file, 'r') as f:
                    comparison_data = json.load(f)
                
                best_model_name = comparison_data.get('best_model', 'Unknown')
                best_accuracy = comparison_data.get('best_accuracy_percent', 0)
                
                self._report_progress("complete", 100, 
                    f"✓ Model trained successfully! Best: {best_model_name} ({best_accuracy:.2f}% accuracy)")

                return {
                    "success": True,
                    "message": f"Model trained successfully! Best model: {best_model_name} ({best_accuracy:.2f}% accuracy)",
                    "best_model": best_model_name,
                    "best_accuracy": best_accuracy,
                    "comparison": comparison_data,
                    "model_file": model_file_path,
                }
            else:
                # Model was trained but comparison file not found
                # Still report success if model file exists
                self._report_progress("complete", 100, f"✓ Model trained successfully! (Model file: {os.path.basename(model_file_path) if model_file_path else 'found'})")
                return {
                    "success": True,
                    "message": f"Model training completed successfully! Model file: {os.path.basename(model_file_path) if model_file_path else 'created'}",
                    "model_file": model_file_path,
                }

        except Exception as e:
            import traceback
            traceback.print_exc()
            self._report_progress("error", 0, f"✗ Training failed: {str(e)}")

            return {
                "success": False,
                "error": str(e),
                "message": f"Model training failed: {str(e)}",
            }
