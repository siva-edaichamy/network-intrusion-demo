#!/usr/bin/env python3
"""
Tanzu Data Intelligence Demo
Main entry point for the application
"""

import os
import sys
import webbrowser
import time
from pathlib import Path

# Ensure we can import from src
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.config_manager import get_config


def check_environment():
    """Check if the environment is properly set up"""
    print("Checking environment...")

    # Check if .env exists
    if not os.path.exists(".env"):
        print("\n⚠️  WARNING: .env file not found!")
        print("Please copy .env.example to .env and configure your credentials:")
        print("  cp .env.example .env")
        print("\nThen edit .env with your actual configuration values.\n")

        create = input("Would you like to create .env from .env.example now? (y/n): ")
        if create.lower() == "y":
            if os.path.exists(".env.example"):
                import shutil

                shutil.copy(".env.example", ".env")
                print(
                    "✓ Created .env file. Please edit it with your credentials before continuing."
                )
                sys.exit(0)
            else:
                print("✗ .env.example not found!")
                sys.exit(1)
        else:
            sys.exit(0)

    # Check if required directories exist
    required_dirs = ["logs", "data", "gpss", "models"]
    for dir_name in required_dirs:
        dir_path = Path(dir_name)
        if not dir_path.exists():
            print(f"Creating directory: {dir_name}")
            dir_path.mkdir(parents=True, exist_ok=True)

    # Validate configuration
    config = get_config()
    is_valid, errors = config.validate()

    if not is_valid:
        print("\n⚠️  Configuration validation failed:")
        for error in errors:
            print(f"  - {error}")
        print("\nPlease fix these issues in your .env file before continuing.\n")

        proceed = input("Proceed anyway? (y/n): ")
        if proceed.lower() != "y":
            sys.exit(1)
    else:
        print("✓ Configuration valid")

    return True


def create_sample_model():
    """Create a sample trained model if it doesn't exist"""
    model_path = Path("models/sample_kdd_model.pkl")

    if model_path.exists():
        print("✓ Sample model found")
        return True

    print("Creating sample model...")

    try:
        from sklearn.tree import DecisionTreeClassifier
        from sklearn.datasets import make_classification
        import pickle
        import numpy as np

        # Create a simple trained model
        # In production, this would be extracted from MADlib
        X, y = make_classification(
            n_samples=1000,
            n_features=41,  # KDD dataset has 41 features (excluding id and target)
            n_informative=20,
            n_redundant=10,
            n_classes=2,
            random_state=42,
        )

        model = DecisionTreeClassifier(
            max_depth=5, min_samples_split=3, min_samples_leaf=1, random_state=42
        )

        model.fit(X, y)

        # Save the model
        with open(model_path, "wb") as f:
            pickle.dump(model, f)

        print("✓ Sample model created")
        return True

    except Exception as e:
        print(f"✗ Failed to create sample model: {str(e)}")
        print("  You can upload your own model via the Control Panel")
        return False


def open_browser(url, delay=2):
    """Open browser after a short delay"""
    time.sleep(delay)
    try:
        webbrowser.open(url)
    except:
        pass  # Browser opening is optional


def print_banner():
    """Print application banner"""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║    TANZU DATA INTELLIGENCE DEMO                              ║
║    Network Intrusion Detection                               ║
║                                                              ║
║    Components: RabbitMQ • Gemfire • Greenplum • GPSS        ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)


def main():
    """Main entry point"""
    print_banner()

    # Check environment
    if not check_environment():
        sys.exit(1)

    # Create sample model if needed
    create_sample_model()

    # Get web configuration
    config = get_config()
    web_config = config.get_web_config()
    port = web_config["port"]

    print("\n" + "=" * 60)
    print("Starting Tanzu Data Intelligence Demo Server")
    print("=" * 60)
    print(f"\n📊 Control Panel: http://localhost:{port}/control")
    print(f"🎯 Demo Dashboard: http://localhost:{port}/demo")
    print("\nPress Ctrl+C to stop the server")
    print("=" * 60 + "\n")

    # Open browser in background
    import threading

    browser_thread = threading.Thread(
        target=open_browser, args=(f"http://localhost:{port}/control",), daemon=True
    )
    browser_thread.start()

    # Start the web server
    try:
        from web.app import run_server

        run_server()
    except KeyboardInterrupt:
        print("\n\nShutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n✗ Error starting server: {str(e)}")
        print("\nTroubleshooting tips:")
        print("1. Check if port 8080 is already in use")
        print(
            "2. Verify all dependencies are installed: pip install -r requirements.txt"
        )
        print("3. Check logs in ./logs/ directory")
        print("4. See TROUBLESHOOTING.md for more help")
        sys.exit(1)


if __name__ == "__main__":
    main()
