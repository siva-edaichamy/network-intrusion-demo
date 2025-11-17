#!/bin/bash

###############################################################################
# Tanzu Data Intelligence Demo - Quick Start Script
# This script automates the initial setup process
###############################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_banner() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║                                                          ║"
    echo "║    TANZU DATA INTELLIGENCE DEMO                          ║"
    echo "║    Quick Start Setup                                     ║"
    echo "║                                                          ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
check_prerequisites() {
    print_info "Checking prerequisites..."

    # Check Python
    if ! command_exists python3; then
        print_error "Python 3 is not installed"
        exit 1
    fi

    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
    print_success "Python $PYTHON_VERSION found"

    # Check pip
    if ! command_exists pip3 && ! command_exists pip; then
        print_error "pip is not installed"
        exit 1
    fi
    print_success "pip found"

    # Check git (optional)
    if command_exists git; then
        print_success "git found"
    else
        print_warning "git not found (optional)"
    fi
}

# Setup virtual environment
setup_venv() {
    print_info "Setting up virtual environment..."

    if [ -d "venv" ]; then
        print_warning "Virtual environment already exists"
        read -p "Remove and recreate? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            rm -rf venv
        else
            print_info "Using existing virtual environment"
            return 0
        fi
    fi

    python3 -m venv venv
    print_success "Virtual environment created"

    # Activate virtual environment
    source venv/bin/activate
    print_success "Virtual environment activated"
}

# Install dependencies
install_dependencies() {
    print_info "Installing dependencies..."

    # Upgrade pip
    pip install --upgrade pip setuptools wheel

    # Install requirements
    pip install -r requirements.txt

    print_success "Dependencies installed"
}

# Setup configuration
setup_config() {
    print_info "Setting up configuration..."

    if [ -f ".env" ]; then
        print_warning ".env file already exists"
        read -p "Overwrite with template? (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            print_info "Keeping existing .env file"
            return 0
        fi
    fi

    if [ ! -f ".env.example" ]; then
        print_error ".env.example not found"
        exit 1
    fi

    cp .env.example .env
    print_success ".env file created"

    print_warning "IMPORTANT: You must edit .env with your actual credentials"
    echo ""
    echo "Required settings:"
    echo "  - RABBITMQ_HOST, RABBITMQ_USERNAME, RABBITMQ_PASSWORD"
    echo "  - GREENPLUM_HOST, GREENPLUM_USER, GREENPLUM_PASSWORD"
    echo "  - GEMFIRE_PUBLIC_IP"
    echo "  - KDD_DATASET_PATH"
    echo ""

    read -p "Open .env in editor now? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        if command_exists nano; then
            nano .env
        elif command_exists vi; then
            vi .env
        else
            print_warning "No editor found. Please edit .env manually"
        fi
    fi
}

# Create directories
create_directories() {
    print_info "Creating necessary directories..."

    mkdir -p logs
    mkdir -p data
    mkdir -p models
    mkdir -p gpss

    print_success "Directories created"
}

# Check dataset
check_dataset() {
    print_info "Checking for dataset..."

    if [ -f "data/kdd_cup_data.csv" ]; then
        print_success "Dataset found"
        return 0
    fi

    print_warning "Dataset not found at: data/kdd_cup_data.csv"
    echo ""
    echo "Please place your KDD Cup dataset CSV file at: data/kdd_cup_data.csv"
    echo ""
    read -p "Continue anyway? (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_error "Setup cancelled"
        exit 1
    fi
}

# Verify setup
verify_setup() {
    print_info "Verifying setup..."

    # Check if .env exists and is configured
    if [ ! -f ".env" ]; then
        print_error ".env file not found"
        return 1
    fi

    # Check critical environment variables
    source .env

    if [ -z "$RABBITMQ_HOST" ] || [ -z "$GREENPLUM_HOST" ] || [ -z "$GEMFIRE_PUBLIC_IP" ]; then
        print_warning "Some configuration values are missing in .env"
        print_warning "Please edit .env before running the application"
        return 1
    fi

    print_success "Setup verification complete"
    return 0
}

# Main setup process
main() {
    print_banner

    check_prerequisites
    echo ""

    setup_venv
    echo ""

    install_dependencies
    echo ""

    create_directories
    echo ""

    setup_config
    echo ""

    check_dataset
    echo ""

    verify_setup
    echo ""

    print_success "Setup complete!"
    echo ""
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║  Next Steps:                                             ║"
    echo "║                                                          ║"
    echo "║  1. Edit .env with your credentials (if not done)       ║"
    echo "║  2. Place KDD dataset at: data/kdd_cup_data.csv         ║"
    echo "║  3. Activate venv: source venv/bin/activate             ║"
    echo "║  4. Run application: python main.py                     ║"
    echo "║                                                          ║"
    echo "║  The Control Panel will open at:                        ║"
    echo "║  http://localhost:8080/control                          ║"
    echo "║                                                          ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""
}

# Run main function
main
