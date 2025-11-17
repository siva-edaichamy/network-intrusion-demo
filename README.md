# Tanzu Data Intelligence Demo

**Real-time Network Intrusion Detection** using RabbitMQ, Gemfire, and Greenplum

## 🎯 What This Demo Shows

- **RabbitMQ**: Message streaming from simulated network devices
- **Greenplum Streaming Server (GPSS)**: Real-time data ingestion into Greenplum
- **Gemfire**: In-memory caching for prediction results 
- **Greenplum**: Historical data storage and used for model training 
- **ML Pipeline**: Decision tree model for intrusion detection on KDD Cup dataset

## 🚀 Quick Start

### Prerequisites
- Python 3.8+ (Python 3.9+ recommended)
- Access to RabbitMQ, Greenplum, and Gemfire instances
- KDD Cup dataset (CSV format) - see https://www.kaggle.com/datasets/galaxyh/kdd-cup-1999-data
- GPSS (Greenplum Streaming Server) installed and accessible
- Virtual environment (recommended)

### Installation

```bash
# 1. Clone the repository
git clone <repository-url>
cd tdi-network-intrusion-demo

# 2. Create and activate virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure credentials
cp .env.example .env
# Edit .env with your actual RabbitMQ, Greenplum, and Gemfire credentials

# 5. Place your KDD datasets
# Copy your KDD CSV files to:
#   - Training dataset: ./data/kdd_cup_data_history.csv
#   - Simulation dataset: ./data/kdd_cup_data_stream.csv
# Note: These files are gitignored - you need to add them manually

# 6. Start the application
python main.py
```

The Control Panel will automatically open in your browser at `http://localhost:8080/control`

### Dataset Requirements

You need two KDD Cup dataset files:

1. **Training Dataset** (`kdd_cup_data_history.csv`): Used for training the ML model
   - Place in: `./data/kdd_cup_data_history.csv`
   - Should contain historical network data with labels

2. **Simulation Dataset** (`kdd_cup_data_stream.csv`): Used for real-time simulation
   - Place in: `./data/kdd_cup_data_stream.csv`
   - Should contain network connection data to simulate

**Note**: These CSV files are not included in the repository (gitignored). You must obtain the KDD Cup dataset separately and place the files in the `data/` directory.

## 📋 Demo Flow

### Control Panel (Setup)
1. **Configuration**: Verify all settings loaded from `.env`
2. **Pre-flight Checks**: Test connections to all services
3. **Train Model**: Train ML model using training dataset (kdd_cup_data_history.csv)
4. **One-Time Setup**: Create tables, deploy GPSS
5. **Start Demo**: Launch simulation using simulation dataset (kdd_cup_data_stream.csv) and open dashboard
6. **Reset Demo**: Clear all data (RabbitMQ queues, Greenplum table, Gemfire cache) while keeping the trained model

### Demo Dashboard (Presentation)
- Real-time visualization of data flowing through the pipeline
- Live intrusion detection predictions
- Metrics and statistics
- Clean, modern interface perfect for demos

## 🔄 Running Multiple Demos

The application is **fully restartable**:
- All setup is idempotent (safe to run multiple times)
- Soft Reset: Stop and restart without losing setup
- Hard Reset: Clean everything and start fresh

## 📁 Project Structure

```
tdi-network-intrusion-demo/
├── main.py                      # Entry point
├── requirements.txt             # Python dependencies
├── .env.example                 # Environment variables template
├── .env                         # Your credentials (create from .env.example, gitignored)
├── config/
│   ├── config.yaml             # Application settings
│   ├── load_kdd.yaml.template  # GPSS configuration template
│   └── load_kdd.yaml           # Generated GPSS config (gitignored - contains credentials)
├── gpss/
│   ├── gpss_config.json        # GPSS server configuration
│   └── create_table.sql        # Greenplum table DDL
├── models/                      # Trained ML models (gitignored)
│   ├── .gitkeep                # Keeps directory in git
│   └── *.pkl                   # Model files (created after training)
├── data/                        # Dataset files (gitignored)
│   ├── .gitkeep                # Keeps directory in git
│   ├── kdd_cup_data_history.csv    # Training dataset (you must add)
│   └── kdd_cup_data_stream.csv      # Simulation dataset (you must add)
├── logs/                        # Application logs (gitignored)
│   └── .gitkeep                # Keeps directory in git
├── src/                         # Core application code
│   ├── config_manager.py       # Configuration management
│   ├── consumer_service.py     # RabbitMQ consumer & ML inference
│   ├── device_simulator.py     # Data simulation
│   ├── demo_controller.py      # Demo orchestration
│   ├── gemfire_client.py       # Gemfire cache client
│   ├── model_trainer.py        # Model training orchestration
│   ├── preflight_checker.py    # Pre-flight validation
│   ├── setup_manager.py        # Environment setup
│   ├── ssh_client.py           # SSH operations
│   └── utils.py                # Utility functions
├── tools/
│   └── kdd_intrusion_detection.py  # ML model training script
└── web/                         # Web interface
    ├── app.py                   # Flask application
    └── templates/
        ├── control_panel.html   # Setup/control interface
        └── demo_dashboard.html  # Real-time demo dashboard
```

## 🔧 Configuration

All configurations are in two places:
- **`.env`**: Credentials and connection strings (sensitive)
- **`config/config.yaml`**: Application settings (non-sensitive)

**Important:** The GPSS configuration (`config/load_kdd.yaml`) is **automatically generated** from `load_kdd.yaml.template` using your `.env` values during setup. You never need to edit it manually.

## 📖 Documentation

- **README.md**: This file - quick start and overview
- **SETUP.md**: Detailed setup instructions (if exists)
- **TROUBLESHOOTING.md**: Common issues and solutions (if exists)

## 🆘 Getting Help

1. Check **TROUBLESHOOTING.md** for common issues
2. Use "Download Diagnostics" in Control Panel
3. Check logs in `./logs/` directory

## 🎥 Demo Tips

- Use **Full Screen** mode on Demo Dashboard
- Run pre-flight checks before each demo
- Consider purging RabbitMQ queue for clean start
- Default rate: 3 messages/second (easy to follow visually)

## 📊 Key Metrics Displayed

- Messages per second
- Total messages processed
- Prediction distribution (Normal vs. Attack types)
- Gemfire inference latency
- Greenplum storage rate

## 🔒 Security Notes

- **Never commit sensitive files**: `.env`, `config/load_kdd.yaml`, and dataset files are gitignored
- Use `.env.example` as template for sharing configuration structure
- Sanitize logs before sharing diagnostics
- Keep SSH keys secure and never commit them (`.pem`, `.key` files are gitignored)
- The `config/load_kdd.yaml` file contains credentials and is auto-generated - never commit it

## 🐛 Troubleshooting

### Common Issues

1. **"Model file not found"**
   - Solution: Train the model first using the "Train Model" button in the Control Panel
   - Ensure `kdd_cup_data_history.csv` is in the `data/` directory

2. **"Connection refused" errors**
   - Solution: Verify all services (RabbitMQ, Greenplum, Gemfire) are running and accessible
   - Check your `.env` file has correct hostnames and ports
   - Run pre-flight checks in the Control Panel

3. **"Dataset not found" errors**
   - Solution: Ensure both dataset files are in the `data/` directory:
     - `kdd_cup_data_history.csv` (for training)
     - `kdd_cup_data_stream.csv` (for simulation)

4. **GPSS connection errors**
   - Solution: Ensure GPSS is running on the configured host/port
   - The setup process will attempt to start GPSS automatically if configured

5. **Port already in use**
   - Solution: Change `WEB_PORT` in `.env` or stop the process using port 8080

### Getting Help

- Check logs in `./logs/` directory
- Use "Download Diagnostics" in the Control Panel
- Verify all environment variables in `.env` are set correctly
- Ensure all prerequisites are installed

## 📝 License



## 👥 Contributors

[Add contributors here]

## 🙏 Acknowledgments

- KDD Cup 1999 dataset

