# PBS Job Statistics Monitor
## Overview

This system automatically collects and analyzes job execution statistics from PBS clusters, tracking:

- Jobs per user
- Jobs per compute node
- Daily job counts
- Historical trends

## Repository Structure
```
pbs-monitor/
├── bin/
│ ├── pbs_monitor.py # Data collection service
│ └── pbs_stats.py # Statistics query tool
├── etc/
│ └── systemd/
│ ├── pbs-monitor.service
│ └── pbs-monitor.timer
├── var/
│ ├── lib/pbs_monitor/ # Database location
│ └── log/pbs_monitor/ # Logs and backups
├── LICENSE
└── README.md
```

## Installation

### Prerequisites
```bash
# Test PBS commands are available
which pbsnodes qstat

# Verify Python 3 is installed
python3 --version
```
### 1. Clone Repository
```
git clone https://github.com/joao-nilson/pbs-monitor.git
cd pbs-monitor
```

### Create directories
```
sudo mkdir -p /var/lib/pbs_monitor /var/log/pbs_monitor/json_backups
```

### Copy executables
```
sudo cp bin/* /usr/local/bin/
sudo chmod +x /usr/local/bin/pbs_*.py
```
### Install systemd files
```
sudo cp etc/systemd/* /etc/systemd/system/
```
