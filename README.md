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
sudo chown root:root /var/lib/pbs_monitor /var/log/pbs_monitor
sudo chmod 755 /var/lib/pbs_monitor /var/log/pbs_monitor
```

### Copy executables
```
sudo cp bin/* /usr/local/bin/
sudo chmod +x /usr/local/bin/pbs_*.py
```
### Make both scripts executable
```
sudo chmod +x /usr/local/bin/pbs_monitor.py /usr/local/bin/pbs_stats.py
```
### Install systemd files
```
sudo cp etc/systemd/* /etc/systemd/system/
```
### Enable and Start
```
sudo systemctl daemon-reload
sudo systemctl enable pbs-monitor.timer
sudo systemctl start pbs-monitor.timer
```
## Usage
### Data Collection
The monitoring service runs automatically every hour. To manually trigger collection:
```
sudo systemctl start pbs-monitor.service
```
### Viewing Statistics
Basic usage (last 7 days):
```
sudo pbs_stats.py
```
Common options:
```
# Last 30 days
sudo pbs_stats.py --days 30

# Filter by user
sudo pbs_stats.py --user rodrigooliveira

# Filter by machine
sudo pbs_stats.py --machine compute-0-8
```
### Sample Output
```
PBS Job Statistics - Last 7 days (since 2025-08-06)
Total Jobs: 1423

User                Machine         Jobs       Last Run
------------------------------------------------------
jsmith              compute-0-8     324        Mon Aug 12 09:45:32 2025
bjones              compute-1-1     287        Mon Aug 12 08:12:09 2025
mwilson             compute-0-11    156        Sun Aug 11 17:22:45 2025
```
## How It Works
1. Data Collection
  - Runs hourly via systemd timer
  - Collects node information using `pbsnodes -av -F json`
  - Collects job information using `qstat -fx -F json`
  - Stores raw JSON backups and parsed data in SQLite
2. Data Storage:
  - Jobs table: job_id, user, machine, start_time, data_json
  - Nodes table: node_name, data_json
3. Statistics Generation:
  - Queries the SQLite database
  - Groups by user and machine
  - Filters by date range
  - Calculates job counts and time ranges
## Maintenance
- Database Location: `/var/lib/pbs_monitor/pbs_stats.db`
- Logs: `journalctl -u pbs-monitor.service`
- JSON Backups: `/var/log/pbs_monitor/json_backups/`
To reset the database (will lose historical data):
```
sudo rm /var/lib/pbs_monitor/pbs_stats.db
sudo systemctl restart pbs-monitor.service
```
## Dependencies
- Python 3
- SQLite 3
- PBS commands (`pbsnodes`, `qstat`)
- systemd
