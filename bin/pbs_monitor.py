# /usr/local/bin/pbs_monitor.py
import os
import json
import sqlite3
from datetime import datetime, timedelta
import subprocess

# Configuration - UPDATE THESE PATHS!
DB_PATH = '/var/lib/pbs_monitor/pbs_stats.db'
DATA_DIR = '/var/log/pbs_monitor/json_backups'
PBSNODES_PATH = '/opt/pbs/bin/pbsnodes'
QSTAT_PATH = '/opt/pbs/bin/qstat'

def init_db():
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        c.execute('''CREATE TABLE IF NOT EXISTS nodes
                     (timestamp TEXT, node_name TEXT, data_json TEXT)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS jobs
                     (timestamp TEXT, job_id TEXT, user TEXT, 
                      machine TEXT, start_time TEXT, data_json TEXT)''')
        
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Database initialization failed: {str(e)}", file=sys.stderr)
        raise

def collect_data():
    timestamp = datetime.now().isoformat()
    
    try:
        # Get current data with full paths
        nodes_json = subprocess.check_output([PBSNODES_PATH, '-av', '-F', 'json'])
        jobs_json = subprocess.check_output([QSTAT_PATH, '-fx', '-F', 'json'])
        
        # Store raw JSON backups
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(f"{DATA_DIR}/nodes_{timestamp}.json", 'wb') as f:
            f.write(nodes_json)
        with open(f"{DATA_DIR}/jobs_{timestamp}.json", 'wb') as f:
            f.write(jobs_json)
        
        # Parse and store in database
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Store nodes data
        nodes_data = json.loads(nodes_json.decode())
        for node, info in nodes_data.items():
            c.execute("INSERT INTO nodes VALUES (?, ?, ?)",
                     (timestamp, node, json.dumps(info)))
        
        # Store jobs data
        jobs_data = json.loads(jobs_json.decode()).get('Jobs', {})
        for job_id, job_info in jobs_data.items():
            user = job_info.get('Job_Owner', '').split('@')[0]
            exec_host = job_info.get('exec_host', '').split('/')[0] if job_info.get('exec_host') else None
            stime = job_info.get('stime', '')
            
            c.execute("INSERT INTO jobs VALUES (?, ?, ?, ?, ?, ?)",
                     (timestamp, job_id, user, exec_host, stime, json.dumps(job_info)))
        
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Data collection failed: {str(e)}", file=sys.stderr)
        raise

if __name__ == "__main__":
    import sys
    try:
        init_db()
        collect_data()
    except Exception as e:
        sys.exit(1)
