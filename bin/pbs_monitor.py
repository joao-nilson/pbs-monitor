#!/usr/bin/python3
# /usr/local/bin/pbs_monitor.py
import os
import json
import sqlite3
from datetime import datetime, timedelta
import subprocess
import sys

# Configuration - UPDATE THESE PATHS!
DB_PATH = '/var/lib/pbs_monitor/pbs_stats.db'
DATA_DIR = '/var/log/pbs_monitor/json_backups'
PBSNODES_PATH = '/opt/pbs/bin/pbsnodes'
QSTAT_PATH = '/opt/pbs/bin/qstat'

def should_store_job(job_data, conn, time_window_minutes=5):
    """Check if we should store this job or if it's a recent duplicate"""
    c = conn.cursor()

    # Check if same job was stored recently
    c.execute("""
        SELECT COUNT(*)
        FROM jobs
        WHERE job_id = ? AND user = ? AND machine = ?
        AND start_time >= datetime('now', ?)
    """, (job_data['job_id'], job_data['user'], job_data['machine'], f'-{time_window_minutes} minutes'))

    recent_count = c.fetchone()[0]
    return recent_count == 0  # Only store if no recent duplicate

def init_db():
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        c.execute('''CREATE TABLE IF NOT EXISTS nodes
                     (timestamp TEXT, node_name TEXT, data_json TEXT)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS jobs
                     (job_id TEXT, user TEXT, 
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
        print("Collecting PBS nodes data.   .   .")
        nodes_json = subprocess.check_output([PBSNODES_PATH, '-av', '-F', 'json'])
        print("Collecting PBS jobs data.    .   .")
        jobs_json = subprocess.check_output([QSTAT_PATH, '-fx', '-F', 'json'])
        
        # Store raw JSON backups
        os.makedirs(DATA_DIR, exist_ok=True)
        safe_timestamp = timestamp.replace(':', '-')
        
        nodes_backup = f"{DATA_DIR}/nodes_{safe_timestamp}.json"
        jobs_backup = f"{DATA_DIR}/jobs_{safe_timestamp}.json"
        
        with open(nodes_backup, 'wb') as f:
            f.write(nodes_json)
        with open(jobs_backup, 'wb') as f:
            f.write(jobs_json)
        
        print(f"JSON backups saved: {nodes_backup}, {jobs_backup}")
        
        # Parse and store in database
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Store nodes data
        nodes_data = json.loads(nodes_json.decode())
        nodes_count = 0
        for node, info in nodes_data.items():
            c.execute("INSERT INTO nodes VALUES (?, ?, ?)",
                     (timestamp, node, json.dumps(info)))
            nodes_count += 1

        # Store jobs data with duplicate checking
        jobs_data = json.loads(jobs_json.decode()).get('Jobs', {})
        jobs_stored = 0
        jobs_skipped = 0
        jobs_count = 0        
        for job_id, job_info in jobs_data.items():
            user = job_info.get('Job_Owner', '').split('@')[0]
            exec_host = job_info.get('exec_host', '').split('/')[0] if job_info.get('exec_host') else None
            stime = job_info.get('stime', '')

            # Create job data dictionary for duplicate checking
            job_data = {
                'job_id': job_id,
                'user': user,
                'machine': exec_host or 'unknown',
                'start_time': stime
            }
            
            # Check if we should store this job (not a recent duplicate)
            if should_store_job(job_data, conn):
                c.execute("INSERT INTO jobs VALUES (?, ?, ?, ?, ?)",
                         (job_id, user, exec_host, stime, json.dumps(job_info)))
                jobs_count += 1
                jobs_stored += 1
            else:
                jobs_skipped += 1
        
        conn.commit()
        conn.close()
        
        # Log the results
        print(f"Data collection completed: {jobs_stored} jobs stored, {jobs_skipped} duplicates skipped")
    
    except subprocess.CalledProcessError as e:
        print(f"PBS command failed: {str(e)}", file=sys.stderr)
        raise
    except json.JSONDecodeError as e:
        print(f"JSON parsing failed: {str(e)}", file=sys.stderr)
        raise
    except Exception as e:
        print(f"Data collection failed: {str(e)}", file=sys.stderr)
        raise

if __name__ == "__main__":
    try:
        init_db()
        collect_data()
        print("PBS monitoring completed successfully")
    except Exception as e:
        print(f"PBS monitoring failed: {str(e)}", file=sys.stderr)
        sys.exit(1)
