#!/usr/bin/python3
# /usr/local/bin/pbs_stats.py
import sqlite3
from datetime import datetime, timedelta
import argparse
import sys
import json
import subprocess
import time

DB_PATH = '/var/lib/pbs_monitor/pbs_stats.db'
QSTAT_PATH = '/opt/pbs/bin/qstat'

def parse_pbs_date(date_str):
    """Convert PBS date format to datetime object"""
    if not date_str:
        return None
        #return datetime.min
    try:
        return datetime.strptime(date_str, "%a %b %d %H:%M:%S %Y")
    except ValueError:
        return None
        #return datetime.min

def format_pbs_date(date_str):
    """Format PBS date string to DD-MM-YYYY HH:MM:SS"""
    if not date_str:
        return "N/A"
    try:
        dt = parse_pbs_date(date_str)
        return dt.strftime("%d-%m-%Y %H:%M:%S") if dt else "N/A"
    except Exception as e:
        return "N/A"

def parse_input_date(date_str):
    """Parse DD-MM-YYYY format into PBS date format"""
    if not date_str:
        return None
    try:
        # Parse DD-MM-YYYY format
        dt = datetime.strptime(date_str, "%d-%m-%Y")
        # Convert to PBS format: "Wed Sep 25 14:30:45 2024"
        return dt.strftime("%a %b %d %H:%M:%S %Y")
    except ValueError as e:
        raise ValueError(f"Invalid date format: {date_str}. Use DD-MM-YYYY format") from e

def get_real_time_jobs(user=None, machine=None, verbose=False):
    """Get real-time job information using qstat command"""
    try:
        # Build qstat command
        cmd = [QSTAT_PATH, '-fx', '-F', 'json']
        if user:
            cmd.extend(['-u', user])

        # Execute qstat command
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

        if result.returncode != 0:
            if verbose:
                print(f"DEBUG: qstat command failed: {result.stderr}", file=sys.stderr)
            return []

        # Parse JSON output
        data = json.loads(result.stdout)
        jobs_data = data.get('Jobs', {})

        real_time_jobs = []
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for job_id, job_info in jobs_data.items():
            # Filter by machine if specified
            exec_host = job_info.get('exec_host')
            if machine and exec_host:
                if machine not in exec_host:
                    continue

            #Filter jobs not finalized
            job_state = job_info.get('job_state', 'N/A')
            if job_state == 'F':  # Skip finished jobs
                continue

            # Extract job details
            job_user = job_info.get('Job_Owner', '').split('@')[0]
            job_state = job_info.get('job_state', 'N/A')
            job_queue = job_info.get('queue', 'N/A')
            job_name = job_info.get('Job_Name', 'N/A')

            # Get resources
            resources_used = job_info.get('resources_used', {})
            resources_requested = job_info.get('Resource_List', {})

            real_time_jobs.append({
                'job_id': job_id,
                'user': job_user,
                'machine': exec_host or 'N/A',
                'queue': job_queue,
                'job_name': job_name,
                'state': job_state,
                'start_time': current_time,  # Current time for real-time jobs
                'resources': {
                    'cpus': resources_requested.get('ncpus', 'N/A'),
                    'mem': resources_requested.get('mem', 'N/A'),
                    'walltime': resources_requested.get('walltime', 'N/A'),
                },
                'used': {
                    'cpus': resources_used.get('ncpus', 'N/A'),
                    'mem': resources_used.get('mem', 'N/A'),
                    'walltime': resources_used.get('walltime', 'N/A'),
                    'cpu_time': resources_used.get('cput', 'N/A'),
                },
                'exit_status': job_info.get('exit_status', 'N/A'),
                'submit_args': job_info.get('Submit_arguments', 'N/A'),
                'is_real_time': True
            })

        return real_time_jobs

    except subprocess.TimeoutExpired:
        if verbose:
            print("DEBUG: qstat command timed out", file=sys.stderr)
        return []
    except json.JSONDecodeError:
        if verbose:
            print("DEBUG: Failed to parse qstat JSON output", file=sys.stderr)
        return []
    except Exception as e:
        if verbose:
            print(f"DEBUG: Error getting real-time jobs: {str(e)}", file=sys.stderr)
        return []

def parse_walltime(walltime_str):
    """Convert PBS walltime format (HH:MM:SS or D+HH:MM:SS) to seconds"""
    if not walltime_str or walltime_str == 'N/A':
        return None

    # Handle HH:MM:SS format
    if ':' in walltime_str:
        parts = walltime_str.split(':')
        if len(parts) == 3:  # HH:MM:SS
            hours, minutes, seconds = map(int, parts)
            return hours * 3600 + minutes * 60 + seconds
        elif len(parts) == 2:  # MM:SS
            minutes, seconds = map(int, parts)
            return minutes * 60 + seconds
    
    # Handle days+time format (D+HH:MM:SS)
    if '+' in walltime_str:
        days_part, time_part = walltime_str.split('+')
        days = int(days_part)
        time_seconds = parse_walltime(time_part)
        return days * 86400 + time_seconds if time_seconds else None
    
    # Try to convert directly to integer (seconds)
    try:
        return int(walltime_str)
    except ValueError:
        return None

def format_duration(seconds):
    """Convert seconds to human-readable duration"""
    if seconds is None:
        return "N/A"
    try:
        seconds = int(seconds)
    except (ValueError, TypeError):
        return "N/A"

    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    
    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0 or days > 0:
        parts.append(f"{hours}h")
    if minutes > 0 or hours > 0 or days > 0:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")

    return ' '.join(parts)


def parse_pbs_date_to_datetime(date_str):
    """Convert PBS date format to datetime object for proper comparison"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%a %b %d %H:%M:%S %Y")
    except ValueError:
        return None

def parse_input_date_to_datetime(date_str):
    """Parse DD-MM-YYYY format into datetime object"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%d-%m-%Y")
    except ValueError as e:
        raise ValueError(f"Invalid date format: {date_str}. Use DD-MM-YYYY format") from e

def get_job_stats(days=None, date=None, start_date=None, end_date=None, user=None, machine=None, real_time=None, verbose=False):
    """Get detailed job information with all available fields"""

    if real_time:
        # Get real-time jobs
        real_time_jobs = get_real_time_jobs(user=user, machine=machine, verbose=verbose)
        return {
            'period': 'Real-time (current)',
            'total_jobs': len(real_time_jobs),
            'results': real_time_jobs,
            'is_real_time': True
        }

    # Get historical jobs from database
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Build basic WHERE clauses (without date filtering)
    conditions = []
    params = []
    date_range = "All history"

    # Apply non-date filters first
    if user:
        conditions.append("user = ?")
        params.append(user)
    if machine:
        conditions.append("machine = ?")
        params.append(machine)

    # Only add user IS NOT NULL if we're not filtering by a specific user
    if not user:
        conditions.append("user IS NOT NULL")

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    # Get all jobs first, then filter by date in Python
    base_query = f"""
        SELECT
            user,
            COALESCE(machine, 'N/A') as machine,
            start_time,
            data_json
        FROM jobs
        WHERE {where_clause}
    """

    if verbose:
        print(f"DEBUG: Base query: {base_query}", file=sys.stderr)
        print(f"DEBUG: With parameters: {params}", file=sys.stderr)

    # Execute the base query
    try:
        if params:
            c.execute(base_query, params)
        else:
            c.execute(base_query)
        
        all_rows = c.fetchall()
        if verbose:
            print(f"DEBUG: Found {len(all_rows)} total rows before date filtering", file=sys.stderr)
     
    except Exception as e:
        if verbose:
            print(f"DEBUG: Query failed: {e}", file=sys.stderr)
        conn.close()
        return {
            'period': date_range,
            'total_jobs': 0,
            'results': [],
            'is_real_time': False
        }

    conn.close()

    # Parse date range for filtering
    start_dt = None
    end_dt = None
    
    if date:
        # Single date search
        start_dt = datetime.strptime(date, "%d-%m-%Y")
        end_dt = start_dt + timedelta(days=1)
        date_range = f"Specific date: {date}"

    elif start_date or end_date:
        # Date range search
        if start_date and end_date:
            start_dt = datetime.strptime(start_date, "%d-%m-%Y")
            end_dt = datetime.strptime(end_date, "%d-%m-%Y") + timedelta(days=1)
            date_range = f"Date range: {start_date} to {end_date}"
        elif start_date:
            start_dt = datetime.strptime(start_date, "%d-%m-%Y")
            date_range = f"From date: {start_date}"
        elif end_date:
            end_dt = datetime.strptime(end_date, "%d-%m-%Y") + timedelta(days=1)
            date_range = f"Until date: {end_date}"

    elif days and days != 'all':
        # Existing relative days filter
        try:
            days_int = int(days)
            start_dt = datetime.now() - timedelta(days=days_int)
            date_range = f"Last {days} days"
        except ValueError:
            pass

    # Filter jobs by date in Python
    filtered_jobs = []
    for row in all_rows:
        start_time_str = row['start_time']
        if not start_time_str:
            continue
            
        job_dt = parse_pbs_date(start_time_str)
        if not job_dt:
            continue
            
        # Apply date filtering
        include_job = True
        if start_dt and job_dt < start_dt:
            include_job = False
        if end_dt and job_dt >= end_dt:
            include_job = False
            
        if include_job:
            filtered_jobs.append({
                'user': row['user'],
                'machine': row['machine'],
                'start_time': start_time_str
            })

    if verbose:
        print(f"DEBUG: After date filtering: {len(filtered_jobs)} jobs", file=sys.stderr)

    # Group and count jobs by user and machine
    job_counts = {}
    last_runs = {}
    
    for job in filtered_jobs:
        key = (job['user'], job['machine'])
        
        # Count jobs
        if key in job_counts:
            job_counts[key] += 1
        else:
            job_counts[key] = 1
            
        # Track latest run
        job_dt = parse_pbs_date(job['start_time'])
        if key in last_runs:
            if job_dt > last_runs[key]:
                last_runs[key] = job_dt
        else:
            last_runs[key] = job_dt

    # Convert to results format
    results = []
    for (user, machine), count in job_counts.items():
        last_run_dt = last_runs.get((user, machine))
        last_run_display = format_pbs_date(last_run_dt.strftime("%a %b %d %H:%M:%S %Y")) if last_run_dt else "N/A"
        
        results.append({
            'user': user,
            'machine': machine,
            'jobs': count,
            'last_run': last_run_display
        })

    # Sort by job count descending
    results.sort(key=lambda x: x['jobs'], reverse=True)

    if verbose:
        print(f"DEBUG: Found {len(filtered_jobs)} total jobs, {len(results)} user-machine combinations", file=sys.stderr)
        if results:
            print(f"DEBUG: Sample results after filtering:", file=sys.stderr)
            for i, result in enumerate(results[:3]):
                print(f"DEBUG: Row {i}: user='{result['user']}', machine='{result['machine']}', jobs={result['jobs']}", file=sys.stderr)

    return {
        'period': date_range,
        'total_jobs': len(filtered_jobs),
        'results': results,
        'is_real_time': False
    }

#def get_job_stats(days=None, date=None, start_date=None, end_date=None, user=None, machine=None, real_time=None, verbose=False):
#    """Get detailed job information with all available fields"""
#
#    if real_time:
#        # Get real-time jobs
#        real_time_jobs = get_real_time_jobs(user=user, machine=machine, verbose=verbose)
#        return {
#            'period': 'Real-time (current)',
#            'total_jobs': len(real_time_jobs),
#            'results': real_time_jobs,
#            'is_real_time': True
#        }
#
#    # Get historical jobs from database
#    conn = sqlite3.connect(DB_PATH)
#    conn.row_factory = sqlite3.Row
#    c = conn.cursor()
#
#    # Build WHERE clauses
#    conditions = []
#    params = []
#    date_range = "All history"
#
#    # Date filtering
##    if date:
##        # Single date search
##        pbs_date_str = parse_input_date(date)
##        conditions.append("start_time >= ? AND start_time < ?")
##        params.extend([pbs_date_str,
##                      (datetime.strptime(pbs_date_str, "%a %b %d %H:%M:%S %Y") +
##                       timedelta(days=1)).strftime("%a %b %d %H:%M:%S %Y")])
##        date_range = f"Specific date: {date}"
##
##    elif start_date or end_date:
##        # Date range search
##        if start_date and end_date:
##            start_pbs = parse_input_date(start_date)
##            end_pbs = (datetime.strptime(parse_input_date(end_date), "%a %b %d %H:%M:%S %Y") +
##                      timedelta(days=1)).strftime("%a %b %d %H:%M:%S %Y")
##            conditions.append("start_time >= ? AND start_time < ?")
##            params.extend([start_pbs, end_pbs])
##            date_range = f"Date range: {start_date} to {end_date}"
##        elif start_date:
##            start_pbs = parse_input_date(start_date)
##            conditions.append("start_time >= ?")
##            params.append(start_pbs)
##            date_range = f"From date: {start_date}"
##        elif end_date:
##            end_pbs = (datetime.strptime(parse_input_date(end_date), "%a %b %d %H:%M:%S %Y") +
##                      timedelta(days=1)).strftime("%a %b %d %H:%M:%S %Y")
##            conditions.append("start_time < ?")
##            params.append(end_pbs)
##            date_range = f"Until date: {end_date}"
##
##    elif days and days != 'all':
##        # Existing relative days filter (for backward compatibility)
##        try:
##            days_int = int(days)
##            cutoff_date = datetime.now() - timedelta(days=days_int)
##            cutoff_str = cutoff_date.strftime("%a %b %d %H:%M:%S %Y")
##            conditions.append("start_time >= ?")
##            params.append(cutoff_str)
##            date_range = f"Last {days} days"
##        except ValueError:
##            pass
#    
#    # Date filtering with proper datetime handling
#    if date:
#        # Single date search
#        start_dt = parse_input_date_to_datetime(date)
#        end_dt = start_dt + timedelta(days=1)
#        conditions.append("start_time >= ? AND start_time < ?")
#        params.extend([start_dt.strftime("%a %b %d %H:%M:%S %Y"),
#                      end_dt.strftime("%a %b %d %H:%M:%S %Y")])
#        date_range = f"Specific date: {date}"
#
#    elif start_date or end_date:
#        # Date range search
#        if start_date and end_date:
#            start_dt = parse_input_date_to_datetime(start_date)
#            end_dt = parse_input_date_to_datetime(end_date) + timedelta(days=1)
#            conditions.append("start_time >= ? AND start_time < ?")
#            params.extend([start_dt.strftime("%a %b %d %H:%M:%S %Y"),
#                          end_dt.strftime("%a %b %d %H:%M:%S %Y")])
#            date_range = f"Date range: {start_date} to {end_date}"
#        elif start_date:
#            start_dt = parse_input_date_to_datetime(start_date)
#            conditions.append("start_time >= ?")
#            params.append(start_dt.strftime("%a %b %d %H:%M:%S %Y"))
#            date_range = f"From date: {start_date}"
#        elif end_date:
#            end_dt = parse_input_date_to_datetime(end_date) + timedelta(days=1)
#            conditions.append("start_time < ?")
#            params.append(end_dt.strftime("%a %b %d %H:%M:%S %Y"))
#            date_range = f"Until date: {end_date}"
#
#    elif days and days != 'all':
#        # Existing relative days filter
#        try:
#            days_int = int(days)
#            cutoff_date = datetime.now() - timedelta(days=days_int)
#            conditions.append("start_time >= ?")
#            params.append(cutoff_date.strftime("%a %b %d %H:%M:%S %Y"))
#            date_range = f"Last {days} days"
#        except ValueError:
#            pass
#
#    if user:
#        conditions.append("user = ?")
#        params.append(user)
#    if machine:
#        conditions.append("machine = ?")
#        params.append(machine)
#
#    # Only add user IS NOT NULL if we're not filtering by a specific user
#    if not user:
#        conditions.append("user IS NOT NULL")
#
#    where_clause = " AND ".join(conditions) if conditions else "1=1"
#    
#    # DEBUG: Check what's being filtered
#    if verbose:
#        print(f"DEBUG: Final conditions: {conditions}", file=sys.stderr)
#        print(f"DEBUG: Final params: {params}", file=sys.stderr)
#        print(f"DEBUG: Final date_range: {date_range}", file=sys.stderr)
#
#    # Main query    
#    query = f"""
#        SELECT
#            user,
#            COALESCE(machine, 'N/A') as machine,
#            MAX(start_time) as last_run_in_range,
#            COUNT(*) as job_count
#        FROM jobs
#        WHERE {where_clause}
#        GROUP BY user, COALESCE(machine, 'N/A')
#        ORDER BY job_count DESC
#    """
#
#    if verbose:
#        print(f"DEBUG: Executing query: {query}", file=sys.stderr)
#        print(f"DEBUG: With parameters: {params}", file=sys.stderr)
#        print(f"DEBUG: Date range: {date_range}", file=sys.stderr)
#
#    # Execute the query
#    try:
#        if params:
#            c.execute(query, params)
#        else:
#            c.execute(query)
#        
#        rows = c.fetchall()
#        # DEBUG: Check what we actually got
#        if verbose:
#            print(f"DEBUG: Raw SQL result - {len(rows)} rows returned", file=sys.stderr)
#            for i, row in enumerate(rows[:5]):  # Show first 5 rows
#                print(f"DEBUG: Row {i}: user='{row['user']}', machine='{row['machine']}', jobs={row['job_count']}", file=sys.stderr)
#
##            # Also check the actual date range in the database
##            debug_query = f"SELECT MIN(start_time) as min_date, MAX(start_time) as max_date FROM jobs WHERE {where_clause}"
##            if params:
##                c.execute(debug_query, params)
##            else:
##                c.execute(debug_query)
##            date_range_result = c.fetchone()
##            print(f"DEBUG: Actual date range in filtered results: {date_range_result['min_date']} to {date_range_result['max_date']}", file=sys.stderr)
#            # Debug: Check actual date range in results
#            if rows:
#                dates = [parse_pbs_date_to_datetime(row['last_run_in_range']) for row in rows if row['last_run_in_range']]
#                valid_dates = [d for d in dates if d is not None]
#                if valid_dates:
#                    min_date = min(valid_dates)
#                    max_date = max(valid_dates)
#                    print(f"DEBUG: Actual date range in results: {min_date.strftime('%a %b %d %H:%M:%S %Y')} to {max_date.strftime('%a %b %d %H:%M:%S %Y')}", file=sys.stderr)
#
#
#    except Exception as e:
#        if verbose:
#            print(f"DEBUG: Query failed: {e}", file=sys.stderr)
#        conn.close()
#        return {
#            'period': date_range,
#            'total_jobs': 0,
#            'results': [],
#            'is_real_time': False
#        }
#
#    results = []
#
#    # Process the results
#    for row in rows:
#        results.append({
#            'user': row['user'],
#            'machine': row['machine'],
#            'jobs': row['job_count'],
#            'last_run': format_pbs_date(row['last_run_in_range'])
#        })
#
#    # Get total jobs count using the same WHERE clause
#    count_query = f"SELECT COUNT(*) as total FROM jobs WHERE {where_clause}"
#    try:
#        if params:
#            c.execute(count_query, params)
#        else:
#            c.execute(count_query)
#
#        total_jobs_result = c.fetchone()
#        total_jobs = total_jobs_result['total'] if total_jobs_result else 0
#
#    except Exception as e:
#        if verbose:
#            print(f"DEBUG: Count query failed: {e}", file=sys.stderr)
#        total_jobs = 0
#
#    conn.close()
#
#    if verbose:
#        print(f"DEBUG: Found {total_jobs} total jobs, {len(results)} user-machine combinations", file=sys.stderr)
#
#    return {
#        'period': date_range,
#        'total_jobs': total_jobs,
#        'results': results,
#        'is_real_time': False
#    }

def get_job_details(user=None, machine=None, days=None, verbose=False):
    """Get detailed job information with all available fields"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    conditions = []
    params = []

    if user:
        conditions.append("user = ?")
        params.append(user)
    if machine:
        conditions.append("machine = ?")
        params.append(machine)

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    query = f"""
        SELECT
            job_id,
            user,
            machine,
            start_time,
            data_json
        FROM jobs
        WHERE {where_clause}
        ORDER BY start_time DESC
    """

    if verbose:
        print(f"DEBUG: Executing query: {query}", file=sys.stderr)
        print(f"DEBUG: With parameters: {params}", file=sys.stderr)

    # Safe execution with parameters
    if params:
        c.execute(query, params)
    else:
        c.execute(query)

    results = []

    # Calculate cutoff date if days is specified
    cutoff_date = None
    if days and days != 'all':
        try:
            days = int(days)
            cutoff_date = datetime.now() - timedelta(days=days)
        except ValueError:
            pass

    for row in c.fetchall():
        job_data = json.loads(row['data_json'])
        #resources_used = job_data.get('resources_used', {})
        start_time_str = row['start_time']
        start_time = parse_pbs_date(start_time_str)

        # Skip jobs older than the cutoff date
        if cutoff_date and (start_time is None or start_time < cutoff_date):
            continue

        # Initialize resources_used safely
        resources_used = job_data.get('resources_used', {})
        resource_list = job_data.get('Resource_List', {})

        # Handle walltime conversion
        walltime_str = resources_used.get('walltime', 'N/A')
        walltime_seconds = parse_walltime(walltime_str) if walltime_str != 'N/A' else None

        results.append({
            'job_id': row['job_id'],
            'user': row['user'],
            'machine': row['machine'],
            'start_time': format_pbs_date(row['start_time']),
            'queue': job_data.get('queue', 'N/A'),
            'job_name': job_data.get('Job_Name', 'N/A'),
            'state': job_data.get('job_state', 'N/A'),
            'resources': {
                'cpus': job_data.get('Resource_List', {}).get('ncpus', 'N/A'),
                'mem': job_data.get('Resource_List', {}).get('mem', 'N/A'),
                'walltime': job_data.get('Resource_List', {}).get('walltime', 'N/A'),
            },
            'used': {
                'cpus': resources_used.get('ncpus', 'N/A'),
                'mem': resources_used.get('mem', 'N/A'),
                'walltime': format_duration(walltime_seconds),
                'cpu_time': format_duration(resources_used.get('cput')),
            },
            'exit_status': job_data.get('exit_status', 'N/A'),
            'submit_args': job_data.get('Submit_arguments', 'N/A'),
        })

    conn.close()
    return results

def print_job_details(jobs, verbose=False, is_real_time=False):
    """Print detailed job information"""
    if not jobs:
        print("No jobs found matching the specified criteria.")
        return

    if is_real_time:
        print("\n{:<12} {:<15} {:<20} {:<10} {:<15} {:<10} {:<12} {:<12}".format(
            "Job ID", "User", "Machine", "Queue", "Job Name", "State", "CPU Used", "Walltime"))
        print("-" * 100)

    # Create a dictionary to store unique jobs based on job_id
    unique_jobs = {}
    for job in jobs:
        job_id = job.get('job_id')
        if not job_id:
            continue

        # Get the parsed dates for comparison
        current_date = parse_pbs_date(unique_jobs.get(job_id, {}).get('start_time'))
        new_date = parse_pbs_date(job.get('start_time'))

        # Handle None values - we'll consider None as "very old" (datetime.min)
        current_date = current_date if current_date is not None else datetime.min
        new_date = new_date if new_date is not None else datetime.min

        # If we haven't seen this job before or if this entry is newer
        if job_id not in unique_jobs or new_date > current_date:
            unique_jobs[job_id] = job
    


    # Sort jobs by start time (newest first), with jobs without times at the end
    sorted_jobs = sorted(
        unique_jobs.values(),
        key=lambda x: parse_pbs_date(x.get('start_time')) or datetime.min,
        reverse=True
    )

    for job in sorted_jobs:
        # Safely extract and format all values
        job_id = str(job.get('job_id', 'N/A')).split('.')[0]
        user = str(job.get('user', 'N/A'))
        machine = str(job.get('machine', 'N/A'))
        queue = str(job.get('queue', 'N/A'))
        job_name = str(job.get('job_name', 'N/A'))
        job_name = job_name[:15] + '...' if len(job_name) > 15 else job_name
        state = str(job.get('state', 'N/A'))
        cpu_used = str(job.get('used', {}).get('cpus', 'N/A'))
        walltime = str(job.get('used', {}).get('walltime', 'N/A'))

        # Handle start time
        start_time = 'N/A'
        if job.get('start_time'):
            start_time_parts = str(job['start_time']).split()
            start_time = start_time_parts[0] if start_time_parts else 'N/A'
        
        # Extract resource usage
        cpu_used = str(job.get('used', {}).get('cpus', 'N/A'))
        walltime = str(job.get('used', {}).get('walltime', 'N/A'))
        
        # Print compact job line
        print("{:<12} {:<15} {:<20} {:<10} {:<15} {:<10} {:<12} {:<12} {:<12}".format(
            job_id, user, machine, queue, job_name, state, start_time, cpu_used, walltime))
        
        # Show additional details in verbose mode
        if verbose:
            print("\nAdditional Details:")
            print(f"  Full Start Time: {job.get('start_time', 'N/A')}")
            resources = job.get('resources', {})
            print(f"  Resources Requested: CPUs={resources.get('cpus', 'N/A')}, "
                  f"Mem={resources.get('mem', 'N/A')}, Walltime={resources.get('walltime', 'N/A')}")
            used = job.get('used', {})
            print(f"  Resources Used: CPUs={used.get('cpus', 'N/A')}, "
                  f"Mem={used.get('mem', 'N/A')}, Walltime={used.get('walltime', 'N/A')}, "
                  f"CPU Time={used.get('cpu_time', 'N/A')}")
            print(f"  Exit Status: {job.get('exit_status', 'N/A')}")
            if job.get('submit_args', 'N/A') != 'N/A':
                print(f"  Submit Arguments: {job.get('submit_args')}")
            print("-" * 60)

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

def debug_database_content():
    """Debug function to check what's actually in the database"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    print("=== DATABASE DEBUG INFO ===")
    
    # Check date range of jobs
    c.execute("SELECT MIN(start_time) as min_date, MAX(start_time) as max_date FROM jobs")
    date_range = c.fetchone()
    print(f"Date range in database: {date_range['min_date']} to {date_range['max_date']}")
    
    # Check current time for comparison
    current_time = datetime.now().strftime("%a %b %d %H:%M:%S %Y")
    print(f"Current time: {current_time}")
    
    # Test the query without date filtering
    c.execute("""
        SELECT 
            user, 
            COALESCE(machine, 'N/A') as machine,
            MAX(start_time) as last_run,
            COUNT(*) as job_count
        FROM jobs
        WHERE user = 'rodrigooliveira'
        GROUP BY user, COALESCE(machine, 'N/A')
        ORDER BY job_count DESC
        LIMIT 5
    """)
    
    print("Sample results for rodrigooliveira:")
    for row in c.fetchall():
        print(f"  User: {row['user']}, Machine: {row['machine']}, Jobs: {row['job_count']}, Last: {row['last_run']}")
    
    conn.close()

def debug_query_execution():
    """Debug the actual query execution"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    print("=== QUERY DEBUGGING ===")
    
    # Test the exact same query that's failing
    test_query = """
        SELECT 
            user, 
            COALESCE(machine, 'N/A') as machine,
            MAX(start_time) as last_run,
            COUNT(*) as job_count
        FROM jobs
        WHERE user = 'rodrigooliveira'
        GROUP BY user, COALESCE(machine, 'N/A')
        ORDER BY job_count DESC
        LIMIT 10
    """
    
    print(f"Executing: {test_query}")
    c.execute(test_query)
    
    rows = c.fetchall()
    print(f"Number of rows returned: {len(rows)}")
    
    if rows:
        print("First few rows:")
        for i, row in enumerate(rows[:5]):
            print(f"  Row {i}: user='{row['user']}', machine='{row['machine']}', job_count={row['job_count']}, last_run='{row['last_run']}'")
    else:
        print("No rows returned - checking why...")
        
        # Let's check what happens without GROUP BY
        c.execute("SELECT user, machine FROM jobs WHERE user = 'rodrigooliveira' LIMIT 5")
        sample_rows = c.fetchall()
        print("Sample rows without GROUP BY:")
        for row in sample_rows:
            print(f"  user='{row['user']}', machine='{row['machine']}'")
    
    conn.close()

def debug_database_dates():
    """Debug function to check what dates are actually in the database"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    print("=== DATABASE DATE DEBUG ===")

    # Check overall date range
    c.execute("SELECT MIN(start_time) as min_date, MAX(start_time) as max_date FROM jobs")
    overall_range = c.fetchone()
    print(f"Overall date range in database: {overall_range['min_date']} to {overall_range['max_date']}")

    # Check specific date range we're querying for
    test_start = "Mon Jul 28 00:00:00 2025"
    test_end = "Thu Jul 31 00:00:00 2025"
    c.execute("SELECT COUNT(*) as count FROM jobs WHERE start_time >= ? AND start_time < ?",
              [test_start, test_end])
    count_result = c.fetchone()
    print(f"Jobs between {test_start} and {test_end}: {count_result['count']}")

    # Check a few sample jobs in that range
    c.execute("SELECT user, machine, start_time FROM jobs WHERE start_time >= ? AND start_time < ? LIMIT 5",
              [test_start, test_end])
    sample_jobs = c.fetchall()
    print("Sample jobs in date range:")
    for job in sample_jobs:
        print(f"  {job['user']} on {job['machine']} at {job['start_time']}")

    conn.close()


def main():
    parser = argparse.ArgumentParser(description='PBS Job Statistics')
    parser.add_argument('--days', '-d', nargs='?', const=7, default=None,help='Number of days to report (default: last week), use --days all or -d all for entire history, --days 30 for last 30 days, etc.')
    parser.add_argument('--date', help='Search for a specific date (format: DD-MM-YYYY)')
    parser.add_argument('--start-date', help='Start date for range search (format: DD-MM-YYYY)')
    parser.add_argument('--end-date', help='End date for range search (format: DD-MM-YYYY)')
    parser.add_argument('--user', help='Filter by specific user')
    parser.add_argument('--machine', help='Filter by specific compute node')
    parser.add_argument('--real-time', '-r', action='store_true', help='Show real-time job information')
    parser.add_argument('--jobs', action='store_true', help='Show detailed job information')
    parser.add_argument('--verbose', action='store_true', help='Show additional debug information and details')
    parser.add_argument('--watch', '-w', action='store_true', help='Watch mode - continuously update real-time information')
    parser.add_argument('--interval', '-n', type=int, default=5, help='Update interval in seconds for watch mode (default: 5)')
    args = parser.parse_args()

#    debug_database_content()
#    debug_query_execution()
#    debug_database_dates()

    if args.verbose:
        print(f"DEBUG: Starting with arguments: {vars(args)}", file=sys.stderr)
    
    if args.watch and not args.real_time:
        args.real_time = True

    if args.watch:
        # Watch mode - continuously update
        try:
            while True:
                # Clear screen (Unix/Linux)
                print("\033c", end="")
                
                stats = get_job_stats(
                    days=args.days, 
                    user=args.user, 
                    machine=args.machine, 
                    real_time=args.real_time,
                    verbose=args.verbose
                )
                
                # Print header with timestamp
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"PBS Job Statistics - {stats['period']} - {current_time}")
                print(f"Total Jobs: {stats['total_jobs']}")
                
                if stats['results']:
                    print_job_details(stats['results'], args.verbose, stats['is_real_time'])
                else:
                    print("\nNo jobs found matching the specified criteria.")
                
                print(f"\nRefreshing every {args.interval} seconds... (Ctrl+C to stop)")
                time.sleep(args.interval)
                
        except KeyboardInterrupt:
            print("\nMonitoring stopped.")
            
    else:
        # Single execution mode
        if args.verbose:
            print(f"DEBUG: Querying jobs for user={args.user}, machine={args.machine}, "
                  f"real_time={args.real_time}", file=sys.stderr)
        
        stats = get_job_stats(
            days=args.days,
            date=args.date,
            start_date=args.start_date,
            end_date=args.end_date,
            user=args.user, 
            machine=args.machine, 
            real_time=args.real_time,
            verbose=args.verbose
        )
        
        if args.real_time or args.jobs:
            print(f"\nPBS Job Statistics - {stats['period']}")
            print(f"Total Jobs: {stats['total_jobs']}")
        
            if args.verbose and not stats['results']:
                print("DEBUG: No results found with current filters", file=sys.stderr)
        
            if stats['results']:
                print_job_details(stats['results'], args.verbose, stats['is_real_time'])
            else:
                print("\nNo jobs found matching the specified criteria.")
        else:
            print(f"\nPBS Job Statistics - {stats['period']}")
            print(f"Total Jobs: {stats['total_jobs']}")

            if args.verbose:
                print(f"DEBUG: Found {len(stats['results'])} matching jobs", file=sys.stderr)

            if stats['results']:
                print("\n{:<20} {:<15} {:<10} {:<15}".format(
                    "User", "Machine", "Jobs", "Last Run"))
                print("-" * 60)

                for row in stats['results']:
                    print("{:<20} {:<15} {:<10} {:<15}".format(
                        row['user'],
                        row['machine'],
                        row['jobs'],
                        row['last_run']))
            else:
                print("\nNo jobs found matching the specified criteria.")

if __name__ == "__main__":
    main()

