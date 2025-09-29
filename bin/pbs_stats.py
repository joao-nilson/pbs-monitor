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


def get_job_stats(days=None, user=None, machine=None, real_time=None, verbose=False):
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

    # Build WHERE clauses
    conditions = []
    params = []

    # Date filtering - disabled for now
    if days and days != 'all':
        try:
            days_int = int(days)
            if verbose:
                print(f"DEBUG: Date filtering disabled - all data is from 2025", file=sys.stderr)
        except ValueError:
            pass

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

    if days == 'all' or not days:
        date_range = "All history"
    else:
        date_range = f"Last {days} days"

    # Main query - FIXED: Add WHERE clause and handle parameters correctly
    if conditions:
        # If we have conditions, use parameterized query
        query = f"""
            SELECT
                user,
                COALESCE(machine, 'N/A') as machine,
                MAX(start_time) as last_run,
                COUNT(*) as job_count
            FROM jobs
            WHERE {where_clause}
            GROUP BY user, COALESCE(machine, 'N/A')
            ORDER BY job_count DESC
        """
    else:
        # If no conditions, use simple query without parameters
        query = """
            SELECT
                user,
                COALESCE(machine, 'N/A') as machine,
                MAX(start_time) as last_run,
                COUNT(*) as job_count
            FROM jobs
            GROUP BY user, COALESCE(machine, 'N/A')
            ORDER BY job_count DESC
        """
        params = []  # Clear params since we don't need them

    if verbose:
        print(f"DEBUG: Executing query: {query}", file=sys.stderr)
        print(f"DEBUG: With parameters: {params}", file=sys.stderr)
        print(f"DEBUG: Date range: {date_range}", file=sys.stderr)

    # Execute the query
    try:
        if params:
            c.execute(query, params)
        else:
            c.execute(query)
        
        # DEBUG: Check what we actually got
        rows = c.fetchall()
        if verbose:
            print(f"DEBUG: Raw SQL result - {len(rows)} rows returned", file=sys.stderr)
            for i, row in enumerate(rows[:3]):  # Show first 3 rows
                print(f"DEBUG: Row {i}: user='{row['user']}', machine='{row['machine']}', jobs={row['job_count']}", file=sys.stderr)
     
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

    results = []

    # Process the results
    for row in rows:
        results.append({
            'user': row['user'],
            'machine': row['machine'],
            'jobs': row['job_count'],
            'last_run': format_pbs_date(row['last_run'])
        })

    # Get total jobs count using the same WHERE clause
    count_query = f"SELECT COUNT(*) as total FROM jobs WHERE {where_clause}"
    try:
        if params:
            c.execute(count_query, params)
        else:
            c.execute(count_query)

        total_jobs_result = c.fetchone()
        total_jobs = total_jobs_result['total'] if total_jobs_result else 0

    except Exception as e:
        if verbose:
            print(f"DEBUG: Count query failed: {e}", file=sys.stderr)
        total_jobs = 0

    conn.close()

    if verbose:
        print(f"DEBUG: Found {total_jobs} total jobs, {len(results)} user-machine combinations", file=sys.stderr)

    return {
        'period': date_range,
        'total_jobs': total_jobs,
        'results': results,
        'is_real_time': False
    }

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

def main():
    parser = argparse.ArgumentParser(description='PBS Job Statistics')
    parser.add_argument('--days', '-d', nargs='?', const=7, default=None,
                       help='Number of days to report (default: last week), use --days all or -d all for entire history, --days 30 for last 30 days, etc.')
    parser.add_argument('--user', help='Filter by specific user')
    parser.add_argument('--machine', help='Filter by specific compute node')
    parser.add_argument('--real-time', '-r', action='store_true',
                       help='Show real-time job information')
    parser.add_argument('--jobs', action='store_true',
                       help='Show detailed job information')
    parser.add_argument('--verbose', action='store_true',
                       help='Show additional debug information and details')
    parser.add_argument('--watch', '-w', action='store_true',
                       help='Watch mode - continuously update real-time information')
    parser.add_argument('--interval', '-n', type=int, default=5,
                       help='Update interval in seconds for watch mode (default: 5)')
    args = parser.parse_args()

#    debug_database_content()
#    debug_query_execution()

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

