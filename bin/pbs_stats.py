#!/usr/bin/python3
# /usr/local/bin/pbs_stats.py
import sqlite3
from datetime import datetime, timedelta
import argparse
import sys
import json

DB_PATH = '/var/lib/pbs_monitor/pbs_stats.db'

def parse_pbs_date(date_str):
    """Convert PBS date format to datetime object"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%a %b %d %H:%M:%S %Y")
    except ValueError:
        return None

def format_pbs_date(date_str):
    """Format PBS date string to DD-MM-YYYY HH:MM:SS"""
    dt = parse_pbs_date(date_str)
    return dt.strftime("%d-%m-%Y %H:%M:%S") if dt else "N/A"

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

def get_job_stats(days=None, user=None, machine=None, verbose=False):
    """Get detailed job information with all available fields"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    # Build WHERE clauses
    conditions = []
    params = []
   
   # Date filtering in Python (since dates are in text format)
   # We'll filter dates after fetching due to format issues
   
    if user:
        conditions.append("user = ?")
        params.append(user)
    if machine:
        conditions.append("machine = ?")
        params.append(machine)
    
    # Ensure we don't count NULL entries
    conditions.append("user IS NOT NULL AND machine IS NOT NULL")
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    # Handle date range display
    if days == 'all':
        c.execute(f"SELECT MIN(start_time) FROM jobs WHERE {where_clause}", params)
        min_date_str = c.fetchone()[0]
        formatted_date = format_pbs_date(min_date_str) if min_date_str else "unknown date"
        date_range = f"Complete history (since {formatted_date})"
    else:
        days = int(days) if days else 7
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        date_range = f"Last {days} days (since {start_date.strftime('%Y-%m-%d')})"

    # Main query
    query = f"""
        SELECT 
            user, 
            machine, 
            start_time,
            COUNT(*) as job_count
        FROM jobs
        WHERE {where_clause}
        GROUP BY user, machine
        ORDER BY job_count DESC
    """
    #query = f"""
    #    SELECT
    #        job_id,
    #        user,
    #        machine,
    #        start_time,
    #        data_json
    #    FROM jobs
    #    WHERE {where_clause}
    #    ORDER BY start_time DESC
    #"""
    
    if verbose:
        print(f"DEBUG: Executing query: {query}", file=sys.stderr)
        print(f"DEBUG: With parameters: {params}", file=sys.stderr)
    
    c.execute(query, params)
    results = []
    total_jobs = 0
    
    for row in c.fetchall():
        job_date = parse_pbs_date(row['start_time'])
        if days == 'all' or (job_date and (datetime.now() - job_date <= timedelta(days=days))):
            total_jobs += row['job_count']
            results.append({
                'user': row['user'],
                'machine': row['machine'],
                'jobs': row['job_count'],
                'last_run': format_pbs_date(row['start_time'])
            })

#    for row in c.fetchall():
#        job_data = json.loads(row['data_json'])
#        resources_used = job_data.get('resources_used', {})

        # Handle walltime conversion
#        walltime_str = resources_used.get('walltime', 'N/A')
#        walltime_seconds = parse_walltime(walltime_str) if walltime_str != 'N/A' else None

#        results.append({
#            'job_id': row['job_id'],
#            'user': row['user'],
#            'machine': row['machine'],
#            'start_time': format_pbs_date(row['start_time']),
#            'queue': job_data.get('queue', 'N/A'),
#            'job_name': job_data.get('Job_Name', 'N/A'),
#            'state': job_data.get('job_state', 'N/A'),
#            'resources': {
#                'cpus': job_data.get('Resource_List', {}).get('ncpus', 'N/A'),
#                'mem': job_data.get('Resource_List', {}).get('mem', 'N/A'),
#                'walltime': job_data.get('Resource_List', {}).get('walltime', 'N/A'),
#            },
#            'used': {
#                'cpus': job_data.get('resources_used', {}).get('ncpus', 'N/A'),
#                'mem': job_data.get('resources_used', {}).get('mem', 'N/A'),
#                'walltime': format_duration(job_data.get('resources_used', {}).get('walltime')),
#                'cpu_time': format_duration(job_data.get('resources_used', {}).get('cput')),
#            },
#            'exit_status': job_data.get('exit_status', 'N/A'),
#            'submit_args': job_data.get('Submit_arguments', 'N/A'),
#        })

    conn.close()
    return {
        'period': date_range,
        'total_jobs': total_jobs,
        'results': results
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

    # Date filtering
    if days and days != 'all':
        try:
            days = int(days)
            cutoff_date = datetime.now() - timedelta(days=days)
            conditions.append("start_time >= ?")
            params.append(cutoff_date.strftime("%d-%m-%Y"))
        except ValueError:
            pass

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

    c.execute(query, params)
    results = []

    for row in c.fetchall():
        job_data = json.loads(row['data_json'])
        resources_used = job_data.get('resources_used', {})

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

def print_job_details(jobs, verbose=False):
    """Print detailed job information in a readable format"""
    if not jobs:
        print("No jobs found matching the specified criteria.")
        return

    # Print header
    print("\n{:<12} {:<15} {:<20} {:<10} {:<15} {:<10} {:<12} {:<12} {:<12}".format(
        "Job ID", "User", "Machine", "Queue", "Job Name", "State", "Start Time", "CPU Used", "Walltime"))
    print("-" * 120)
    
    for job in jobs:
        # Safely extract basic information
        job_id = str(job.get('job_id', 'N/A')).split('.')[0]  # Show only the main job ID part
        user = str(job.get('user', 'N/A'))
        machine = str(job.get('machine', 'N/A'))
        queue = str(job.get('queue', 'N/A'))
        job_name = str(job.get('job_name', 'N/A'))
        job_name = job_name[:15] + '...' if len(job_name) > 15 else job_name
        state = str(job.get('state', 'N/A'))

        start_time = 'N/A'
        if job.get('start_time'):
            start_time_parts = str(job['start_time']).split()
            start_time = start_time_parts[0] if start_time_parts else 'N/A'
        
        # Extract resource usage
        cpu_used = str(job.get('used', {}).get('cpus', 'N/A'))
        walltime = str(job.get('used', {}).get('walltime', 'N/A'))
        
        # Print compact job line
        print("{:<12} {:<15} {:<20} {:<10} {:<15} {:<10} {:<12} {:<12} {:<12}".format(
            job_id,
            user,
            machine,
            queue,
            job_name,
            state,
            start_time,
            cpu_used,
            walltime))
        
        # Show additional details in verbose mode
        if verbose:
            print("\nAdditional Details:")
            print(f"  Full Start Time: {job.get('start_time', 'N/A')}")
            resources = job.get('resources', {})
            print(f"  Resources Requested: CPUs={resources.get('cpus', 'N/A')}, Mem={resources.get('mem', 'N/A')}, Walltime={resources.get('walltime', 'N/A')}")
            used = job.get('used', {})
            print(f"  Resources Used: CPUs={used.get('cpus', 'N/A')}, Mem={used.get('mem', 'N/A')}, Walltime={used.get('walltime', 'N/A')}, CPU Time={used.get('cpu_time', 'N/A')}")
            print(f"  Exit Status: {job.get('exit_status', 'N/A')}")
            submit_args = job.get('submit_args', 'N/A')
            if submit_args != 'N/A':
                print(f"  Submit Arguments: {submit_args}")
            print("-" * 60)

def main():
    parser = argparse.ArgumentParser(description='PBS Job Statistics')
    parser.add_argument('--days', nargs='?', const=7, default=None,
                       help='Number of days to report (default: all), use --days 7 for last week')
    parser.add_argument('--user', help='Filter by specific user')
    parser.add_argument('--machine', help='Filter by specific compute node')
    parser.add_argument('--jobs', action='store_true',
                       help='Show detailed job information')
    parser.add_argument('--verbose', action='store_true',
                       help='Show additional debug information and details')
    args = parser.parse_args()

    if args.verbose:
        print(f"DEBUG: Starting with arguments: {vars(args)}", file=sys.stderr)

    if args.jobs:
        jobs = get_job_details(
            user=args.user,
            machine=args.machine,
            days=args.days,
            verbose=args.verbose
        )
        # Print header for job listing
        if args.user:
            user_filter = f" for user '{args.user}'"
        elif args.machine:
            user_filter = f" on machine '{args.machine}'"
        else:
            user_filter = ""
            
        if args.days and args.days != 'all':
            date_range = f" from last {args.days} days"
        else:
            date_range = ""
            
        print(f"\nPBS Job Details{user_filter}{date_range}")
        print(f"Total Jobs: {len(jobs)}")
        
        print_job_details(jobs, args.verbose)
    else:
        # Original summary functionality (unchanged)
        stats = get_job_stats(
            days=args.days if args.days else 'all',
            user=args.user,
            machine=args.machine,
            verbose=args.verbose
        )

        # Print report
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
