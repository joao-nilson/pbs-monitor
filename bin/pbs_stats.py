#!/usr/bin/python3
# /usr/local/bin/pbs_stats.py
import sqlite3
from datetime import datetime, timedelta
import argparse
import sys

DB_PATH = '/var/lib/pbs_monitor/pbs_stats.db'

def parse_pbs_date(date_str):
    """Convert PBS date format to datetime object"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%a %b %d %H:%M:%S %Y")
    except ValueError:
        return None

def get_job_stats(days=7, user=None, machine=None):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
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
    
    c.execute(query, params)
    results = []
    total_jobs = 0
    
    for row in c.fetchall():
        job_date = parse_pbs_date(row['start_time'])
        if job_date and start_date <= job_date <= end_date:
            total_jobs += row['job_count']
            results.append({
                'user': row['user'],
                'machine': row['machine'],
                'jobs': row['job_count'],
                'last_run': row['start_time']
            })
    
    conn.close()
    return {
        'period': f"Last {days} days (since {start_date.strftime('%Y-%m-%d')})",
        'total_jobs': total_jobs,
        'results': results
    }

def main():
    parser = argparse.ArgumentParser(description='PBS Job Statistics')
    parser.add_argument('--days', type=int, default=7, 
                       help='Number of days to report (default: 7)')
    parser.add_argument('--user', 
                       help='Filter results by specific user')
    parser.add_argument('--machine', 
                       help='Filter results by specific compute node')
    parser.add_argument('--verbose', action='store_true',
                       help='Show additional debug information')
    args = parser.parse_args()
    
    if args.verbose:
        print(f"DEBUG: Querying jobs for user={args.user}, machine={args.machine}", 
              file=sys.stderr)
    
    stats = get_job_stats(days=args.days, user=args.user, machine=args.machine)
    
    # Print report
    print(f"\nPBS Job Statistics - {stats['period']}")
    print(f"Total Jobs: {stats['total_jobs']}")
    
    if args.verbose and not stats['results']:
        print("DEBUG: No results found with current filters", file=sys.stderr)
    
    if stats['results']:
        print("\n{:<20} {:<15} {:<10} {:<25}".format(
            "User", "Machine", "Jobs", "Last Run"))
        print("-" * 70)
        
        for row in stats['results']:
            print("{:<20} {:<15} {:<10} {:<25}".format(
                row['user'],
                row['machine'],
                row['jobs'],
                row['last_run']))
    else:
        print("\nNo jobs found matching the specified criteria.")

if __name__ == "__main__":
    main()
