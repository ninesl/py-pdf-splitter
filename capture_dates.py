#!/usr/bin/env python3
import sys
import argparse
from datetime import datetime, timedelta

def parse_date(date_str):
    """Parse date string in various formats (DD-MM-YYYY, MM/DD/YYYY, YYYY-MM-DD)"""
    formats = [
        '%d-%m-%Y',
        '%m/%d/%Y', 
        '%Y-%m-%d',
        '%d/%m/%Y',
        '%m-%d-%Y'
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    raise ValueError(f"Unable to parse date: {date_str}")

def generate_date_range(start_date, end_date, output_format='%d %m %Y'):
    """Generate all dates between start and end (inclusive)"""
    current = start_date
    dates = []
    
    while current <= end_date:
        dates.append(current.strftime(output_format))
        current += timedelta(days=1)
    
    return dates

def main():
    parser = argparse.ArgumentParser(
        description='Capture all dates from a given range for ETL pipeline processing'
    )
    
    parser.add_argument(
        'start_date',
        help='Start date (formats: DD-MM-YYYY, MM/DD/YYYY, YYYY-MM-DD)'
    )
    
    parser.add_argument(
        'end_date',
        help='End date (formats: DD-MM-YYYY, MM/DD/YYYY, YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--format',
        default='%d %m %Y',
        help='Output format (default: "%%d %%m %%Y" for compatibility with pdf_parse.py)'
    )
    
    parser.add_argument(
        '--separator',
        default='\n',
        help='Separator between dates (default: newline)'
    )
    
    parser.add_argument(
        '--reverse',
        action='store_true',
        help='Output dates in reverse chronological order'
    )
    
    args = parser.parse_args()
    
    try:
        start = parse_date(args.start_date)
        end = parse_date(args.end_date)
        
        if start > end:
            print(f"Error: Start date ({args.start_date}) is after end date ({args.end_date})", file=sys.stderr)
            sys.exit(1)
        
        dates = generate_date_range(start, end, args.format)
        
        if args.reverse:
            dates.reverse()
        
        print(args.separator.join(dates))
        
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()