#!/usr/bin/env python3
import sys
from datetime import datetime, timedelta

def parse_date(date_str):
    """Parse date string in PostgreSQL format (YYYY-MM-DD)"""
    formats = [
        '%Y-%m-%d',
        '%d-%m-%Y',
        '%m/%d/%Y',
        '%d/%m/%Y',
        '%m-%d-%Y'
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    raise ValueError(f"Unable to parse date: {date_str}")

def generate_date_range(start_date, end_date=None):
    """Generate all dates between start and end (inclusive)"""
    if end_date is None:
        # Default to yesterday
        end_date = datetime.now() - timedelta(days=1)
    
    current = start_date
    dates = []
    
    while current <= end_date:
        # Output format matches what pdf_parse.py expects: DAY MONTH YEAR
        day = str(current.day)
        month = str(current.month)
        year = str(current.year)
        dates.append(f"{day} {month} {year}")
        current += timedelta(days=1)
    
    return dates

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} START_DATE [END_DATE]", file=sys.stderr)
        print("START_DATE: Date in format YYYY-MM-DD (from PostgreSQL)", file=sys.stderr)
        print("END_DATE: Optional, defaults to yesterday", file=sys.stderr)
        sys.exit(1)
    
    start_date_str = sys.argv[1]
    end_date = None
    
    if len(sys.argv) >= 3:
        end_date_str = sys.argv[2]
        try:
            end_date = parse_date(end_date_str)
        except ValueError as e:
            print(f"Error parsing end date: {e}", file=sys.stderr)
            sys.exit(1)
    
    try:
        start = parse_date(start_date_str)
        dates = generate_date_range(start, end_date)
        
        for date_str in dates:
            print(date_str)
        
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()