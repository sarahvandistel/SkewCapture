#!/usr/bin/env python3
"""
CLI script to run the complete SkewCapture pipeline.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from skewcapture.runner import run_pipeline


def main():
    """Main CLI entry point."""
    if len(sys.argv) != 2:
        print("Usage: python scripts/run_pipeline.py YYYY-MM-DD")
        print("Example: python scripts/run_pipeline.py 2025-08-04")
        sys.exit(1)
    
    date_str = sys.argv[1]
    run_pipeline(date_str)


if __name__ == '__main__':
    main() 