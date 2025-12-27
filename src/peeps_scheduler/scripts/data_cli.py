#!/usr/bin/env python3
"""
Simple CLI for data period management using the DataManager.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from peeps_scheduler.data_manager import DataManager


def create_period(period_slug: str) -> bool:
	"""Create a new period directory for working."""
	try:
		data_manager = DataManager()
		period_path = data_manager.ensure_period_exists(period_slug)
		print(f"✅ Period directory created: {period_path}")
		print(f"💡 You can now:")
		print(f"   - Put your members.csv and responses.csv in {period_path}")
		print(f"   - Run scheduler with --data-folder {period_slug}")
		return True
	except Exception as e:
		print(f"❌ Failed to create period: {e}")
		return False


def list_periods() -> None:
	"""List all periods."""
	try:
		data_manager = DataManager()
		periods = data_manager.list_periods()
		
		if not periods:
			print("📁 No periods found")
			return
		
		print(f"📁 Available Periods ({len(periods)}):")
		for period in sorted(periods):
			period_path = data_manager.get_period_path(period)
			
			# Count files in period
			file_count = len([f for f in period_path.glob("*") if f.is_file()])
			
			print(f"   📅 {period} ({file_count} files)")
		print()
		
	except Exception as e:
		print(f"❌ Failed to list periods: {e}")


def show_period_details(period_slug: str) -> None:
	"""Show details for a specific period."""
	try:
		data_manager = DataManager()
		period_path = data_manager.get_period_path(period_slug)
		
		if not period_path.exists():
			print(f"❌ Period not found: {period_slug}")
			return
		
		print(f"📅 Period: {period_slug}")
		print(f"📍 Location: {period_path}")
		
		# List files
		files = [f for f in period_path.glob("*") if f.is_file()]
		print(f"\n📁 Files ({len(files)}):")
		for file_path in sorted(files):
			size_kb = file_path.stat().st_size / 1024
			print(f"   📄 {file_path.name} ({size_kb:.1f} KB)")
			
	except Exception as e:
		print(f"❌ Failed to show period details: {e}")


def main():
	"""Main CLI interface."""
	parser = argparse.ArgumentParser(
		description="Peeps Data Management CLI",
		formatter_class=argparse.RawDescriptionHelpFormatter,
		epilog="""
Examples:
  # Create a new period directory for working
  python scripts/data_cli.py create --period 2025-10
  
  # List all periods
  python scripts/data_cli.py list-periods
  
  # Show details for specific period
  python scripts/data_cli.py show --period 2025-09
		"""
	)
	
	parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
	
	subparsers = parser.add_subparsers(dest="command", help="Available commands")
	
	# Create command
	create_parser = subparsers.add_parser("create", help="Create a new period directory")
	create_parser.add_argument("--period", required=True, help="Period slug (e.g., 2025-10)")
	
	# List periods command
	list_parser = subparsers.add_parser("list-periods", help="List all periods")
	
	# Show period details command
	show_parser = subparsers.add_parser("show", help="Show period details")
	show_parser.add_argument("--period", required=True, help="Period slug")
	
	args = parser.parse_args()
	
	if args.verbose:
		logging.basicConfig(level=logging.DEBUG)
	
	if args.command == "create":
		success = create_period(args.period)
		sys.exit(0 if success else 1)
		
	elif args.command == "list-periods":
		list_periods()
		
	elif args.command == "show":
		show_period_details(args.period)
		
	else:
		parser.print_help()
		sys.exit(1)


if __name__ == "__main__":
	main()
