import csv
from collections import defaultdict
from models import Role

def load_csv(filename):
	with open(filename, newline='', encoding='utf-8') as csvfile:
		return list(csv.DictReader(csvfile))

def parse_availability(responses_file, members_file):
	members = {row["Email Address"].strip().lower(): row for row in load_csv(members_file)}
	availability = defaultdict(lambda: {"Lead": [], "Follow": []})

	for row in load_csv(responses_file):
		email = row["Email Address"].strip().lower()
		role = row["Role"].strip()
		dates = [d.strip() for d in row["Availability"].split(",") if d.strip()]
		
		member = members.get(email)
		if not member:
			print(f"⚠️ Skipping unmatched email: {email}")
			continue

		for date in dates:
			availability[date][role].append(member["Name"])

	return availability

def print_availability(availability):
	for date in sorted(availability.keys()):
		print(f"\n📅 {date}")
		print(f"  Leaders  ({len(availability[date]['Lead'])}): {', '.join(availability[date]['Lead'])}")
		print(f"  Followers({len(availability[date]['Follow'])}): {', '.join(availability[date]['Follow'])}")

if __name__ == "__main__":
	responses_file = "data/2025-06/responses.csv"
	members_file = "data/2025-06/members.csv"
	availability = parse_availability(responses_file, members_file)
	print_availability(availability)
