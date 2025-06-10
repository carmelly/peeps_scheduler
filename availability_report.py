import csv
from collections import defaultdict
from models import Role

def load_csv(filename):
	with open(filename, newline='', encoding='utf-8') as csvfile:
		return list(csv.DictReader(csvfile))

def parse_availability(responses_file, members_file):
	members = {row["Email Address"].strip().lower(): row for row in load_csv(members_file)}
	availability = defaultdict(lambda: {"Lead": [], "Follow": []})
	unavailable = []
	responders = set()

	for row in load_csv(responses_file):
		email = row["Email Address"].strip().lower()
		role = row["Role"].strip()
		dates = [d.strip() for d in row["Availability"].split(",") if d.strip()]

		member = members.get(email)
		if not member:
			print(f"⚠️  Skipping unmatched email: {email}")
			continue
		if email in responders: 
			print(f"⚠️  Duplicate email: {email}")
			continue
		responders.add(email)

		# Identify responders with no availability 
		if not dates: 
			unavailable.append(member["Display Name"])

		for date in dates:
			availability[date][role].append(member["Display Name"])

	# Identify non-responders
	non_responders = [
		member["Display Name"] for email, member in members.items()
		if email not in responders and member.get("Active", "TRUE").upper() == "TRUE"
	]

	

	return availability, unavailable, non_responders

def print_availability(availability, unavailable, non_responders):
	for date in sorted(availability.keys()):
		print(f"\n📅  {date}")
		print(f"    Leaders  ({len(availability[date]['Lead'])}): {', '.join(availability[date]['Lead'])}")
		print(f"    Followers({len(availability[date]['Follow'])}): {', '.join(availability[date]['Follow'])}")
	
	print("\n🚫  No availability:")
	for name in sorted(unavailable):
		print(f"  - {name}")

	print("\n❌  Did not respond:")
	for name in sorted(non_responders):
		print(f"  - {name}")

if __name__ == "__main__":
	responses_file = "data/2025-07/responses.csv"
	members_file = "data/2025-07/members.csv"
	availability, unavailable, non_responders = parse_availability(responses_file, members_file)
	print_availability(availability, unavailable, non_responders)
