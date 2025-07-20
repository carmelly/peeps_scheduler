import csv
from collections import defaultdict
from models import Role, SwitchPreference
from utils import parse_event_date

def load_csv(filename):
	with open(filename, newline='', encoding='utf-8') as csvfile:
		return list(csv.DictReader(csvfile))

def parse_availability(responses_file, members_file):
	members = {row["Email Address"].strip().lower(): row for row in load_csv(members_file)}
	availability = defaultdict(lambda: {"Leader": [], "Follower": [], "Leader_fill": [], "Follower_fill": []})
	unavailable = []
	responders = set()

	for row in load_csv(responses_file):
		email = row["Email Address"].strip().lower()
		role = Role(row["Primary Role"].strip())
		switch_pref = SwitchPreference.from_string(row["Secondary Role"].strip())
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
			availability[date][role.value].append(member["Display Name"])
			if switch_pref != SwitchPreference.PRIMARY_ONLY: 
				availability[date][f"{role.opposite().value}_fill"].append(member["Display Name"])

	# Identify non-responders
	non_responders = [
		member["Display Name"] for email, member in members.items()
		if email not in responders and member.get("Active", "TRUE").upper() == "TRUE"
	]
	return availability, unavailable, non_responders

def print_availability(availability, unavailable, non_responders):
	
	for date in sorted(availability.keys(), key=lambda d:parse_event_date(d)):
		print(f"\n📅  {date}")
		print(f"    Leaders  ({len(availability[date]['Leader'])}): {', '.join(availability[date]['Leader'])} ( + {', '.join(availability[date]['Leader_fill'])})")
		print(f"    Followers({len(availability[date]['Follower'])}): {', '.join(availability[date]['Follower'])} ( + {', '.join(availability[date]['Follower_fill'])})")
	
	print("\n🚫  No availability:")
	for name in sorted(unavailable):
		print(f"  - {name}")

	print("\n❌  Did not respond:")
	for name in sorted(non_responders):
		print(f"  - {name}")

if __name__ == "__main__":
	responses_file = "data/2025-09/responses.csv"
	members_file = "data/2025-09/members.csv"
	availability, unavailable, non_responders = parse_availability(responses_file, members_file)
	print_availability(availability, unavailable, non_responders)
