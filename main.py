import random
import copy

verbosity = 1

class Person:
	def __init__(self, id, name):
		self.id = id
		self.name = name
		self.weight = 0
		self.availability_coef = 1.0
		self.attended = 0
		self.opportunities = 0
		self.guarantee_flag = False

	def __str__(self):
		return (f"{self.name}, attended: {self.attended}, opportunities: {self.opportunities}, ratio: {(self.attended / self.opportunities):.2f} weight: {self.weight}, ability_coef: {self.availability_coef:.2f}")


def print_spread(peeps):
	attended = [peep.attended for peep in peeps]
	print(f"min: {min(attended)}, max: {max(attended)}")

def event(peeps, open_spots):
	# this is a stable sort: the relative order is preserved 
	sorted_peeps = sorted(peeps, reverse=True, key=lambda peep: peep.weight)

	if verbosity > 1:
		print("==========EVENT==========")
		print("All Peeps:")
		for peep in peeps:
			print(f"    {str(peep)}")	
		print("Sorted Peeps:")
		for peep in sorted_peeps:
			print(f"    {str(peep)}")	
	
	# peeps who can attend
	attendees = []
	# peeps who cant attend
	losers = []

	# go through all the peeps while there are open spots
	cur_peep = 0
	while open_spots > 0 and cur_peep < len(sorted_peeps):
		peep = sorted_peeps[cur_peep]
		# mark as an opportunity
		peep.opportunities += 1
		# dice roll for this peep for this event
		if peep.guarantee_flag == True or peep.availability_coef >= random.uniform(0.0, 1.0):
			attendees.append(peep)
			open_spots -= 1
			# mark as attended
			peep.weight = 0
			peep.attended += 1
		else: 
			losers.append(peep)
		cur_peep += 1

		#always clear guarantee flag
		peep.guarantee_flag = False

	for peep in peeps:
		if peep not in attendees:
			peep.weight += 1

	# take everyone who was asked and put them @ the bottom of the list in the order that they appear
	# we use the "peeps" list and not the sorted list as it maintains the snapshot of relative order independent of who was selected- the list is sorted when complete
	for peep in losers:
		# find the index for peep in peeps, remove it, then append the peep @ the end of the list
		for i in range(len(peeps)):
			if peeps[i].id == peep.id:
				peeps.pop(i)
				peeps.append(peep)
				break

	# take everyone who attended and do the same, but first randomize the winners - this prevents cyclical results based on relative availability between two peeps
	random_attendees = copy.deepcopy(attendees)
	random.shuffle(random_attendees)
	for peep in random_attendees:
		# find the index for peep in peeps, remove it, then append the peep @ the end of the list
		for i in range(len(peeps)):
			if peeps[i].id == peep.id:
				peeps.pop(i)
				peeps.append(peep)
				break
		
	if verbosity > 1:
		print("Attendees:")
		for peep in attendees:
			print(f"    {str(peep)}")	
		
	# re-sort
	return sorted(peeps, reverse=True, key=lambda peep: peep.weight)

# run simulatiuon 
def sim(in_peeps, event_size, num_events):
	peeps = copy.deepcopy(in_peeps)
		
	for i in range(num_events):
		peeps = event(peeps, event_size)

	if verbosity > 0:
		print("RESULTS")
		for peep in peeps:
			print(f"    {str(peep)}")	

	print_spread(peeps)
	return sorted(peeps, reverse=True, key=lambda peep: peep.weight)

def import_file( ):
	pass

def main():
	num_peeps = 15
	event_size = 6
	num_events = 2000
	peeps = [
		Person(i, f"person{i}") for i in range(num_peeps)
	]

	peeps[0].availability_coef = 1.0
	peeps[1].availability_coef = 1.0
	peeps[2].availability_coef = .8
	peeps[3].availability_coef = .8 
	peeps[4].availability_coef = .8 
	peeps[5].availability_coef = .6 
	peeps[6].availability_coef = .6 
	peeps[7].availability_coef = .6 
	peeps[8].availability_coef = .6 
	peeps[9].availability_coef = .6 
	peeps[10].availability_coef = .4 
	peeps[11].availability_coef = .4 
	peeps[12].availability_coef = .4 
	peeps[13].availability_coef = .4 
	peeps[14].availability_coef = .1 

	def randomize_availability():
		for peep in peeps:
			peep.availability_coef = random.uniform(0.25, 1.0)
	# randomize_availability()

	sim(peeps, event_size, num_events)	

if __name__ == "__main__":
	main()