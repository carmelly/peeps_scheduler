import time 
import math 
import itertools
import tracemalloc

''' 
Temporary file testing out different scenarios for walking 
eventsequence permutations, in an attempt to speed up evaluation 
'''
num_events = 10


def perm_wrapper():
   
	s_perm =  [0] * num_events
	s_perms = [[0] * num_events] * math.factorial(num_events)
	cur_perm = 0

	s_used = [False] * num_events
	s_current = [0] * num_events

	def generatePermutations(index): 
		if (index == num_events): 
			# s_perms.append(s_current); 
			return	

		for i in range(num_events):
			if (s_used[i]):
				continue

			s_used[i] = True
			s_current[index] = i
			generatePermutations(index + 1)
			s_used[i] = False

	generatePermutations(0)


def walk_permutations_bf(perms): 
	cache = {} 

	for perm in perms: 
		history = [] 
		for event_id in perm: 
			num = 0 
			prefix  = tuple(history)
			history.append(event_id)
			path = tuple(history) # (0,)
			
			if path in cache: 
				continue 

			found_cache = cache.get(prefix, None)
			if not found_cache: 
				assert len(history) == 1 
 			
			if found_cache: 
				num = found_cache['num'] # in reality, deepcopy 
			
			num += 1 
			cache[path] = {"num": num}


def walk_permutations(perms): 
	cache = [{}] * num_events 
	history = [] 

	for perm in perms: 
		history.clear()
		for i, event_id in enumerate(perm): 
			num = 0 
			history.append(event_id)
			path = tuple(history) # (0,)

			found_cache = cache[i].get('path', None) if cache[i] else None
			if found_cache != path: 
				if i > 0: 
					num = cache[i-1]['num']
				num += 1 
				cache[i] = {'path': path, 'num': num}

		
		
			
		
if __name__ == "__main__":

	index_sequences = list(itertools.permutations([i for i in range(num_events)], num_events))

	# Start tracking
	tracemalloc.start()
	
	start_time = time.perf_counter()  # Start timing
	walk_permutations(index_sequences)
	end_time = time.perf_counter()  # End timing
	elapsed_time = end_time - start_time
	print(f"Evaluation complete. Elapsed time: {elapsed_time:.2f} seconds")
	
	# Get memory usage
	current, peak = tracemalloc.get_traced_memory()
	print(f"Current memory usage: {current / 1024 / 1024:.2f} MB")
	print(f"Peak memory usage: {peak / 1024 / 1024:.2f} MB")
	# Stop tracing
	tracemalloc.stop()
		
	# Start tracking
	tracemalloc.start()
	start_time = time.perf_counter()  # Start timing
	walk_permutations(index_sequences)
	end_time = time.perf_counter()  # End timing
	elapsed_time = end_time - start_time
	print(f"Elapsed time: {elapsed_time:.2f} seconds")
	# Get memory usage
	current, peak = tracemalloc.get_traced_memory()
	print(f"Current memory usage: {current / 1024 / 1024:.2f} MB")
	print(f"Peak memory usage: {peak / 1024 / 1024:.2f} MB")
	# Stop tracing
	tracemalloc.stop()



