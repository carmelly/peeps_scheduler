# Peeps Event Scheduler

## Overview

Peeps Event Scheduler is a scheduling system designed to fairly assign dancers to events based on their preferences, availability, and participation history. The primary goal is to maximize overall attendance while ensuring a balanced number of leaders and followers at each event. The system also prioritizes dancers who have historically had lower availability, giving everyone an equitable opportunity to participate.

## Features

- **Maximizes Participation:** Evaluates all possible event sequences to assign as many dancers as possible.
- **Prioritizes Low-Availability Dancers:** Dancers with fewer past assignments receive higher priority.
- **Balances Roles:** Ensures an even number of leaders and followers for each event.
- **Enforces Personal Limits:** Honors each dancer's maximum number of sessions and minimum interval days between events.
- **Handles Complex Constraints Automatically:** Filters out events that can't be filled and removes redundant event sequences.
- **Flexible Data Input:** Supports loading dancer preferences and availability from CSV files or pre-generated JSON.

## How It Works

1. **Data Input:**

   - Dancer information (roles, limits, availability) and event details are loaded from CSV files or JSON.

2. **Sanitization:**

   - Events that cannot meet minimum attendance requirements are filtered out.

3. **Permutation & Evaluation:**

   - The system generates all possible orders of remaining events.
   - Each sequence is evaluated to assign dancers while respecting their limits and balancing roles.

4. **Optimization:**

   - Duplicate sequences are removed.
   - The sequence with the highest participation and balance is selected.

5. **Output:**

   - The results can be exported for use in class rosters or to update scheduling systems.
