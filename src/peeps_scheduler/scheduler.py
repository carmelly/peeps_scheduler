import copy
import logging
import time
from peeps_scheduler import constants
from peeps_scheduler import file_io
from peeps_scheduler.models import Event, EventSequence, Peep, Role, SwitchPreference
from peeps_scheduler import utils
from peeps_scheduler.data_manager import get_data_manager

class Scheduler:
    def __init__(
        self,
        data_folder,
        max_events,
        interactive=True,
        sequence_choice=0,
        cancellations_file="cancellations.json",
        partnerships_file="partnerships.json",
    ):
        self.data_folder = data_folder
        self.max_events = max_events
        self.interactive = interactive
        self.sequence_choice = (
            sequence_choice  # Which tied sequence to auto-select in non-interactive mode
        )
        self.cancellations_file = cancellations_file
        self.partnerships_file = partnerships_file
        self.data_manager = get_data_manager()
        self.partnership_requests = {}

        # Set up logging for scheduler
        from peeps_scheduler.logging_config import get_logger

        self.logger = get_logger("scheduler", "scheduler", console_output=True)

        # Ensure period directory exists
        self.period_path = self.data_manager.ensure_period_exists(data_folder)
        self.output_json = self.period_path / "output.json"
        self.result_json = self.period_path / "results.json"
        self.target_max = None  # max per role used for each run

    def sanitize_events(self, events, peeps):
        """Sanitize events to ensure there are enough leaders and followers to fill roles."""
        valid_events = []
        removed_events = []
        for event in events:
            num_leaders = sum(
                1 for peep in peeps if event.id in peep.availability and peep.role == Role.LEADER
            )
            num_followers = sum(
                1 for peep in peeps if event.id in peep.availability and peep.role == Role.FOLLOWER
            )

            if num_leaders >= constants.ABS_MIN_ROLE and num_followers >= constants.ABS_MIN_ROLE:
                valid_events.append(event)
            else:
                removed_events.append(event)

        return valid_events

    def evaluate_sequence(self, sequence: EventSequence, keep_invalid=False):
        """
        Evaluates an event sequence by assigning peeps to events and updating stats.
        Respects role limits, peep availability, and switch preferences.
        """
        for event in sequence.events:
            effective_max_role = min(event.max_role, self.target_max or event.max_role)

            # Attempt to assign each peep to this event
            for peep in sequence.peeps:
                if not peep.can_attend(event):
                    continue  # Skip if unavailable, over limit, or on cooldown

                primary_role = peep.role
                secondary_role = primary_role.opposite()

                # Try assigning in primary role
                if event.num_attendees(primary_role) < effective_max_role:
                    event.add_attendee(peep, primary_role)

                # Try secondary role if flexible and primary is full
                elif (
                    peep.switch_pref == SwitchPreference.SWITCH_IF_PRIMARY_FULL
                    and event.num_attendees(secondary_role) < effective_max_role
                ):
                    event.add_attendee(peep, secondary_role)
                    self.logger.debug(
                        f"{peep.name} assigned in secondary role {secondary_role.name} "
                        f"(primary was full) for Event {event.id} on {event.formatted_date()}"
                    )

                # Otherwise add as alternate in primary role
                else:
                    event.add_alternate(peep, primary_role)

            # Promote SWITCH_IF_NEEDED alternates if it enables the session to fill
            for role in [Role.LEADER, Role.FOLLOWER]:
                opposite_role = role.opposite()

                # Check if this role is underfilled
                if event.num_attendees(role) < event.min_role:
                    # Find SWITCH_IF_NEEDED alternates in opposite role who could help fill this role
                    eligible_alternates = [
                        peep
                        for peep in event.get_alternates(opposite_role)
                        if peep.switch_pref == SwitchPreference.SWITCH_IF_NEEDED
                    ]

                    # Promote them to the underfilled role until it meets min_role or we run out
                    for peep in eligible_alternates:
                        if event.num_attendees(role) >= event.min_role:
                            break  # Already filled, stop promoting

                        if event.num_attendees(role) < effective_max_role:
                            # Remove from alternate list in their primary role
                            event.remove_alternate(peep, opposite_role)
                            # Add as attendee in the underfilled role
                            event.add_attendee(peep, role)
                            self.logger.debug(
                                f"{peep.name} promoted from {opposite_role.name} alternate to {role.name} attendee "
                                f"(SWITCH_IF_NEEDED enables session fill) for Event {event.id} on {event.formatted_date()}"
                            )

            # Only consider events that meet the absolute minimums
            if event.meets_absolute_min():
                # Balance roles (demoting extras if needed)
                event.balance_roles()

                # If underfilled for event-specific duration, try to downgrade
                if not event.meets_min():
                    event.downgrade_duration()

            # Only keep event if it now meets per-duration min_role
            if event.meets_min():
                Peep.update_event_attendees(sequence.peeps, event)
                sequence.valid_events.append(event)
            else:
                if not keep_invalid:
                    event.clear_participants()

        # Remove any alternates who are now ineligible (e.g. due to attending another event)
        for event in sequence.valid_events:
            event.validate_alternates()

        # Update peep stats and compute utilization metrics
        sequence.finalize()
        sequence.calculate_partnerships_fulfilled(self.partnership_requests)

    def evaluate_all_event_sequences(self, og_peeps, og_events):
        """Generates and evaluates all possible event sequences based on peep availability and role limits."""
        event_perm = utils.generate_event_permutations(og_events)
        event_map = {event.id: event for event in og_events}
        sequences = []

        start_time = time.perf_counter()
        for perm in event_perm:
            events = [copy.deepcopy(event_map[id]) for id in perm]
            sequence = EventSequence(events, copy.deepcopy(og_peeps))
            self.evaluate_sequence(sequence)
            if sequence.valid_events:
                sequences.append(sequence)
        end_time = time.perf_counter()

        self.logger.debug(f"Evaluation complete. Elapsed time: {end_time - start_time:.2f}s")
        return sequences

    def remove_high_overlap_events(self, events, peeps, max_events):
        """
        Remove events that have the highest participant overlap with all other events in the list,
        until we have no more than max_events in the list. If overlap is the same, remove the lowest-weighted event.
        Returns a new list.
        """

        def find_overlapping_events(events, peeps):
            """
            Identify the event with the highest participant overlap.

            Overlap is calculated by counting the number of shared participants between each pair of events.
            If a peep is available for both event A and event B, they contribute to the overlap score for both events.
            """
            overlap_scores = {event.id: 0 for event in events}

            self.logger.debug("Computing event overlap...")

            # Create a lookup for peep availability
            peep_event_map = {peep.id: set(peep.availability) for peep in peeps}

            # Compute event overlap
            for i, event_a in enumerate(events):
                for j, event_b in enumerate(events):
                    if i >= j:
                        continue  # Avoid redundant checks

                    # Count shared peeps who are available for both events
                    shared_peeps = sum(
                        1
                        for peep in peeps
                        if event_a.id in peep_event_map[peep.id]
                        and event_b.id in peep_event_map[peep.id]
                    )

                    overlap_scores[event_a.id] += shared_peeps
                    overlap_scores[event_b.id] += shared_peeps

            self.logger.debug(f"Overlap scores: {overlap_scores}")
            return overlap_scores

        def find_event_to_remove(events, peeps):
            """
            Find the event with the highest overlap. If there's a tie, remove the event with the lowest weight.
            """
            overlap_scores = find_overlapping_events(events, peeps)
            max_overlap = max(overlap_scores.values())
            candidates = [event for event in events if overlap_scores[event.id] == max_overlap]

            self.logger.debug(
                f"Events with max overlap ({max_overlap}): {[event.id for event in candidates]}"
            )

            if len(candidates) == 1:
                return candidates[0]

            # Use weight as a tiebreaker
            event_weights = {
                event: sum(peep.priority for peep in peeps if event.id in peep.availability)
                for event in candidates
            }
            event_to_remove = min(event_weights, key=event_weights.get)

            self.logger.debug(f"Tie on overlap. Removing event based on lowest weight")
            return event_to_remove

        self.logger.debug(f"Initial event count: {len(events)}. Target event count: {max_events}.")
        while len(events) > max_events:
            event_to_remove = find_event_to_remove(events, peeps)
            self.logger.debug(
                f"Removing event: Event({event_to_remove.id}) Date: {event_to_remove.date}. Remaining events: {len(events) - 1}."
            )
            events = [event for event in events if event.id != event_to_remove.id]

        self.logger.debug(f"Final event count: {len(events)}.")
        return events

    def get_top_sequences(self, sequences):
        self.logger.debug(f"Evaluating {len(sequences)} total sequences")

        unique = EventSequence.get_unique_sequences(sequences)
        if not unique:
            return []

        # sort by each metric in order of importance
        sorted_unique = sorted(
            unique,
            key=lambda s: (
                -s.num_unique_attendees,  # Maximize how many got in
                -s.priority_fulfilled,  # Favor overdue people
                -s.mutual_unique_fulfilled,  # Mutual partnership requests (unique)
                -s.normalized_utilization,  # Capacity usage per-person
                -s.mutual_repeat_fulfilled,  # Mutual partnership repeats
                -s.one_sided_fulfilled,  # One-sided request bonus
            ),
        )

        best_unique = sorted_unique[0].num_unique_attendees
        best_priority = sorted_unique[0].priority_fulfilled
        best_mutual_unique = sorted_unique[0].mutual_unique_fulfilled
        best_util = sorted_unique[0].normalized_utilization
        best_mutual_repeat = sorted_unique[0].mutual_repeat_fulfilled
        best_one_sided = sorted_unique[0].one_sided_fulfilled

        # return all sequences tied by unique attendees
        return [
            s
            for s in sorted_unique
            if s.num_unique_attendees == best_unique
            and s.priority_fulfilled == best_priority
            and s.mutual_unique_fulfilled == best_mutual_unique
            and s.normalized_utilization == best_util
            and s.mutual_repeat_fulfilled == best_mutual_repeat
            and s.one_sided_fulfilled == best_one_sided
        ]

    def run(self, generate_test_data=False, load_from_csv=False):
        # Extract year from data_folder for cancellations parsing
        # (e.g., "2026-01" -> 2026) - handle both absolute paths and folder names
        from pathlib import Path

        folder_name = Path(self.data_folder).name
        try:
            year = (
                int(folder_name[:4])
                if folder_name and len(folder_name) >= 4 and folder_name[:4].isdigit()
                else None
            )
        except (ValueError, TypeError):
            year = None

        if generate_test_data:
            self.logger.info(f"Generating test data and saving to {self.output_json}")
            utils.generate_test_data(5, 30, self.output_json)
        elif load_from_csv:
            responses_csv = (self.period_path / "responses.csv").as_posix()
            peeps_csv = (self.period_path / "members.csv").as_posix()
            self.logger.info(f"Loading data from {peeps_csv} and {responses_csv}")

            # Extract year from data_folder (e.g., "2026-01" -> 2026)
            # Handle both absolute paths and folder names
            from pathlib import Path

            folder_name = Path(self.data_folder).name
            try:
                year = (
                    int(folder_name[:4])
                    if folder_name and len(folder_name) >= 4 and folder_name[:4].isdigit()
                    else None
                )
            except (ValueError, TypeError):
                year = None

            file_io.convert_to_json(
                str(responses_csv), str(peeps_csv), str(self.output_json), year=year
            )

        self.logger.info(f"Loading data from {self.output_json}")

        peeps, events = file_io.load_data_from_json(str(self.output_json))
        self.partnership_requests = file_io.load_partnerships(
            str(self.period_path),
            partnerships_filename=self.partnerships_file,
            valid_peep_ids={peep.id for peep in peeps},
        )
        if self.partnership_requests:
            total_requests = sum(len(partners) for partners in self.partnership_requests.values())
            logging.info(f"Loaded {total_requests} partnership request(s)")

        date_string_to_event_id = {e.date.strftime("%Y-%m-%d %H:%M"): e.id for e in events}
        event_id_to_date_string = {e.id: e.date.strftime("%Y-%m-%d %H:%M") for e in events}

        cancellations_path = self.period_path / self.cancellations_file
        cancelled_event_ids, cancelled_availability = file_io.load_cancellations(
            str(cancellations_path), year=year
        )

        if cancelled_event_ids:
            loaded_event_ids = set(date_string_to_event_id.keys())
            unknown_cancelled = cancelled_event_ids - loaded_event_ids
            if unknown_cancelled:
                event_word = "event" if len(unknown_cancelled) == 1 else "events"
                raise ValueError(
                    f"cancelled {event_word} not found in loaded events: {sorted(unknown_cancelled)}"
                )

            original_count = len(events)
            events = [
                e for e in events if e.date.strftime("%Y-%m-%d %H:%M") not in cancelled_event_ids
            ]
            excluded_count = original_count - len(events)
            if excluded_count > 0:
                logging.info(f"Excluding {excluded_count} cancelled event(s) from scheduling")

        if cancelled_availability:
            peeps_by_email = {file_io.normalize_email(p.email): p for p in peeps}
            unknown_emails = set(cancelled_availability.keys()) - set(peeps_by_email.keys())
            if unknown_emails:
                raise ValueError(
                    f"cancelled availability email(s) not found in members: {sorted(unknown_emails)}"
                )

            loaded_event_ids = set(date_string_to_event_id.keys())
            unknown_events = set()
            unavailable_events = {}
            for email, event_ids in cancelled_availability.items():
                unknown_for_email = event_ids - loaded_event_ids
                if unknown_for_email:
                    unknown_events.update(unknown_for_email)

                peep = peeps_by_email[email]
                peep_event_ids = {
                    event_id_to_date_string[event_id]
                    for event_id in peep.availability
                    if event_id in event_id_to_date_string
                }
                missing = event_ids - peep_event_ids
                if missing:
                    unavailable_events[email] = sorted(missing)

            if unknown_events:
                raise ValueError(
                    f"cancelled availability event(s) not found in loaded events: {sorted(unknown_events)}"
                )
            if unavailable_events:
                raise ValueError(
                    f"cancelled availability includes events not in member availability: {unavailable_events}"
                )

            for email, event_ids in cancelled_availability.items():
                peep = peeps_by_email[email]
                cancelled_event_int_ids = {
                    date_string_to_event_id[event_id]
                    for event_id in event_ids
                    if event_id in date_string_to_event_id
                }
                if cancelled_event_int_ids:
                    peep.availability = [
                        event_id
                        for event_id in peep.availability
                        if event_id not in cancelled_event_int_ids
                    ]
        responders = [p for p in peeps if p.responded]
        no_availability = [p.name for p in responders if not p.availability]
        non_responders = [p.name for p in peeps if not p.responded and p.active]
        num_available = len([p for p in responders if p.availability])
        num_active = len([p for p in peeps if p.active])

        if self.interactive:
            print(
                f"\n📋 Mini Availability Report: {len(responders)} responses /  {num_available} available / {num_active} active / {len(peeps)} total"
            )
            print(
                "  🚫  No availability:",
                ", ".join(sorted(no_availability)) if no_availability else "None",
            )
            print(
                "  ❌  Did not respond:",
                ", ".join(sorted(non_responders)) if non_responders else "None",
            )
            print()

        self.logger.debug("Initial Peeps")
        self.logger.debug(Peep.peeps_str(peeps))

        # Get all events that can be filled to the minimum
        sanitized_events = self.sanitize_events(events, peeps)
        self.logger.debug(f"Sanitized Events: {len(sanitized_events)}/{len(events)}")

        # If too many events, remove some
        if len(sanitized_events) > self.max_events:
            logging.warning(
                f"Too many valid events. Trimming to {self.max_events} based on overlap."
            )
            sanitized_events = self.remove_high_overlap_events(
                sanitized_events, peeps, self.max_events
            )

        # Try events with different max per role to get the *actual* best sequence
        all_sequences = []
        for target_max in range(constants.ABS_MIN_ROLE, constants.ABS_MAX_ROLE + 1):
            self.target_max = target_max
            sequences = self.evaluate_all_event_sequences(peeps, sanitized_events)
            all_sequences.extend(sequences)

        best = self.get_top_sequences(all_sequences)
        if not best:
            self.logger.info("No sequence could fill any events.")
            return

        if len(best) == 1:
            best_sequence = best[0]
            self.logger.info(f"Auto-selected best sequence: {best_sequence}")
            file_io.save_event_sequence(best_sequence, str(self.result_json))
            self.logger.debug("Final Peeps:")
            self.logger.debug(Peep.peeps_str(best_sequence.peeps))
            return best_sequence
        else:
            if self.interactive:
                print(
                    f"Found {len(best)} tied top sequences with {best[0].num_unique_attendees} unique attendees:"
                )
                for i, seq in enumerate(best):
                    print(f"[{i}] {seq}")

            if self.interactive:
                choice = input(f"Enter the index of the sequence to save (0-{len(best) - 1}): ")
                try:
                    chosen_index = int(choice)
                    best_sequence = best[chosen_index]
                    self.logger.info(f"Selected {best_sequence}")
                    file_io.save_event_sequence(best_sequence, str(self.result_json))
                    self.logger.debug("Final Peeps:")
                    self.logger.debug(Peep.peeps_str(best_sequence.peeps))
                    return best_sequence
                except (ValueError, IndexError):
                    logging.error("Invalid choice. No sequence was saved.")
                    return None
            else:
                # In non-interactive mode, auto-select the specified sequence
                if self.sequence_choice < len(best):
                    best_sequence = best[self.sequence_choice]
                    self.logger.info(
                        f"Auto-selected tied sequence {self.sequence_choice}: {best_sequence}"
                    )
                else:
                    logging.warning(
                        f"Sequence choice {self.sequence_choice} out of range, selecting first"
                    )
                    best_sequence = best[0]
                    self.logger.info(f"Auto-selected first tied sequence: {best_sequence}")
                file_io.save_event_sequence(best_sequence, str(self.result_json))
                self.logger.debug("Final Peeps:")
                self.logger.debug(Peep.peeps_str(best_sequence.peeps))
                return best_sequence
