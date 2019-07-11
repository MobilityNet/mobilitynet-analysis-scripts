# Fill in the phone view of the calibration data
# More details in __init__.py
# Resulting data structure
# - android
#   - ucb-sdb-android-1
#       - transitions
#       - calibration_transitions
#       - calibration_ranges
#           - high_accuracy_stationary_0
#               - battery_df
#               - location_df
#               - ....
#           - high_accuracy_stationary_1
#               - battery_df
#               - location_df
#           - ....
#           - high_accuracy_train_AO
#               - ....
#           - medium_accuracy_train_AO
#               - battery_df
#               - location_df
#   - ucb-sdb.android-2
#   -...
# - ios
#   - ucb-sdb-ios-1
#   - ucb-sdb-ios-2
#   - ...
#


import copy
import arrow
import pandas as pd
import emeval.validate.phone_view as evpv

class PhoneView:
    def __init__(self, spec_details):
        self.phone_view_map = {}
        self.spec_details = spec_details

        print(20 * "-", "About to read calibration transitions from server", 20 * "-")
        self.fill_transitions()
        print(20 * "-", "About to fill calibration ranges", 20 * "-")
        self.fill_calibration_ranges()
        print(20 * "-", "About to fill in battery information", 20 * "-")
        self.fill_battery_df()
        print(20 * "-", "About to fill in location information", 20 * "-")
        self.fill_location_df()
        print(20 * "-", "Done populating calibration information", 20 * "-")

    def validate(self):
        print(20 * "-", "About to validate config settings", 20 * "-")
        evpv.validate_config_settings(self)
        print(20 * "-", "About to validate range durations", 20 * "-")
        evpv.validate_range_durations(self)

    def map(self):
        return self.phone_view_map

    #
    # BEGIN: Imported from Validate_calibration_*.ipynb
    #
    def fill_transitions(self):
        self.phone_view_map = copy.deepcopy(self.spec_details.phone_labels)
        for phoneOS, phone_map in self.phone_view_map.items():
            print("Reading data for %s phones" % phoneOS)
            for phone_label in phone_map:
                print("Loading transitions for phone %s" % phone_label)
                curr_phone_transitions = self.spec_details.retrieve_data_from_server(
                    phone_label, ["manual/evaluation_transition"],
                    self.spec_details.eval_start_ts, self.spec_details.eval_end_ts)
                curr_phone_role = phone_map[phone_label]
                phone_map[phone_label] = {"role": curr_phone_role}
                phone_map[phone_label]["transitions"] = curr_phone_transitions

    """
    Inputs:
    - phone_view: the phone view with retrieved transitions
    - start_tt, end_tt: string representation of the start and end transitions
        (retrieved in case the pipeline had not yet run)
    - start_ti, end_ti: int representation of the start and end transitions
        (retrieved in case the pipeline has run)
    - storage_key: the prefix for the storage (e.g. calibration -> calibration_transitions)
    """
    def filter_transitions(self, start_tt, end_tt, start_ti, end_ti,
        storage_key_prefix):
        for phoneOS, phone_map in self.phone_view_map.items():
            print("Processing data for %s phones" % phoneOS)
            for phone_label in phone_map:
                print("Processing transitions for phone %s" % phone_label)
                curr_phone_transitions = [t["data"] for t in phone_map[phone_label]["transitions"]]
                # print(curr_phone_transitions)
                curr_calibration_transitions = [t for t in curr_phone_transitions
                    if (t["transition"] in [start_tt, end_tt, start_ti, end_ti]) and
                    t["spec_id"] == self.spec_details.CURR_SPEC_ID]
                print("Filtered %d total -> %d calibration transitions " %
                    (len(curr_phone_transitions), len(curr_calibration_transitions)))
                phone_map[phone_label]["{}_transitions".format(storage_key_prefix)] = \
                    sorted(curr_calibration_transitions, key=lambda t: t["ts"])

    """
    Convert list of transitions into a list of ranges.
    Inputs are the same as `filter_transitions`: 
    """
    @staticmethod
    # We expect that transitions occur in pairs
    def transitions_to_ranges(transition_list, start_tt, end_tt, start_ti, end_ti,
            transition_entry_template, spec_end_ts):
        start_transitions = transition_list[::2]
        end_transitions = transition_list[1::2]
        if len(transition_list) % 2 == 0:
            print("All ranges are complete, nothing to change")
        else:
            print("Incomplete range, adding fake end transition")
            last_start_transition = transition_entry_template
            fake_end_transition = copy.copy(last_start_transition)
            fake_end_transition["data"]["transition"] = end_tt
            curr_ts = arrow.get().timestamp
            if curr_ts > spec_end_ts:
                fake_end_transition["data"]["ts"] = spec_end_ts
            else:
                fake_end_transition["data"]["ts"] = curr_ts
            if "fmt_time" in last_start_transition["data"]:
                fake_end_transition["data"]["fmt_time"] = arrow.get(curr_ts).to(eval_tz)
            fake_end_transition["metadata"]["write_ts"] = curr_ts
            if "write_fmt_time" in last_start_transition["metadata"]:
                fake_end_transition["metadata"]["write_fmt_time"] = arrow.get(curr_ts).to(eval_tz)
            fake_end_transition["metadata"]["platform"] = "fake"
            if "local_dt" in fake_end_transition["data"]:
                del fake_end_transition["data"]["local_dt"]

        range_list = []
        for (s, e) in zip(start_transitions, end_transitions):
            # print("------------------------------------- \n %s -> \n %s" % (s, e))
            assert s["transition"] == start_tt or s["transition"] == start_ti, "Start transition has %s transition" % s["transition"]
            assert e["transition"] == end_tt or s["transition"] == end_ti, "Stop transition has %s transition" % s["transition"]
            assert s["trip_id"] == e["trip_id"], "trip_id mismatch! %s != %s" % (s["trip_id"], e["trip_id"])
            assert e["ts"] > s["ts"], "end %s is before start %s" % (arrow.get(e["ts"]), arrow.get(s["ts"]))
            for f in ["spec_id", "device_manufacturer", "device_model", "device_version"]:
                assert s[f] == e[f], "Field %s mismatch! %s != %s" % (f, s[f], e[f])
            curr_range = {"trip_id": s["trip_id"], "start_ts": s["ts"], "end_ts": e["ts"], "duration": (e["ts"] - s["ts"])}
            range_list.append(curr_range)
            
        return range_list

    def fill_calibration_ranges(self):
        self.filter_transitions(
            "START_CALIBRATION_PERIOD", "STOP_CALIBRATION_PERIOD", 0, 1,
            "calibration")
        for phoneOS, phone_map in self.phone_view_map.items():
            print("Processing data for %s phones" % phoneOS)
            for phone_label in phone_map:
                curr_calibration_ranges = PhoneView.transitions_to_ranges(
                    phone_map[phone_label]["calibration_transitions"],
                    "START_CALIBRATION_PERIOD", "STOP_CALIBRATION_PERIOD", 0, 1,
                    phone_map[phone_label]["transitions"][-1], self.spec_details.eval_end_ts)
                print("Found %d ranges for phone %s" % (len(curr_calibration_ranges), phone_label))
                phone_map[phone_label]["calibration_ranges"] = curr_calibration_ranges

    def fill_battery_df(self):
        for phoneOS, phone_map in self.phone_view_map.items():
            print("Processing data for %s phones" % phoneOS)
            for phone_label in phone_map:
                curr_calibration_ranges = phone_map[phone_label]["calibration_ranges"]
                for r in curr_calibration_ranges:
                    battery_entries = self.spec_details.retrieve_data_from_server(phone_label, ["background/battery"], r["start_ts"], r["end_ts"])
                    # ios entries before running the pipeline are marked with battery_level_ratio, which is a float from 0 ->1
                    # convert it to % to be consistent with android and easier to understand
                    if phoneOS == "ios":
                        for e in battery_entries:
                            if "battery_level_pct" not in e["data"]:
                                e["data"]["battery_level_pct"] = e["data"]["battery_level_ratio"] * 100
                                del e["data"]["battery_level_ratio"]
                    battery_df = pd.DataFrame([e["data"] for e in battery_entries])
                    battery_df["hr"] = (battery_df.ts-r["start_ts"])/3600.0
                    r["battery_df"] = battery_df


    def fill_location_df(self):
        for phoneOS, phone_map in self.phone_view_map.items():
            print("Processing data for %s phones" % phoneOS)
            for phone_label in phone_map:
                curr_calibration_ranges = phone_map[phone_label]["calibration_ranges"]
                for r in curr_calibration_ranges:
                    all_done = False
                    location_entries = []
                    curr_start_ts = r["start_ts"]
                    prev_retrieved_count = 0

                    while not all_done:
                        print("About to retrieve data for %s from %s -> %s" % (phone_label, curr_start_ts, r["end_ts"]))
                        curr_location_entries = self.spec_details.retrieve_data_from_server(phone_label, ["background/location"], curr_start_ts, r["end_ts"])
                        print("Retrieved %d entries with timestamps %s..." % (len(curr_location_entries), [cle["data"]["ts"] for cle in curr_location_entries[0:10]]))
                        if len(curr_location_entries) == 0 or len(curr_location_entries) == 1 or len(curr_location_entries) == prev_retrieved_count:
                            all_done = True
                        else:
                            location_entries.extend(curr_location_entries)
                            curr_start_ts = curr_location_entries[-1]["data"]["ts"]
                            prev_retrieved_count = len(curr_location_entries)
                    location_df = pd.DataFrame([e["data"] for e in location_entries])
                    location_df["hr"] = (location_df.ts-r["start_ts"])/3600.0
                    r["location_df"] = location_df
    #
    # END: Imported from Validate_calibration_*.ipynb
    #
