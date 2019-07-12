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

        print(20 * "-", "About to read transitions from server", 20 * "-")
        self.fill_transitions()
        print(20 * "-", "About to fill calibration ranges", 20 * "-")
        self.fill_calibration_ranges()
        print(20 * "-", "About to fill evaluation ranges", 20 * "-")
        self.fill_evaluation_ranges()
        print(20 * "-", "About to fill evaluation trips", 20 * "-")
        self.fill_eval_role_maps()
        self.fill_accuracy_control_trip_ranges()
        self.copy_trip_ranges_to_eval_power_maps()
        print(20 * "-", "About to fill in battery information", 20 * "-")
        self.fill_battery_df("calibration")
        self.fill_battery_df("evaluation")
        print(20 * "-", "About to fill in location information", 20 * "-")
        self.fill_location_df("calibration")
        self.fill_location_df("evaluation")
        print(20 * "-", "About to select trip specific ranges", 20 * "-")
        self.fill_trip_specific_battery_and_locations()
        print(20 * "-", "Done populating information from server", 20 * "-")

    def validate(self):
        print(20 * "-", "About to validate calibration settings", 20 * "-")
        evpv.validate_calibration_settings(self)
        print(20 * "-", "About to validate evaluation settings", 20 * "-")
        evpv.validate_evaluation_settings(self)
        print(20 * "-", "About to validate calibration range durations", 20 * "-")
        evpv.validate_range_durations_for_calibration(self)
        print(20 * "-", "About to validate evaluation range durations", 20 * "-")
        evpv.validate_range_durations_for_evaluation(self)

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

    ## Link evaluation ranges to each other
    # Since the evaluation ranges have separate IDs, it is hard to link them
    # the way we link the calibration ranges.  So let's add a common field
    # (`eval_common_trip_id`) to each of the matching
    # ranges for easy checking

    def link_common_eval_ranges(self):
        for phoneOS, phone_map in self.phone_view_map.items(): # android, ios
            print("Processing data for %s phones" % phoneOS)
            all_eval_ranges = [m["evaluation_ranges"] for m in phone_map.values()]
            # all the lengths are equal - i.e. the set of lengths has one entr
            assert len(set([len(a) for a in all_eval_ranges])) == 1
            for ctuple in zip(*all_eval_ranges):
                eval_cols = ctuple[1:-1]
                # print([ct["trip_id"] for ct in ctuple], [ct["trip_id"] for ct in eval_cols])
                get_common_name = lambda r: r["trip_id"].split(":")[0]
                get_separate_role = lambda r: r["trip_id"].split(":")[1]
                common_names = set([get_common_name(r) for r in eval_cols])
                separate_roles = [get_separate_role(r) for r in eval_cols]
                # print(separate_roles)
                assert len(common_names) == 1
                common_name = list(common_names)[0]
                # print(common_name)
                for r in ctuple:
                    r["eval_common_trip_id"] = common_name
                ctuple[0]["eval_role"] = "accuracy_control"
                for r, sr in zip(eval_cols, separate_roles):
                    r["eval_role"] = sr
                ctuple[-1]["eval_role"] = "power_control"

    def fill_evaluation_ranges(self):
        self.filter_transitions(
            "START_EVALUATION_PERIOD", "STOP_EVALUATION_PERIOD", 2, 3,
            "evaluation")
        for phoneOS, phone_map in self.phone_view_map.items():
            print("Processing data for %s phones" % phoneOS)
            for phone_label in phone_map:
                curr_evaluation_ranges = PhoneView.transitions_to_ranges(
                    phone_map[phone_label]["evaluation_transitions"],
                    "START_EVALUATION_PERIOD", "STOP_EVALUATION_PERIOD", 2, 3,
                    phone_map[phone_label]["transitions"][-1], self.spec_details.eval_end_ts)
                print("Found %d ranges for phone %s" % (len(curr_evaluation_ranges), phone_label))
                phone_map[phone_label]["evaluation_ranges"] = curr_evaluation_ranges
        self.link_common_eval_ranges()

    def fill_battery_df(self, storage_key):
        for phoneOS, phone_map in self.phone_view_map.items():
            print("Processing data for %s phones" % phoneOS)
            for phone_label in phone_map:
                curr_calibration_ranges = phone_map[phone_label]["{}_ranges".format(storage_key)]
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


    def fill_location_df(self, storage_key):
        for phoneOS, phone_map in self.phone_view_map.items():
            print("Processing data for %s phones" % phoneOS)
            for phone_label in phone_map:
                curr_calibration_ranges = phone_map[phone_label]["{}_ranges".format(storage_key)]
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

    def fill_eval_role_maps(self):
        self.accuracy_control_maps = {}
        self.power_control_maps = {}
        self.eval_phone_maps = {}

        for phoneOS, phone_map in self.phone_view_map.items():
            print("Processing data for %s phones" % phoneOS)
            self.eval_phone_maps[phoneOS] = {}
            for phone_label in phone_map:
                curr_role = phone_map[phone_label]["role"]
                if curr_role == "accuracy_control":
                    self.accuracy_control_maps[phoneOS] = phone_map[phone_label]
                elif curr_role == "power_control":
                    self.power_control_maps[phoneOS] = phone_map[phone_label]
                else:
                    assert curr_role.startswith("evaluation")
                    self.eval_phone_maps[phoneOS][phone_label] = phone_map[phone_label]

        print("Lengths (accuracy, power, eval) = (%d, %d, %d)" %
            (len(self.accuracy_control_maps),
            len(self.power_control_maps),
            len(self.eval_phone_maps)))
        print("keys (accuracy, power, eval) = (%s, %s, %s)" %
            (self.accuracy_control_maps.keys(),
            self.power_control_maps.keys(),
            self.eval_phone_maps.keys()))
        print("eval_phone_keys (android, ios) = (%s, %s)" %
            (self.eval_phone_maps["android"].keys(), self.eval_phone_maps["ios"].keys()))

    def fill_accuracy_control_trip_ranges(self):
        for phoneOS, phone_map in self.accuracy_control_maps.items():
            curr_control_transitions = [t["data"] for t in phone_map["transitions"]] # from control phone
            curr_evaluation_ranges = phone_map["evaluation_ranges"] # from this phone
            trip_type_check = lambda t: t["transition"] in ["START_EVALUATION_TRIP", "STOP_EVALUATION_TRIP", 4, 5]
            trip_time_check = lambda t, r: t["ts"] >= r["start_ts"] and t["ts"] <= r["end_ts"]
            for i, r in enumerate(curr_evaluation_ranges):
                # We have to get the evaluation details from one of the evaluation phones
                curr_eval_trips_transitions = [t for t in curr_control_transitions if trip_type_check(t) and trip_time_check(t, r)]
                # print("\n".join([str((t["transition"], t["trip_id"], t["ts"], arrow.get(t["ts"]).to(eval_tz))) for t in curr_eval_trips_transitions]))
                # print(len(curr_eval_trips_transitions))
                curr_eval_trips_ranges = PhoneView.transitions_to_ranges(
                    curr_eval_trips_transitions,
                    "START_EVALUATION_TRIP", "STOP_EVALUATION_TRIP", 4, 5,
                    curr_control_transitions[-1], self.spec_details.eval_end_ts)
                print("%s: Found %s trips for evaluation %s" % (phoneOS, len(curr_eval_trips_ranges), r["trip_id"]))
                # print(curr_eval_trips_ranges)
                print("\n".join([str((tr["trip_id"], tr["duration"],
                    arrow.get(tr["start_ts"]).to(self.spec_details.eval_tz),
                    arrow.get(tr["end_ts"]).to(self.spec_details.eval_tz)))
                    for tr in curr_eval_trips_ranges]))
                r["evaluation_trip_ranges"] = curr_eval_trips_ranges

    def copy_trip_ranges_to_eval_power_maps(self):
        for phoneOS in self.accuracy_control_maps.keys():
            matching_accuracy_control_map = self.accuracy_control_maps[phoneOS]
            matching_eval_power_maps = self.phone_view_map[phoneOS]
            print(matching_eval_power_maps.keys())
            for phone_label, phone_map in matching_eval_power_maps.items():
                if phone_map == matching_accuracy_control_map:
                    print("Found accuracy control, skipping copy")
                    continue
                curr_eval_evaluation_ranges = phone_map["evaluation_ranges"]
                curr_accuracy_evaluation_ranges = matching_accuracy_control_map["evaluation_ranges"]
                assert len(curr_eval_evaluation_ranges) == len(curr_accuracy_evaluation_ranges)
                for i, (re, ra) in enumerate(zip(curr_eval_evaluation_ranges, curr_accuracy_evaluation_ranges)):
                    accuracy_eval_trips_ranges = ra["evaluation_trip_ranges"] # from this phone
                    print("%s: Copying %s accuracy trips to %s, before = %s" % (phoneOS, phone_label, len(accuracy_eval_trips_ranges),
                        len(re["evaluation_trip_ranges"]) if "evaluation_trip_ranges" in re else 0))
                    re["evaluation_trip_ranges"] = copy.deepcopy(accuracy_eval_trips_ranges)
                    # print(curr_eval_trips_ranges)
                    print("\n".join([str((tr["trip_id"], tr["duration"],
                        arrow.get(tr["start_ts"]).to(self.spec_details.eval_tz),
                        arrow.get(tr["end_ts"]).to(self.spec_details.eval_tz)))
                        for tr in re["evaluation_trip_ranges"]]))

    def fill_trip_specific_battery_and_locations(self):
        for phoneOS, phone_map in self.phone_view_map.items(): # android, ios
            for phone_label in phone_map:
                print("Filling label %s for OS %s" % (phone_label, phoneOS))
                for r in phone_map[phone_label]["evaluation_ranges"]:
                    # print(r["battery_df"].head())
                    for tr in r["evaluation_trip_ranges"]:
                        query = "ts > %s & ts <= %s" % (tr["start_ts"], tr["end_ts"])
                        # print("%s %s %s" % (phone_label, tr["trip_id"], query))
                        # print(r["battery_df"].query(query).head())
                        tr["battery_df"] = r["battery_df"].query(query)
                        # print(80 * '~')
                        # print(tr["battery_df"])
                        tr["location_df"] = r["location_df"].query(query)
                        # print(80 * "-")

    #
    # END: Imported from Validate_calibration_*.ipynb
    #
