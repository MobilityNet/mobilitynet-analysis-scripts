# Fill in the evaluation view of the calibration data
# More details in __init__.py
# Resulting data structure
# - android
#   - high_accuracy_stationary
#       - high_accuracy_stationary_0
#           - ucb-sdb-android-1
#               - battery_df
#               - location_df
#               - ....
#           - ucb-sdb-android-2
#               - battery_df
#               - location_df
#               - ....
#       - high_accuracy_stationary_1
#           - ucb-sdb-android-1
#               - battery_df
#               - location_df
#               - ....
#           - ucb-sdb-android-2
#               - battery_df
#               - location_df
#               - ....
#   - medium_accuracy_stationary
#       - medium_accuracy_stationary_0
#           - ucb-sdb-android-1
#               - battery_df
#               - location_df
#               - ....
#           - ucb-sdb-android-2
#               - battery_df
#               - location_df
#               - ....
#       - medium_accuracy_stationary_1
#           - ucb-sdb-android-1
#               - battery_df
#               - location_df
#               - ....
#           - ucb-sdb-android-2
#               - battery_df
#               - location_df
#               - ....
#       - high_accuracy_train_AO
#               - ....
#       - medium_accuracy_train_AO
#               - battery_df
#               - location_df
# - ios
#   - high_accuracy_stationary
#   - medium_accuracy_stationary
#   - ucb-sdb-ios-1
#   - ucb-sdb-ios-2
#   - ...
#
# More details in __init__.py

import copy
import arrow
import pandas as pd

#
# Inputs
#

class EvaluationView:
    def __init__(self):
        self.calib_eval_view_map = {}
        self.eval_eval_view_map = {}

    """
    eval_type is "calibration" or "evaluation"
    """
    def map(self, eval_type):
        if eval_type == "calibration":
            return self.calib_eval_view_map
        else:
            assert eval_type == "evaluation"
            return self.eval_eval_view_map

    """
    phone_view: the view to read data from
    match_trip_id_pattern: e.g.
    - "stationary" for the stationary trips, which we have multiple runs for, or
    - "AO" for the always on moving calibrations, which we have only single runs for
    - "" for all trips
    """
    def from_view_multiple_runs(self, phone_view, match_trip_id_pattern):
        for phoneOS, phone_map in phone_view.map().items(): # android, ios
            print("Processing data for %s phones" % phoneOS)
            self.calib_eval_view_map[phoneOS] = {}
            for phone_label, phone_data_map in phone_map.items(): # ucb-sdb-android-1
                for r in phone_data_map["calibration_ranges"]:
                    if match_trip_id_pattern not in r["trip_id"]:
                        print("trip %s does not match pattern %s, skipping" %
                            (r["trip_id"], match_trip_id_pattern))
                        continue

                    trip_type = "_".join(r["trip_id"].split("_")[:-1])
                    # print(trip_type)
                    if trip_type not in self.calib_eval_view_map[phoneOS]:
                        self.calib_eval_view_map[phoneOS][trip_type] = {}
                    if r["trip_id"] not in self.calib_eval_view_map[phoneOS][trip_type]:
                        self.calib_eval_view_map[phoneOS][trip_type][r["trip_id"]] = {}
                    self.calib_eval_view_map[phoneOS][trip_type][r["trip_id"]][phone_label] = r

    def from_view_single_run(self, phone_view, match_trip_id_pattern):
        for phoneOS, phone_map in phone_view.map().items(): # android, ios
            print("Processing data for %s phones" % phoneOS)
            self.calib_eval_view_map[phoneOS] = {}
            for phone_label, phone_data_map in phone_map.items(): # ucb-sdb-android-1
                for r in phone_data_map["calibration_ranges"]:
                    if match_trip_id_pattern not in r["trip_id"]:
                        print("trip %s does not match pattern %s, skipping" %
                            (r["trip_id"], match_trip_id_pattern))
                        continue

                    if r["trip_id"] not in self.calib_eval_view_map[phoneOS]:
                        self.calib_eval_view_map[phoneOS][r["trip_id"]] = {}
                    self.calib_eval_view_map[phoneOS][r["trip_id"]][phone_label] = r
        return self.calib_eval_view_map

    def from_view_eval_trips(self, phone_view, match_eval_range_pattern, match_trip_id_pattern):
        for phoneOS, phone_map in phone_view.map().items(): # android, ios
            print("Processing data for %s phones" % phoneOS)
            self.eval_eval_view_map[phoneOS] = {}
            all_eval_ranges = [m["evaluation_ranges"] for m in phone_map.values()]
            # all the lengths are equal - i.e. the set of lengths has one entr
            assert len(set([len(a) for a in all_eval_ranges])) == 1
            for ctuple in zip(*all_eval_ranges):
                common_names = set([r["eval_common_trip_id"] for r in ctuple])
                assert len(common_names) == 1
                common_name = list(common_names)[0]
                # print(common_name)
                self.eval_eval_view_map[phoneOS][common_name] = {}

                separate_roles = [r["eval_role"] for r in ctuple]
                print(separate_roles)

                eval_trips_for_range = [r["evaluation_trip_ranges"] for r in ctuple]
                # print([len(et) for et in eval_trips_for_range])
                assert len(set([len(et) for et in eval_trips_for_range])) == 1
                for ctriptuple in zip(*(eval_trips_for_range)):
                    # print([ctt["trip_id"] for ctt in ctriptuple])
                    common_trip_ids = set([ctt["trip_id"] for ctt in ctriptuple])
                    assert(len(common_trip_ids)) == 1
                    common_trip_id = list(common_trip_ids)[0]
                    print(common_trip_id)
                    self.eval_eval_view_map[phoneOS][common_name][common_trip_id] = {}
                    for cr, ctt in zip(separate_roles, ctriptuple):
                        self.eval_eval_view_map[phoneOS][common_name][common_trip_id][cr] = ctt
        print("android keys = %s" % self.eval_eval_view_map["android"].keys())
        print("android first range keys = %s" %
            self.eval_eval_view_map["android"]['HAHFDC v/s HAMFDC'].keys())
        print("android first trip keys = %s" %
            self.eval_eval_view_map["android"]['HAHFDC v/s HAMFDC']['short_walk_suburb'].keys())
        print("android first trip first eval keys = %s" %
            self.eval_eval_view_map["android"]['HAHFDC v/s HAMFDC']['short_walk_suburb']['HAHFDC'].keys())
