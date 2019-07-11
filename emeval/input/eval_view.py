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
        self.eval_view_map = {}

    def map(self):
        return self.eval_view_map

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
            self.eval_view_map[phoneOS] = {}
            for phone_label, phone_data_map in phone_map.items(): # ucb-sdb-android-1
                for r in phone_data_map["calibration_ranges"]:
                    if match_trip_id_pattern not in r["trip_id"]:
                        print("trip %s does not match pattern %s, skipping" %
                            (r["trip_id"], match_trip_id_pattern))
                        continue

                    trip_type = "_".join(r["trip_id"].split("_")[:-1])
                    # print(trip_type)
                    if trip_type not in self.eval_view_map[phoneOS]:
                        self.eval_view_map[phoneOS][trip_type] = {}
                    if r["trip_id"] not in self.eval_view_map[phoneOS][trip_type]:
                        self.eval_view_map[phoneOS][trip_type][r["trip_id"]] = {}
                    self.eval_view_map[phoneOS][trip_type][r["trip_id"]][phone_label] = r

    def from_view_single_run(self, phone_view, match_trip_id_pattern):
        for phoneOS, phone_map in phone_view.map().items(): # android, ios
            print("Processing data for %s phones" % phoneOS)
            self.eval_view_map[phoneOS] = {}
            for phone_label, phone_data_map in phone_map.items(): # ucb-sdb-android-1
                for r in phone_data_map["calibration_ranges"]:
                    if match_trip_id_pattern not in r["trip_id"]:
                        print("trip %s does not match pattern %s, skipping" %
                            (r["trip_id"], match_trip_id_pattern))
                        continue

                    if r["trip_id"] not in self.eval_view_map[phoneOS]:
                        self.eval_view_map[phoneOS][r["trip_id"]] = {}
                    self.eval_view_map[phoneOS][r["trip_id"]][phone_label] = r
        return self.eval_view_map
