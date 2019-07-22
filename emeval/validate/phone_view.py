import arrow
import pandas as pd

MAX_DURATION_VARIATION = 5 * 60 # seconds

def get_expected_config_map_for_calibration(sd):
    expected_config_map = {}
    for ct in sd.curr_spec["calibration_tests"]:
        expected_config_map[ct["id"]] = ct["config"]["sensing_config"]
    return expected_config_map

def get_expected_config_map_for_evaluation(sd):
    expected_config_map = {
        "android": {
            "fixed:ACCURACY_CONTROL": {
                "is_duty_cycling": False,
                "accuracy": ["PRIORITY_HIGH_ACCURACY","kCLLocationAccuracyBest"],
                "filter": 1,
            },
            "fixed:POWER_CONTROL": {
                "is_duty_cycling": False,
                "accuracy": ["PRIORITY_NO_POWER","kCLLocationAccuracyThreeKilometers"],
                "filter": 1200,
            }
        }
    }
    # The control settings were the same for both OSes, since we do put both
    # control values into an array.
    expected_config_map["ios"] = expected_config_map["android"]
    for ct in sd.curr_spec["sensing_settings"]:
        for phoneOS, phone_map in ct.items():
            for s in phone_map["sensing_configs"]:
                expected_config_map[phoneOS]["%s:%s" % (phone_map["name"], s["id"])] = s["sensing_config"]
    # print(expected_config_map)
    return expected_config_map

# Current accuracy constants
# Since we can't read these from the phone, we hardcoded them from the documentation
# If there are validation failures, these need to be updated
# In the future, we could upload the options from the phone (maybe the accuracy control)
# but that seems like overkill here

accuracy_options = {
    "android": {
        "PRIORITY_HIGH_ACCURACY": 100,
        "PRIORITY_BALANCED_POWER_ACCURACY": 102,
        "PRIORITY_LOW_POWER": 104,
        "PRIORITY_NO_POWER": 105
    },
    "ios": {
        "kCLLocationAccuracyBestForNavigation": -2,
        "kCLLocationAccuracyBest": -1,
        "kCLLocationAccuracyNearestTenMeters": 10,
        "kCLLocationAccuracyHundredMeters": 100,
        "kCLLocationAccuracyKilometer": 1000,
        "kCLLocationAccuracyThreeKilometers": 3000,
    }
}

opt_array_idx = lambda phoneOS: 0 if phoneOS == "android" else 1

"""
Internal method to validate the filter settings
"""
def _validate_filter(phoneOS, config_during_test, expected_config):
    # filter checking is a bit tricky because the expected value has two possible values and the real config has two possible values
    expected_filter = expected_config["filter"]
    if type(expected_filter) == int:
        ev = expected_filter
    else:
        assert type(expected_filter) == list, "platform specific filters should be specified in array, not %s" % expected_filter
        ev = expected_filter[opt_array_idx(phoneOS)]
        
    if phoneOS == "android":
        cvf = "filter_time"
        ev = ev * 1000 # milliseconds in the config, specified in secs
    elif phoneOS == "ios":
        cvf = "filter_distance"
        
    assert config_during_test[cvf] == ev,\
            "Field filter mismatch! %s (from %s) != %s (from %s)" %\
            (config_during_test[cvf], config_during_test, ev, expected_config)
    
"""
Internal method to validate the filter settings
"""
def _validate_accuracy(phoneOS, config_during_test, expected_config):
    # expected config accuracy is an array of strings ["PRIORITY_BALANCED_POWER_ACCURACY", "kCLLocationAccuracyNearestTenMeters"]
    # so we find the string at the correct index and then map it to the value from the options
    ev = accuracy_options[phoneOS][expected_config["accuracy"][opt_array_idx(phoneOS)]]
    assert config_during_test["accuracy"] == ev, "Field accuracy mismatch! %s != %s" % (config_during_test[accuracy], ev)


def validate_calibration_settings(phone_view):
    expected_config_map = get_expected_config_map_for_calibration(phone_view.spec_details)
    # print(expected_config_map)
    for phoneOS, phone_map in phone_view.map().items():
        print("Processing data for %s phones" % phoneOS)
        for phone_label in phone_map:
            curr_calibration_ranges = phone_map[phone_label]["calibration_ranges"]
            all_test_ids = [r["trip_id"] for r in curr_calibration_ranges]
            unique_test_ids = sorted(list(set(all_test_ids)))
            spec_test_ids = sorted([ct["id"] for ct in
                phone_view.spec_details.curr_spec["calibration_tests"]])
            # assert unique_test_ids == spec_test_ids, "Missing calibration test while comparing %s, %s" % (unique_test_ids, spec_test_ids)
            for r in curr_calibration_ranges:
                config_during_test_entries = phone_view.spec_details.retrieve_data_from_server(phone_label, ["config/sensor_config"], r["start_ts"], r["end_ts"])
                print("%s -> %s" % (r["trip_id"], [c["data"]["accuracy"] for c in config_during_test_entries]))
                # assert len(config_during_test_entries) == 1, "Out of band configuration? Found %d config changes" % len(config_during_test_entries)
                config_during_test = config_during_test_entries[0]["data"]
                expected_config = expected_config_map[r["trip_id"]]
                # print(config_during_test, expected_config)
                _validate_filter(phoneOS, config_during_test, expected_config)
                _validate_accuracy(phoneOS, config_during_test, expected_config)
                for f in expected_config:
                    if f != "accuracy" and f != "filter":
                        assert config_during_test[f] == expected_config[f],\
                            "Field %s mismatch! %s != %s" % \
                                (f, config_during_test[f], expected_config[f])

def validate_evaluation_settings(phone_view):
    expected_config_map = get_expected_config_map_for_evaluation(phone_view.spec_details)
    for phoneOS, phone_map in phone_view.map().items():
        # print("Processing data for %s phones, next level keys = %s" % (phoneOS, phone_map.keys()))
        for phone_label in phone_map:
            curr_evaluation_ranges = phone_map[phone_label]["evaluation_ranges"]
            unique_test_ids = set(filter(lambda id: id != "fixed", [r["trip_id"].split(":")[0] for r in curr_evaluation_ranges]))
            # This is a tricky check since, unlike in the calibration case, we will have different ids for the different evaluation
            # ranges. For now, let us just assert that the evaluation range is valid
            # print("Unique test ids = %s" % unique_test_ids)
            spec_test_ids = set(
                [ct["android"]["name"] for ct in phone_view.spec_details.curr_spec["sensing_settings"]] +
                [ct["ios"]["name"] for ct in phone_view.spec_details.curr_spec["sensing_settings"]])
            # print(spec_test_ids)
            # <= represents subset for set objects
            assert unique_test_ids <= spec_test_ids, "Invalid evaluation test while comparing %s, %s" % (unique_test_ids, spec_test_ids)
            for r in curr_evaluation_ranges:
                config_during_test_entries = phone_view.spec_details.retrieve_data_from_server(phone_label, ["config/sensor_config"], r["start_ts"], r["end_ts"])
                print("%s -> %s" % (r["trip_id"], [c["data"]["accuracy"] for c in config_during_test_entries]))
                # assert len(config_during_test_entries) == 1, "Out of band configuration? Found %d config changes" % len(config_during_test_entries)
                config_during_test = config_during_test_entries[0]["data"]
                expected_config = expected_config_map[phoneOS][r["trip_id"]]
                # print(config_during_test.keys(), expected_config.keys())
                _validate_filter(phoneOS, config_during_test, expected_config)
                _validate_accuracy(phoneOS, config_during_test, expected_config)
                for f in expected_config:
                    if f != "accuracy" and f != "filter":
                        assert config_during_test[f] == expected_config[f], "Field %s mismatch! %s != %s" % (f, config_during_test[f], expected_config[f])

"""
range_key: the key in each range that links the ranges for an experiment together
"trip_id" for calibration and "eval_common_trip_id" for evaluation
"""

def validate_range_durations(phone_view, range_key, range_entry_id):
    duration_map = {}
    for phoneOS, phone_map in phone_view.map().items():
        print("Processing data for %s phones" % phoneOS)
        for phone_label in phone_map:
            curr_phone_duration_map = {}
            curr_calibration_ranges = phone_map[phone_label]["{}_ranges".format(range_key)]
            for r in curr_calibration_ranges:
                curr_phone_duration_map[r[range_entry_id]] = r["duration"]
            duration_map[phoneOS+"_"+phone_label] = curr_phone_duration_map

    # print(duration_map)
    duration_df = pd.DataFrame(duration_map).transpose()
    print(duration_df)
    for col in duration_df:
        duration_variation = duration_df[col] - duration_df[col].median()
        print("For %s, duration_variation = %s" % (col, duration_variation.tolist()))
        assert duration_variation.abs().max() < MAX_DURATION_VARIATION,\
            "INVALID: for %s, duration_variation.abs().max() %d > threshold %d" % \
                (col, duration_variation.abs().max(), MAX_DURATION_VARIATION)

def validate_range_durations_for_calibration(phone_view):
    validate_range_durations(phone_view, "calibration", "trip_id")

# This is slightly different from the calibration validation because the
# matching durations don't have the same IDs
def validate_range_durations_for_evaluation(phone_view):
    validate_range_durations(phone_view, "evaluation", "eval_common_trip_id")
