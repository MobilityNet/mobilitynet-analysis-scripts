import pandas as pd
import arrow
import numpy as np
import re

THIRTY_MINUTES = 30 * 60
TIME_THRESHOLD = THIRTY_MINUTES

def find_section_transitions(ma_df, transition_mask_fn):
    return ma_df[transition_mask_fn(ma_df)]

def find_section_ranges(transition_points, t2m_fn):
    """
    Incoming motion activity should be range or trip.  NOT section. The related
    ranges should be matched with the sections later as part of `find_ranges`.
    """
    start_transition = None
    range_list = []
    for t in transition_points.to_dict(orient='records'):
        if start_transition is None:
            start_transition = t
        else:
            range_list.append({"start_ts": start_transition["ts"], "end_ts": t["ts"], "mode": t2m_fn(start_transition)})
            start_transition = t
    return range_list

def find_ranges(transition_df, start_transition, end_transition):
    """
    Return ranges formed by alternating start and end transition pairs.
    Unexpected transitions are ignored
    So S, S, E, E, E, E -> 1
    S, E, S, E, S, S -> 2
    E, S, E, S -> 1
    """
    start_ts = None
    stre = re.compile(start_transition)
    etre = re.compile(end_transition)
    range_list = []
    for t in transition_df.to_dict(orient='records'):
        # print("Considering transition %s" % t)
        if start_ts is None and stre.match(t["transition"]) is not None:
            start_ts = t["ts"]
        elif start_ts is not None and etre.match(t["transition"]) is not None:
            range_list.append({"start_ts": start_ts, "end_ts": t["ts"]})
            start_ts = None
    # print("Returning %s" % range_list)
    return range_list

def find_closest_segment_idx(gt, sensed_segments, key):
    ts_diffs = [abs(gt[key] - st[key]) for st in sensed_segments]
    # import arrow
    # print("diffs for %s %s = %s" % (key, arrow.get(gt[key]).to("America/Los_Angeles"), ts_diffs))
    min_diff = min(ts_diffs)
    if min_diff > TIME_THRESHOLD:
        # too far out, maybe this gt_segment doesn't have any matching segment
        return None
    else:
        min_index = ts_diffs.index(min_diff)
        return min_index

def find_matching_segments(gt_segments, id_key, sensed_segments):
    """
    id_key represents the kind of segment that we are find matches for. e.g.
    - trip_id for trips
    - section_id for sections
    """
    matching_segments_map = {}
    
    if len(sensed_segments) == 0:
        print("Found no sensed segments, early return")
        for gt in gt_segments:
            matching_segments_map[gt[id_key]] = {"type": "none", "match": []}
        return matching_segments_map
    if len(gt_segments) == len(sensed_segments):
        print("Found matching lengths %d = %d" % (len(gt_segments), len(sensed_segments)))
        for i, (gt, st) in enumerate(zip(gt_segments, sensed_segments)):
            matching_segments_map[gt[id_key]] = {"type": "both", "match": [st]}
    else:
        print("Found mismatched lengths %d != %d, need to use more complex matching" %
            (len(gt_segments), len(sensed_segments)))
        for gt in gt_segments:
            start_segment_idx = find_closest_segment_idx(gt, sensed_segments, "start_ts")
            # We want to find the end segment id in the segments after the
            # start segment. So we filter the array passed in, and add back the
            # start segment idx. This is more complex than it seems due to None checks
            if start_segment_idx is None:
                end_segment_idx = find_closest_segment_idx(gt,
                    sensed_segments, "end_ts")
            else:
                end_segment_idx = find_closest_segment_idx(gt,
                    sensed_segments[start_segment_idx:], "end_ts")
                if end_segment_idx is not None:
                    end_segment_idx = end_segment_idx + start_segment_idx
            # print("for gt = %s, start_segment_idx = %s, end_segment_id = %s" %
            #     (gt["trip_id"], start_segment_idx, end_segment_idx))
            if start_segment_idx is not None and end_segment_idx is not None:
                # we found both start and end within a reasonable timeframe
                matching_segments_map[gt[id_key]] = {"type": "both",
                    "match": sensed_segments[start_segment_idx:end_segment_idx+1]}
            elif start_segment_idx is not None:
                # we find a segment that starts pretty close by but ends super
                # early, or super late let's pick it anyway
                assert end_segment_idx is None
                matching_segments_map[gt[id_key]] = {"type": "start_ts",
                    "match": [sensed_segments[start_segment_idx]]}
            elif end_segment_idx is not None:
                # we find a segment that ends pretty close by but starts super
                # early/late, let's pick it anyway
                assert start_segment_idx is None
                matching_segments_map[gt[id_key]] = {"type": "end_ts",
                    "match": [sensed_segments[end_segment_idx]]}
            else:
                # we find nothing that is close to either the start or the end;
                # no matching segments
                assert start_segment_idx is None and end_segment_idx is None
                matching_segments_map[gt[id_key]] = {"type": "none", "match": []}

    return matching_segments_map

def get_count_start_end_ts_diff(segment, sensed_segment_range):
    if len(sensed_segment_range["match"]) > 0:
        if sensed_segment_range["type"] == "both" or\
            sensed_segment_range["type"] == "start_ts":
            start_ts_diff = min(abs(segment["start_ts"] -
                    sensed_segment_range["match"][0]["start_ts"]),
                TIME_THRESHOLD)
        else:
            start_ts_diff = TIME_THRESHOLD
            
        if sensed_segment_range["type"] == "both" or\
            sensed_segment_range["type"] == "end_ts":
            end_ts_diff = min(abs(segment["end_ts"] -
                    sensed_segment_range["match"][-1]["end_ts"]),
                TIME_THRESHOLD)
        else:
            end_ts_diff = TIME_THRESHOLD
    else:
        start_ts_diff = TIME_THRESHOLD
        end_ts_diff = TIME_THRESHOLD

    return {"count": len(sensed_segment_range["match"]),
            "start_diff_mins": start_ts_diff / 60,
            "end_diff_mins": end_ts_diff / 60}

#####
# BEGIN: Trip-specific segmentation code
#####

def fill_sensed_trip_ranges(pv):
    for phone_os, phone_map in pv.map().items():
        print(15 * "=*")
        print(phone_os, phone_map.keys())
        for phone_label, phone_detail_map in phone_map.items():
            print(4 * ' ', 15 * "-*")
            print(4 * ' ', phone_label, phone_detail_map["role"], phone_detail_map.keys())
            if "control" in phone_detail_map["role"]:
                print("Ignoring %s phone %s since they are always on" % (phone_detail_map["role"], phone_label))
                continue
            # this spec does not have any calibration ranges, but evaluation ranges are actually cooler
            for r in phone_detail_map["evaluation_ranges"]:
                print(8 * ' ', 30 * "=")
                print(8 * ' ',r.keys())
                print(8 * ' ',r["trip_id"], r["eval_common_trip_id"], r["eval_role"], len(r["evaluation_trip_ranges"]))
                # print(r["transition_df"][["transition", "fmt_time"]])
                if phone_os == "android":
                    query_str = "transition == 'local.transition.exited_geofence' | transition == 'local.transition.stopped_moving'"
                else:
                    assert phone_os == "ios"
                    query_str = "transition == 'T_EXITED_GEOFENCE' | transition == 'T_VISIT_ENDED' | transition == 'T_VISIT_STARTED' | transition == 'T_TRIP_ENDED'"

                sensed_transitions = r["transition_df"].query(query_str)
                print(sensed_transitions[["transition", "fmt_time"]])
                if phone_os == "android":
                    r["sensed_trip_ranges"] = find_ranges(sensed_transitions, "local.transition.exited_geofence", "local.transition.stopped_moving")
                else:
                    assert phone_os == "ios"
                    r["sensed_trip_ranges"] = find_ranges(sensed_transitions, "T_EXITED_GEOFENCE|T_VISIT_ENDED", "T_TRIP_ENDED|T_VISIT_STARTED")

                ground_truth_ranges = r["evaluation_trip_ranges"]
                # print([(r["start_ts"], arrow.get(r["start_ts"]).to("America/Los_Angeles"), r["end_ts"], arrow.get(r["end_ts"]).to("America/Los_Angeles")) for r in ground_truth_ranges])
                print(8 * ' ', len(r["sensed_trip_ranges"]), len(ground_truth_ranges))

#####
# END: Trip-specific segmentation code
#####

#####
# BEGIN: Section-specific segmentation code
#####

def get_transition_mask_android(df):
    # print(df.zzbhB.diff())
    return df.zzbhB.diff().abs().fillna(1) > 0

def get_transition_mask_ios(df):
    if len(df) == 0:
        return np.array([])
    
    ret_list = [True]
    valid_modes = ["walking", "cycling", "running", "automotive"]
    # print("df = %s" % df[valid_modes])
    # print("changes = %s" % np.diff(df[valid_modes], axis=0))
    for row in np.diff(df[valid_modes], axis=0):
        ret_list.append(row.any())
    ret_array = np.array(ret_list)
    # print(df.shape, ret_array.shape, ret_array)
    return ret_array

ANDROID_VALID_QUERY_WITH_STILL = "zzbhB not in [4,5]"
IOS_VALID_QUERY_WITH_STILL = "automotive == True | cycling == True | running == True | walking == True | stationary == True"
VALID_QUERIES_WITH_STILL = {"android": ANDROID_VALID_QUERY_WITH_STILL,
                 "ios": IOS_VALID_QUERY_WITH_STILL}

ANDROID_STILL_ENTRIES = "zzbhB == 3"
IOS_STILL_ENTRIES = "stationary == True"
STILL_ENTRIES = {"android": ANDROID_STILL_ENTRIES,
                 "ios": IOS_STILL_ENTRIES}

ANDROID_VALID_QUERY_NO_STILL = "zzbhB not in [3,4,5]"
IOS_VALID_QUERY_NO_STILL = "automotive == True | cycling == True | running == True | walking == True"
VALID_QUERIES_NO_STILL = {"android": ANDROID_VALID_QUERY_NO_STILL,
                 "ios": IOS_VALID_QUERY_NO_STILL}

def fill_sensed_section_ranges(pv):
    for phone_os, phone_map in pv.map().items():
        print(15 * "=*")
        print(phone_os, phone_map.keys())
        for phone_label, phone_detail_map in phone_map.items():
            print(4 * ' ', 15 * "-*")
            print(4 * ' ', phone_label, phone_detail_map["role"], phone_detail_map.keys())
            if "control" in phone_detail_map["role"]:
                print("Ignoring %s phone %s since they are always on" % (phone_detail_map["role"], phone_label))
                continue
            # this spec does not have any calibration ranges, but evaluation ranges are actually cooler
            for r in phone_detail_map["evaluation_ranges"]:
                print(8 * ' ', 30 * "=")
                print(8 * ' ',r.keys())
                print(8 * ' ',r["trip_id"], r["eval_common_trip_id"], r["eval_role"], len(r["evaluation_trip_ranges"]))
                for tr in r["evaluation_trip_ranges"]:
                    trip_ma_df = tr["motion_activity_df"]
                    # we may get some transitions after the trip ends 
                    # let's expand the activity range to account for that
                    trip_end_ts = tr["end_ts"]
                    extended_ma_df = r["motion_activity_df"].query(
                        "@trip_end_ts <= ts <= @trip_end_ts + 30 * 60")
                    ma_df = pd.concat([trip_ma_df, extended_ma_df],
                            axis="index")

                    curr_trip_section_transitions = find_section_transitions(
                        ma_df.query(VALID_QUERIES_NO_STILL[phone_os]),
                            TRANSITION_FNS[phone_os])
                    
                    last_section = tr["evaluation_section_ranges"][-1]
                    last_section_gt = pv.spec_details.get_ground_truth_for_leg(
                        tr["trip_id_base"], last_section["trip_id_base"])
                    if last_section_gt["mode"] == "WALKING":
                        # For trip that end in walking, we need to include still transitions as valid
                        # otherwise, there is no end transition from walking to a valid mode
                        if len(curr_trip_section_transitions) > 0:
                            curr_last_transition_ts = curr_trip_section_transitions.iloc[-1].ts
                        else:
                            curr_last_transition_ts = 0
                        print("Trip ending in walking found, checking for any final still transitions > %s" % curr_last_transition_ts)
                        still_section_transitions = extended_ma_df.query(
                            "ts > @curr_last_transition_ts").query(
                                STILL_ENTRIES[phone_os])
                        if len(still_section_transitions) > 0:
                            curr_trip_section_transitions = \
                                curr_trip_section_transitions.append(
                                    still_section_transitions.iloc[0])
                        
                    print(curr_trip_section_transitions.index)
                    curr_trip_section_ranges = find_section_ranges(curr_trip_section_transitions,
                                                                           MAP_FNS[phone_os])
                    tr["sensed_section_ranges"] = curr_trip_section_ranges
                    # print([(r["start_ts"], arrow.get(r["start_ts"]).to("America/Los_Angeles"), r["end_ts"], arrow.get(r["end_ts"]).to("America/Los_Angeles")) for r in ground_truth_ranges])
                    print(8 * ' ', len(tr["sensed_section_ranges"]), len(tr["evaluation_section_ranges"]))    

ANDROID_MODE_MAP = {0: "AUTOMOTIVE", 1: "CYCLING", 2: "WALKING", 3: "STATIONARY"}
ANDROID_MAP_FN = lambda t: ANDROID_MODE_MAP[t["zzbhB"]]

def IOS_MAP_FN(t):
    t_series = pd.Series(t)
    all_true = t_series[t_series == True].index.tolist()
    if len(all_true) == 1:
        return all_true[0].upper()
    else:
        # Do something more sophisticated here?
        return "INVALID"

MAP_FNS = {"android": ANDROID_MAP_FN, "ios": IOS_MAP_FN}
TRANSITION_FNS = {"android": get_transition_mask_android, "ios": get_transition_mask_ios}

# For iOS, we simply convert the value to upper case, so we end up with CYCLING
# and AUTOMOTIVE instead of BICYCLING and IN_VEHICLE. So we map the base mode
# accordingly as well

BASE_MODE = {"WALKING": "WALKING",
    "BICYCLING": "CYCLING",
    "ESCOOTER": "CYCLING",
    "BUS": "AUTOMOTIVE",
    "TRAIN": "AUTOMOTIVE",
    "LIGHT_RAIL": "AUTOMOTIVE",
    "SUBWAY": "AUTOMOTIVE",
    "CAR": "AUTOMOTIVE"}

BASE_MODE_ANALYSED = {"WALKING": 1,
     "BICYCLING": 2,
     "ESCOOTER": 2,
     "BUS": 3,
     "TRAIN": 4,
     "LIGHT_RAIL": 4,
     "SUBWAY": 4,
     "CAR": 5}

def get_mode_check_results(segment, segment_gt_leg, matching_section_map):
    base_mode = BASE_MODE_ANALYSED[segment_gt_leg["mode"]]
    print(12 * ' ',segment["trip_id"], segment["trip_id_base"],
        segment_gt_leg["mode"], base_mode)
    sensed_section_range = matching_section_map[segment["trip_id"]]["match"]
    print(sensed_section_range)
    matching_sections = [s for s in sensed_section_range if s["mode"] == base_mode]
    print("For %s (%s -> %s) %s (%s), matching_sections = %s" % 
          (segment["trip_id"], segment["start_ts"], segment["end_ts"],
            segment_gt_leg["mode"], base_mode, matching_sections))
    matching_ts = sum([(s["end_ts"] - s["start_ts"]) for s in matching_sections])
    gt_duration = (segment["end_ts"] - segment["start_ts"])
    print("matching_ts = %s, gt_duration = %s" % (matching_ts, gt_duration))
    matching_pct = matching_ts / gt_duration
    return {"gt_mode": segment_gt_leg["mode"], "gt_duration": gt_duration,
        "gt_base_mode": base_mode, "matching_pct": matching_pct} 


#####
# END: Section-specific segmentation code
#####

