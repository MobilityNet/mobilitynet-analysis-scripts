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
    range_list = []
    for t in transition_df.to_dict(orient='records'):
        # print("Considering transition %s" % t)
        if start_ts is None and t["transition"] == start_transition:
            start_ts = t["ts"]
        elif start_ts is not None and t["transition"] == end_transition:
            range_list.append({"start_ts": start_ts, "end_ts": t["ts"]})
            start_ts = None
    # print("Returning %s" % range_list)
    return range_list

def find_closest_segment_idx(gt, sensed_segments, key):
    ts_diffs = [abs(gt[key] - st[key]) for st in sensed_segments]
    import arrow
    print("diffs for %s %s = %s" % (key, arrow.get(gt[key]).to("America/Los_Angeles"), ts_diffs))
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
            print("for gt = %s, start_segment_idx = %s, end_segment_id = %s" %
                (gt["trip_id"], start_segment_idx, end_segment_idx))
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
