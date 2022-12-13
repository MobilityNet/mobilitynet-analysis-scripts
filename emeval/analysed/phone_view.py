import copy
import arrow
import pandas as pd
import emeval.input.phone_view as eipv

THIRTY_MIN = 30 * 60

def _copy_subset_range(r, tr, key, fuzz_factor = 0):
    print("Before filtering, trips = %s" % [(sr["start_fmt_time"], sr["end_fmt_time"]) for sr in r[key]])
    print("Filter range = %s -> %s" % 
        (arrow.get(tr["start_ts"]).to("America/Los_Angeles"),
         arrow.get(tr["end_ts"]).to("America/Los_Angeles")))
    if len(r[key]) > 0:
        tr[key] = [re for re in r[key]
            if tr["start_ts"] - fuzz_factor <= (re["start_ts"]) and
                re["end_ts"] <= (tr["end_ts"] + fuzz_factor)]
    else:
        # since there is no data, we don't need to select a subset
        tr[key] = r[key]
    print("After filtering, trips = %s" % [sr["start_fmt_time"] for sr in tr[key]])

def create_analysed_view(input_view, analysis_spec, location_key, trip_key, section_key):
    av = copy.deepcopy(input_view)
    print("Finished copying %s, starting overwrite" %
        (input_view.spec_details.CURR_SPEC_ID))
    analysis_spec.CURR_SPEC_ID = av.spec_details.CURR_SPEC_ID
    analysis_spec.curr_spec_entry = av.spec_details.curr_spec_entry
    analysis_spec.populate_spec_details(av.spec_details.curr_spec_entry)
    av.spec_details = analysis_spec
    for phone_os, phone_map in av.map().items():
        print(15 * "=*")
        print(phone_os, phone_map.keys())
        for phone_label, phone_detail_map in phone_map.items():
            print(4 * ' ', 15 * "-*")
            print(4 * ' ', phone_label, phone_detail_map["role"], phone_detail_map.keys())
            for r in phone_detail_map["evaluation_ranges"]:
                print(8 * ' ', 30 * "=")
                print(8 * ' ',r.keys())
                print(8 * ' ',r["trip_id"], r["eval_common_trip_id"], r["eval_role"], len(r["evaluation_trip_ranges"]))

                padded_start_ts = r["start_ts"] - THIRTY_MIN
                padded_end_ts = r["end_ts"] + THIRTY_MIN

                phone_detail_map["location_entries"] = av.spec_details.retrieve_data(
                    phone_label, [location_key], padded_start_ts, padded_end_ts)
                location_df = pd.DataFrame([e["data"] for e in phone_detail_map["location_entries"]])
                if len(location_df) > 0:
                    location_df["hr"] = (location_df.ts-r["start_ts"])/3600.0
                r["location_df"] = location_df

                r["sensed_trip_ranges"] = [st["data"] for st in av.spec_details.retrieve_data(
                    phone_label, [trip_key],
                    padded_start_ts, padded_end_ts)]

                r["sensed_section_ranges"] = [ss["data"] for ss in av.spec_details.retrieve_data(
                    phone_label, [section_key],
                    padded_start_ts, padded_end_ts)]

                for tr in r["evaluation_trip_ranges"]:
                    query = eipv.PhoneView._query_for_seg(tr)
                    eipv.PhoneView._copy_subset(r, tr, "location_df", query)
                    # Since we are not guaranteed to have a 1:1 mapping between
                    # ground truth and sensed ranges, we store the ranges in
                    # the enclosing entry and implement the matching as part of
                    # the valuation. So the list of trips is in the range
                    # (already there, don't need to copy) and the list of
                    # sections is in the trip.
                    _copy_subset_range(r, tr, "sensed_section_ranges", THIRTY_MIN)
                    for sr in tr["evaluation_section_ranges"]:
                        query = eipv.PhoneView._query_for_seg(sr)
                        eipv.PhoneView._copy_subset(tr, sr, "location_df", query)
    return av
