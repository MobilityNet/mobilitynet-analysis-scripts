import copy
import arrow
import pandas as pd
import emeval.input.phone_view as eipv

THIRTY_MIN = 30 * 60

def _copy_subset_range(r, tr, key, fuzz_factor = 0):
    print("Before filtering, trips = %s" % [(sr["data"]["start_fmt_time"], sr["data"]["end_fmt_time"]) for sr in r[key]])
    print("Filter range = %s -> %s" % 
        (arrow.get(tr["start_ts"]).to("America/Los_Angeles"),
         arrow.get(tr["end_ts"]).to("America/Los_Angeles")))
    if len(r[key]) > 0:
        tr[key] = [re for re in r[key]
            if tr["start_ts"] - fuzz_factor <= (re["data"]["start_ts"]) and
                re["data"]["end_ts"] <= (tr["end_ts"] + fuzz_factor)]
    else:
        # since there is no data, we don't need to select a subset
        tr[key] = r[key]
    print("After filtering, trips = %s" % [sr["data"]["start_fmt_time"] for sr in tr[key]])

def create_analysed_view(input_view, analysis_datastore, location_key, trip_key, section_key):
    av = copy.deepcopy(input_view)
    print("Finished copying %s, starting overwrite" %
        (input_view.spec_details.CURR_SPEC_ID))
    asd = av.spec_details
    # Overwrite the result so that we can read from the analysis datastore
    asd.DATASTORE_LOC = analysis_datastore
    for phone_os, phone_map in av.map().items():
        print(15 * "=*")
        print(phone_os, phone_map.keys())
        for phone_label, phone_detail_map in phone_map.items():
            print(4 * ' ', 15 * "-*")
            print(4 * ' ', phone_label, phone_detail_map["role"], phone_detail_map.keys())
            phone_detail_map["location_entries"] = av.spec_details.retrieve_data(
                phone_label, [location_key], av.spec_details.eval_start_ts, arrow.now().timestamp())
            location_df = pd.DataFrame([e["data"] for e in phone_detail_map["location_entries"]])
#             if len(location_df) > 0:
#                 location_df["hr"] = (location_df.ts - r["start_ts"])/3600.0
            phone_detail_map["location_df"] = location_df

            phone_detail_map["sensed_trip_ranges"] = av.spec_details.retrieve_data(
                phone_label, [trip_key],
                av.spec_details.eval_start_ts, arrow.now().timestamp())
            phone_detail_map["sensed_section_ranges"] = av.spec_details.retrieve_data(
                phone_label, [section_key],
                av.spec_details.eval_start_ts, arrow.now().timestamp())

            for r in phone_detail_map["evaluation_ranges"]:
                # moved down here from commented out line above
                if len(location_df) > 0:
                    location_df["hr"] = (location_df.ts - r["start_ts"])/3600.0
                print(8 * ' ', 30 * "=")
                print(8 * ' ',r.keys())
                print(8 * ' ',r["trip_id"], r["eval_common_trip_id"], r["eval_role"], len(r["evaluation_trip_ranges"]))
                # The datastreams API call filters by "metadata.write_ts".
                # Unfortunately, this means that we can't use it to retrieve any analysed results, since 
                # all the data was written and has a write_ts from when the pipeline was run
                # so let's just retrieve all data. We can start it at the start of the spec,
                # since we can't analyse any data before it exists, but we should end it now
                query = eipv.PhoneView._query_for_seg(r)
                eipv.PhoneView._copy_subset(phone_detail_map, r, "location_df", query)
                _copy_subset_range(phone_detail_map, r, "sensed_trip_ranges", THIRTY_MIN)
                for tr in r["evaluation_trip_ranges"]:
                    query = eipv.PhoneView._query_for_seg(tr)
                    eipv.PhoneView._copy_subset(r, tr, "location_df", query)
                    # Since we are not guaranteed to have a 1:1 mapping between
                    # ground truth and sensed ranges, we store the ranges in
                    # the enclosing entry and implement the matching as part of
                    # the evaluation. So the list of trips is in the range
                    # (already there, don't need to copy) and the list of
                    # sections is in the trip.
                    _copy_subset_range(phone_detail_map, tr, "sensed_section_ranges", THIRTY_MIN)
                    for sr in tr["evaluation_section_ranges"]:
                        query = eipv.PhoneView._query_for_seg(sr)
                        eipv.PhoneView._copy_subset(phone_detail_map, sr, "location_df", query)
    return av
