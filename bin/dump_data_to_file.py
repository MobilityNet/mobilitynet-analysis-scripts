import json
import os
import math
import requests
import time
import arrow
import argparse
import sys; sys.path.append("..")
from emeval.input.spec_details import ServerSpecDetails
from emeval.input.phone_view import PhoneView


def dump_to_file(data, spec_id, user, key, start_ts, end_ts, out_dir):
    """
    Dumped outputs are created recursively in folder relative to path of script.
    """
    key = key.replace("/", "~") # key could have a slash, replace with tilde

    out_path = os.path.join(out_dir, user, spec_id, key)
    os.makedirs(out_path, exist_ok=True)

    out_file = os.path.join(out_path, f"{math.floor(start_ts)}_{math.ceil(end_ts)}.json")

    print(f"Creating {out_file=}...")

    with open(out_file, "w") as f:
        json.dump(data, f, indent=4)


def get_all_spec_ids(datastore_url, spec_user):
    spec_data = ServerSpecDetails(datastore_url, spec_user).retrieve_data(spec_user, ["config/evaluation_spec"], 0, arrow.get().timestamp)
    spec_ids = [s["data"]["label"]["id"] for s in spec_data]
    return set(spec_ids)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--out-dir", type=str, default="data")
    parser.add_argument("--datastore-url", type=str, default="http://localhost:8080")
    parser.add_argument("--spec-user", type=str, default="shankari@eecs.berkeley.edu")
    parser.add_argument("--spec-id", type=str)

    # if one of these arguments is specified, the others in this group must also be specified
    if any(arg in sys.argv for arg in ["--key", "--user", "--start-ts", "--end-ts"]):
        parser.add_argument("--key", type=str, required=True)
        parser.add_argument("--user", type=str, required=True)
        parser.add_argument("--start-ts", type=float, required=True)
        parser.add_argument("--end-ts", type=float, required=True)
    
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # verify spec_id is valid if specified
    spec_ids = get_all_spec_ids(args.datastore_url, args.spec_user)
    if args.spec_id:
        assert args.spec_id in spec_ids, f"spec_id `{args.spec_id}` not found within current datastore instance"
        spec_ids = [args.spec_id]

    # if --key, etc are specified, just call the function above
    if "--key" in sys.argv:
        for s_id in spec_ids:
            data = ServerSpecDetails(args.datastore_url, args.user).retrieve_data(args.user, [args.key], args.start_ts, args.end_ts)
            dump_to_file(data, s_id, args.user, args.key, args.start_ts, args.end_ts, args.out_dir)
    else:
        # create spec_details objects depending on flag specified
        print(f"Running full pipeline for {args.spec_id if args.spec_id else 'all specs in datastore'}...")

        sds = []
        for s_id in spec_ids:
            sd = ServerSpecDetails(args.datastore_url, args.spec_user, s_id)
            sds.append(sd)
            dump_to_file(sd.curr_spec_entry, sd.CURR_SPEC_ID, args.spec_user, "config/evaluation_spec", 0, arrow.get().timestamp, args.out_dir)

        # build and dump phone view maps
        for sd in sds:
            pv = PhoneView(sd)
            for phone_os, phone_map in pv.map().items():
                for phone_label, phone_detail_map in phone_map.items():
                    for r in phone_detail_map["evaluation_ranges"]:
                        for key in [k for k in r.keys() if "_entries" in k]:
                            dump_to_file(r[key], sd.CURR_SPEC_ID, phone_label, key, r["start_ts"], r["end_ts"], args.out_dir)
