import json
import os
import requests
import time
import arrow
import argparse
import sys; sys.path.append("..")
from emeval.input.spec_details import ServerSpecDetails
from emeval.input.phone_view import PhoneView


DEFAULT_SPEC_USER = "shankari@eecs.berkeley.edu"
DATASTORE_URL = None

def dump_to_file(data, spec_id, user, key, start_ts, end_ts):
    """
    Dumped outputs are created recursively in folder relative to path of script.
    """
    key = key.replace("/", "~") # key could have a slash, replace with tilde

    out_path = os.path.join("data", spec_id, user, key)
    os.makedirs(out_path, exist_ok=True)

    out_file = os.path.join(out_path, f"{start_ts}_{end_ts}.json")

    print(f"Creating {out_file=}...")

    with open(out_file, "w") as f:
        json.dump(data, f, indent=4)


def retrieve_data_from_server(user, key, start_ts, end_ts):
    """
    Standalone function similar to ServerSpecDetails's implementation of retrieve_data.
    Used when --key, --user, --start-ts, and --end-ts are specified & for retrieving all specs
    """
    post_body = {
        "user": user,
        "key_list": [key],
        "start_time": start_ts,
        "end_time": end_ts
    }

    print(f"Retrieving data for: {post_body=}")
    try:
        response = requests.post(f"{DATASTORE_URL}/datastreams/find_entries/timestamp", json=post_body)
        print(f"{response=}")
        response.raise_for_status()
        data = response.json()["phone_data"]
    except Exception as e:
        print(f"Got {type(e).__name__}: {e}, retrying...")
        time.sleep(10)
        response = requests.post(f"{DATASTORE_URL}/datastreams/find_entries/timestamp", json=post_body)
        print(f"{response=}")
        response.raise_for_status()
        data = response.json()["phone_data"]

    for e in data:
        e["data"]["write_ts"] = e["metadata"]["write_ts"]

    print(f"Found {len(data)} entries")
    return data


def get_all_spec_ids():
    spec_data = retrieve_data_from_server(DEFAULT_SPEC_USER, "config/evaluation_spec", 0, arrow.get().timestamp)
    spec_ids = [s["data"]["label"]["id"] for s in spec_data]
    return set(spec_ids)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--datastore-url", type=str, default="http://localhost:8080")
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

    # set instance variables
    DATASTORE_URL = args.datastore_url

    # verify spec_id is valid if specified
    spec_ids = get_all_spec_ids()
    if args.spec_id:
        assert args.spec_id in spec_ids, f"spec_id `{args.spec_id}` not found within current datastore instance"
        spec_ids = [args.spec_id]

    # if --key, etc are specified, just call the function above
    if "--key" in sys.argv:
        for s_id in spec_ids:
            data = retrieve_data_from_server(args.user, args.key, args.start_ts, args.end_ts)
            dump_to_file(data, s_id, args.user, args.key, args.start_ts, args.end_ts)
    else:
        # create spec_details objects depending on flag specified
        spec_detailss = [ServerSpecDetails(DATASTORE_URL, DEFAULT_SPEC_USER, s_id) for s_id in spec_ids]

        # build and dump phone view maps
        for sd in spec_detailss:
            pv = PhoneView(sd)
            for phone_os, phone_map in pv.map().items():
                for phone_label, phone_detail_map in phone_map.items():
                    for r in phone_detail_map["evaluation_ranges"]:
                        for key in [k for k in r.keys() if "_entries" in k]:
                            dump_to_file(r[key], sd.CURR_SPEC_ID, phone_label, key, r["start_ts"], r["end_ts"])
