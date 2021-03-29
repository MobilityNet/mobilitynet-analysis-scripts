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


def dump_data_to_file(data, spec_id, user, key, start_ts, end_ts, out_dir):
    """
    Accepts serializable data (e.g. dict, array of dicts) and dumps it into an output file.
    Dumped file are created recursively in the folder name specified by `out_dir` as such:

    out_dir
    └── user
        └── spec_id
            └── key
                └── {start_ts}_{end_ts}.json
    """
    # key could have a slash if it corresponds to a key_list argument in SpecDetails::retrieve_data
    # slashes are invalid in directory names, so replace with tilde
    out_path = os.path.join(out_dir, user, spec_id, key.replace("/", "~"))
    os.makedirs(out_path, exist_ok=True)

    out_file = os.path.join(out_path, f"{math.floor(start_ts)}_{math.ceil(end_ts)}.json")
    print(f"Creating {out_file=}...")
    with open(out_file, "w") as f:
        json.dump(data, f, indent=4)


def make_call_to_server(datastore_url, author_email, user, key, start_ts, end_ts):
    """
    Makes a direct call to the E-Mission Server instance based on the specified user/key/start_ts/end_ts.
    """
    return ServerSpecDetails(datastore_url, author_email).retrieve_one_batch(user, [key], start_ts, end_ts)


def get_all_spec_ids(datastore_url, author_email):
    """
    Retrieves list of all spec_id's on E-Mission Server instance being used by script.
    """
    spec_data = make_call_to_server(
        datastore_url,
        author_email,
        author_email,
        "config/evaluation_spec",
        0,
        sys.maxsize)

    spec_ids = [s["data"]["label"]["id"] for s in spec_data]

    return set(spec_ids)


def run_full_pipeline(datastore_url, author_email, spec_ids, out_dir):
    """
    Runs the full data retrieval pipeline in the event that a user/key/start_ts/end_ts combination isn't provided.
    """
    print(f"Running full pipeline for {spec_ids[0] if len(spec_ids) == 1 else 'all specs in datastore'}...")

    # collect ServerSpecDetails objects, dump specs
    sds = []
    for s_id in spec_ids:
        sd = ServerSpecDetails(datastore_url, author_email, s_id)
        sds.append(sd)
        dump_data_to_file(
            sd.curr_spec_entry,
            sd.CURR_SPEC_ID,
            author_email,
            "config/evaluation_spec",
            0,
            sys.maxsize,
            out_dir)

    # build and dump phone view maps
    for sd in sds:
        pv = PhoneView(sd)
        for phone_os, phone_map in pv.map().items():
            for phone_label, phone_detail_map in phone_map.items():
                for key in [k for k in phone_detail_map.keys() if "/" in k]:
                    dump_data_to_file(
                        phone_detail_map[key],
                        sd.CURR_SPEC_ID,
                        phone_label,
                        key,
                        sd.eval_start_ts,
                        sd.eval_end_ts,
                        out_dir)
                for ranges in [phone_detail_map["evaluation_ranges"], phone_detail_map["calibration_ranges"]]:
                    for r in ranges:
                        for key in [k for k in r.keys() if "/" in k]:
                            dump_data_to_file(
                                r[key],
                                sd.CURR_SPEC_ID,
                                phone_label,
                                key,
                                r["start_ts"],
                                r["end_ts"],
                                out_dir)


def parse_args():
    """
    Defines command line arguments for script.
    """
    parser = argparse.ArgumentParser(
        description="Script that retrieves data from an E-Mission Server instance "
                    "and dumps it into a hierarchical collection of JSON files.")

    parser.add_argument("--out-dir",
                        type=str,
                        default="data",
                        help="The name of the directory that data will be dumped to. "
                             "Will be created if not already present. "
                             "[default: data]")

    parser.add_argument("--datastore-url",
                        type=str,
                        default="http://localhost:8080",
                        help="The URL of the E-Mission Server instance from which data will be pulled. "
                             "[default: http://localhost:8080]")
    
    parser.add_argument("--author-email",
                        type=str,
                        default="shankari@eecs.berkeley.edu",
                        help="The user associated with retrieving specs. "
                             "This is usually the email of a spec author. "
                             "[default: shankari@eecs.berkeley.edu]")
    
    parser.add_argument("--spec-id",
                        type=str,
                        help="The particular spec to retrieve data for. "
                             "If not specified, data will be retrieved for all specs "
                             "on the specified datastore instance.")

    # if one of these arguments is specified, the others in this group must also be specified
    parser.add_argument("--key",
                        type=str,
                        help="The time series key to be used if a single call "
                             "to the E-Mission Server instance is to be made. "
                             "--user, --start-ts, and --end-ts must also be specified.")
    
    parser.add_argument("--user",
                        type=str,
                        help="The user to be used if a single call to the E-Mission Server instance is to be made. "
                             "--key, --start-ts, and --end-ts must also be specified.")
    
    parser.add_argument("--start-ts",
                        type=float,
                        help="The starting timestamp from which to pull data if a single call "
                             "to the E-Mission Server instance is to be made. "
                             "--key, --user, and --end-ts must also be specified.")
    
    parser.add_argument("--end-ts",
                        type=float,
                        help="The ending timestamp from which to pull data if a single call "
                             "to the E-Mission Server instance is to be made. "
                             "--key, --user, and --start-ts must also be specified.")
    
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # enforce that --key, --user, --start-ts, and --end-ts are all specifed if one of these arguments is specified
    cond_req_args = ["--key", "--user", "--start-ts", "--end-ts"]
    for arg in cond_req_args:
        if arg in sys.argv:
            assert set(a for a in cond_req_args if a != arg) <= set(sys.argv),\
                   "all of --key --user, --start-ts, and --end-ts must be specified"

    # verify spec_id is valid if specified
    spec_ids = get_all_spec_ids(args.datastore_url, args.author_email)
    if args.spec_id:
        assert args.spec_id in spec_ids,\
               f"spec_id `{args.spec_id}` not found within current datastore instance"
        
        spec_ids = [args.spec_id]

    # if --key, etc are specified, just call retrieve_data from an anonymous ServerSpecDetails instance
    if args.key:
        for s_id in spec_ids:
            data = make_call_to_server(
                args.datastore_url,
                args.author_email,
                args.user,
                args.key,
                args.start_ts,
                args.end_ts)
            
            dump_data_to_file(
                data,
                s_id,
                args.user,
                args.key,
                args.start_ts,
                args.end_ts,
                args.out_dir)
    
    else:
        run_full_pipeline(args.datastore_url, args.author_email, spec_ids, args.out_dir)
