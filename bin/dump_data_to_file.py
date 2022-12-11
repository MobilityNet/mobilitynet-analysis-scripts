import json
import os
import math
import requests
import time
import arrow
import argparse
import sys; sys.path.append("..")
import emeval.input.spec_details as eisd
import emeval.input.phone_view as eipv
import emeval.analysed.phone_view as eapv

THIRTY_MIN = 30 * 60

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
    return eisd.ServerSpecDetails(datastore_url, author_email).retrieve_one_batch(user, [key], start_ts, end_ts)

def download_analysed(args):
    fsd = eisd.FileSpecDetails(args.raw_dir, args.author_email)
    spec_ids = fsd.get_all_spec_ids()
    if args.spec_id:
        assert args.spec_id in spec_ids,\
               f"spec_id `{args.spec_id}` not found within current datastore instance"

        spec_ids = [args.spec_id]

    print(f"download_analysed called with {args}")
    retrieve_analysis_data(args.raw_dir, args.datastore_url, args.author_email, spec_ids, args.out_dir+"/"+args.analysis_tag)

def retrieve_analysis_data(raw_dir, datastore_url, author_email, spec_ids, out_dir):
    print(f"Pulling analysis results for {spec_ids[0] if len(spec_ids) == 1 else 'all specs in datastore'}...")

    # collect ServerSpecDetails objects, dump specs
    sds = []
    for s_id in spec_ids:
        sd = eisd.FileSpecDetails(raw_dir, author_email, s_id)
        pv = eipv.PhoneView(sd)
        asd = eisd.ServerSpecDetails(datastore_url, author_email, s_id)

        ANALYSED_RESULT_KEYS=[
            ("segmentation/raw_trip", "data.start_ts"),
            ("segmentation/raw_section", "data.start_ts"),
            ("segmentation/raw_untracked", "data.start_ts"),
            ("analysis/cleaned_trip", "data.start_ts"),
            ("analysis/cleaned_section", "data.start_ts"),
            ("analysis/cleaned_untracked", "data.start_ts"),
            ("analysis/inferred_section", "data.start_ts"),
            ("analysis/confirmed_trip", "data.start_ts"),
            ("analysis/recreated_location", "data.ts")]

        for phone_os, phone_map in pv.map().items():
            for phone_label, phone_detail_map in phone_map.items():
                for ranges in [phone_detail_map["evaluation_ranges"], phone_detail_map["calibration_ranges"]]:
                    for r in ranges:
                        for key, key_time in ANALYSED_RESULT_KEYS:
                            print(f"Dumping key {key} for key_time = {key_time} and phone {phone_label}")
                            padded_start_ts = r["start_ts"] - THIRTY_MIN
                            padded_end_ts = r["end_ts"] + THIRTY_MIN
                            print(f"original range = {arrow.get(r['start_ts'])} -> {arrow.get(r['end_ts'])},"
                                  f"padded range = {arrow.get(padded_start_ts)} -> {arrow.get(padded_end_ts)}")
                            raw_data = asd.retrieve_data(phone_label, [key],
                                padded_start_ts, padded_end_ts, key_time)
                            dump_data_to_file(
                                raw_data,
                                sd.CURR_SPEC_ID,
                                phone_label,
                                key,
                                padded_start_ts,
                                padded_end_ts,
                                out_dir)

def download_raw_subset(args):
    ssd = eisd.ServerSpecDetails(args.datastore_url, args.author_email)
    spec_ids = ssd.get_all_spec_ids()
    if args.spec_id:
        assert args.spec_id in spec_ids,\
               f"spec_id `{args.spec_id}` not found within current datastore instance"

        spec_ids = [args.spec_id]
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

def download_raw(args):
    ssd = eisd.ServerSpecDetails(args.datastore_url, args.author_email)
    spec_ids = ssd.get_all_spec_ids()
    if args.spec_id:
        assert args.spec_id in spec_ids,\
               f"spec_id `{args.spec_id}` not found within current datastore instance"

        spec_ids = [args.spec_id]
    retrieve_all_data(args.datastore_url, args.author_email, spec_ids, args.out_dir)

def retrieve_all_data(datastore_url, author_email, spec_ids, out_dir):
    """
    Runs the full data retrieval pipeline in the event that a user/key/start_ts/end_ts combination isn't provided.
    """
    print(f"Running full pipeline for {spec_ids[0] if len(spec_ids) == 1 else 'all specs in datastore'}...")

    # collect ServerSpecDetails objects, dump specs
    sds = []
    for s_id in spec_ids:
        sd = eisd.ServerSpecDetails(datastore_url, author_email, s_id)
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
        pv = eipv.PhoneView(sd)
        for phone_os, phone_map in pv.map().items():
            for phone_label, phone_detail_map in phone_map.items():
                for key in [k for k in phone_detail_map.keys() if "/" in k]:
                    print(f"Dumping top level key {key}")
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
                            print(f"Dumping key {key} for range with keys {r.keys()} and phone {phone_label}")
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

    subparsers = parser.add_subparsers(required=True, dest="download_type")
    parser_raw = subparsers.add_parser('raw', help='Download raw data')
    parser_raw.set_defaults(func=download_raw)

    parser_raw_subset = subparsers.add_parser('raw_subset', help='Download a subset of the raw data')
    parser_raw_subset.add_argument("--key",
                        type=str,
                        required=True,
                        help="The time series key to be used if a single call "
                             "to the E-Mission Server instance is to be made. "
                             "--user, --start-ts, and --end-ts must also be specified.")
    
    parser_raw_subset.add_argument("--user",
                        type=str,
                        required=True,
                        help="The user to be used if a single call to the E-Mission Server instance is to be made. "
                             "--key, --start-ts, and --end-ts must also be specified.")
    
    parser_raw_subset.add_argument("--start-ts",
                        type=float,
                        required=True,
                        help="The starting timestamp from which to pull data if a single call "
                             "to the E-Mission Server instance is to be made. "
                             "--key, --user, and --end-ts must also be specified.")
    
    parser_raw_subset.add_argument("--end-ts",
                        type=float,
                        required=True,
                        help="The ending timestamp from which to pull data if a single call "
                             "to the E-Mission Server instance is to be made. "
                             "--key, --user, and --start-ts must also be specified.")
    parser_raw_subset.set_defaults(func=download_raw_subset)

    parser_analysed = subparsers.add_parser('analysed', help='Download analysed data')
    parser_analysed.add_argument("analysis_tag",
                        type=str,
                        help="Tag the analysed data as being for the specified branch/algorithm."
                             "Required to avoid inadvertent overwrites"
                             "Will be appended to out_dir before data is stored")
    parser_analysed.add_argument("--raw_dir",
                        type=str,
                        default="data",
                        help="Location where the raw data has already been downloaded."
                             "This is needed to get the transitions and download the matching analysed data"
                             "[default: data]")
    parser_analysed.set_defaults(func=download_analysed)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # verify spec_id is valid if specified
    args.func(args)
