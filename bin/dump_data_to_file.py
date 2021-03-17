import requests
import time
import argparse
import sys
import json
import arrow
import copy


# instance variables for script
SPEC_USER = "shankari@eecs.berkeley.edu"
DATASTORE_URL = None
SPEC_ID = None

# modified SpecDetails class to keep track of spec details
class Spec:
    def __init__(self, spec):
        self.spec = spec
        self.spec_wrapper = self.spec["data"]
        self.spec = self.spec_wrapper["label"]
        self.id = self.spec["id"]

        print(f"Creating Spec object for spec_id {self.spec['name']}...")

        self.start_ts = self.spec_wrapper["start_ts"]
        self.end_ts = self.spec_wrapper["end_ts"]
        self.timezone = self.spec["region"]["timezone"]

        print(f"Evaluation ran from {arrow.get(self.start_ts).to(self.timezone)} -> {arrow.get(self.end_ts).to(self.timezone)}")

        self.phones = self.spec["phones"]


def retrieve_data_from_server(key, user, start_ts, end_ts):
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

    # dump all API calls to JSON files
    spec_id_str = "all" if not SPEC_ID else SPEC_ID.replace("_", "-")
    key_str = key.replace("/", "~").replace("_", "-")
    user_str = user.replace("@", "#").replace(".", "^")
    out_file = f"data_{spec_id_str}_{key_str}_{user_str}_{int(start_ts)}_{int(end_ts)}.json"

    with open(out_file, "w") as f:
            json.dump(data, f, indent=4)

    print(f"Found {len(data)} entries")
    return data


def get_specs():
    specs = retrieve_data_from_server("config/evaluation_spec", SPEC_USER, 0, arrow.get().timestamp)
    
    # filter by spec_id if specified
    if SPEC_ID:
        specs = [s for s in specs if s["data"]["label"]["id"] == SPEC_ID]

    # remove duplicate specs based on most recently written spec
    s_dict = dict()
    for s in specs:
        s_id = s["data"]["label"]["id"]
        if (s_id not in s_dict) or ((s_id in s_dict) and (s["metadata"]["write_ts"] > s_dict[s_id]["metadata"]["write_ts"])):
            s_dict[s_id] = s

    # populate Spec objects (which consist of spec details) for each unique spec
    specs = [Spec(s) for s in s_dict.values()]
    
    return specs


def fill_transitions(specs):
    pv_maps = dict.fromkeys([s.id for s in specs])
    for spec in specs:
        pv_maps[spec.id] = copy.deepcopy(spec.phones)
        for phone_os, phone_map in pv_maps[spec.id].items():
            for phone_label in phone_map:
                phone_map[phone_label] = {
                    "role": phone_map[phone_label],
                    "transitions": retrieve_data_from_server("manual/evaluation_transition", phone_label, spec.start_ts, spec.end_ts)
                }

    return pv_maps


def _filter_transitions(key_prefix, start_tt, end_tt, start_ti, end_ti):



def build_maps():
    # get specified spec, or get all specs if spec isn't specified
    specs = get_specs()

    # fill transitions
    pv_maps = fill_transitions(specs)


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
    
    # set script-level variables
    DATASTORE_URL = args.datastore_url
    SPEC_ID = args.spec_id

    # handle case where --key, etc are specified
    if "--key" in sys.argv:
        retrieve_data_from_server(args.key, args.user, args.start_ts, args.end_ts)
    else:
        # run data dumping pipeline
        build_maps()
