import requests
import time
import argparse
import sys
import json
import arrow


DEFAULT_USER = "shankari@eecs.berkeley.edu"

# modified SpecDetails class to keep track of spec details
class Spec:
    def __init__(self, spec):
        self.spec = spec
        self.spec_wrapper = self.spec["data"]
        self.spec = self.spec_wrapper["label"]

        print(f"Creating Spec object for spec_id {self.spec['name']}...")

        self.start_ts = self.spec_wrapper["start_ts"]
        self.end_ts = self.spec_wrapper["end_ts"]
        self.timezone = self.spec["region"]["timezone"]

        print(f"Evaluation ran from {arrow.get(self.start_ts).to(self.timezone)} -> {arrow.get(self.end_ts).to(self.timezone)}")

        self.phones = self.spec["phones"]


def retrieve_data_from_server(datastore_url, key, user, start_ts, end_ts):
    post_body = {
        "user": user,
        "key_list": [key],
        "start_time": start_ts,
        "end_time": end_ts
    }

    print(f"Retrieving data for: {post_body=}")
    try:
        response = requests.post(f"{datastore_url}/datastreams/find_entries/timestamp", json=post_body)
        print(f"{response=}")
        response.raise_for_status()
        ret_list = response.json()["phone_data"]
    except Exception as e:
        print(f"Got {type(e).__name__}: {e}, retrying...")
        time.sleep(10)
        response = requests.post(f"{datastore_url}/datastreams/find_entries/timestamp", json=post_body)
        print(f"{response=}")
        response.raise_for_status()
        ret_list = response.json()["phone_data"]

    for e in ret_list:
        e["data"]["write_ts"] = e["metadata"]["write_ts"]

    print(f"Found {len(ret_list)} entries")
    return ret_list


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


def main():
    args = parse_args()

    # handle case where --key, etc are specified
    if "--key" in sys.argv:
        data = retrieve_data_from_server(
            args.datastore_url,
            args.key,
            args.user,
            args.start_ts,
            args.end_ts)

        spec_id_str = "all" if args.spec_id is None else args.spec_id
        key_str = args.key.replace("/", "~").replace("_", "-")
        out_file = f"data_{spec_id_str}_{key_str}_{int(args.start_ts)}_{int(args.end_ts)}.json"

        with open(out_file, "w") as f:
            json.dump(data, f, indent=4)
        return
    
    # otherwise, build up phone view map
    ## 1) get specified spec, or get all specs if spec isn't specified
    specs = retrieve_data_from_server(
        args.datastore_url,
        "config/evaluation_spec",
        DEFAULT_USER,
        0,
        arrow.get().timestamp)
    if args.spec_id:
        specs = [s for s in specs if s["data"]["label"]["id"] == args.spec_id]

    # remove duplicate specs based on most recently written spec
    s_dict = dict()
    for s in specs:
        spec_id = s["data"]["label"]["id"]
        if (spec_id not in s_dict) or ((spec_id in s_dict) and (s["metadata"]["write_ts"] > s_dict[spec_id]["metadata"]["write_ts"])):
            s_dict[spec_id] = s

    ## 2) populate Spec objects (which consist of spec details) for each unique spec
    specs = [Spec(s) for s in s_dict.values()]
    print(len(specs))

if __name__ == "__main__":
    main()
