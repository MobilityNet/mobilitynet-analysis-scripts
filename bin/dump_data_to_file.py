import requests
import time
import argparse
import sys
import json


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
    if args.key:
        data = retrieve_data_from_server(args.datastore_url, args.key, args.user, args.start_ts, args.end_ts)
        with open(f"data_{'all' if args.spec_id is None else args.spec_id}_{args.key.replace('/', '~').replace('_', '-')}_{int(args.start_ts)}_{int(args.end_ts)}.json", "w") as f:
            json.dump(data, f, indent=4)

    # TODO: add spec_details/phone_view logic


if __name__ == "__main__":
    main()
