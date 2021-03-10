import requests
import time
import argparse
import arrow
import json


def retrieve_data_from_server(datastore_url, user, key, start_ts, end_ts):
    post_body = {
        "user": user,
        "key_list": key,
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


def main():
    # parse command line arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("--user", type=str, required=True)
    parser.add_argument("--endpoint", type=str, required=True)
    
    parser.add_argument("--datastore-url", type=str, default="http://localhost:8080")
    parser.add_argument("--start-ts", type=float, default=0)
    parser.add_argument("--end-ts", type=float, default=arrow.get().timestamp)
    
    args = parser.parse_args()

    # make server call
    data = retrieve_data_from_server(args.datastore_url, args.user, args.endpoint, args.start_ts, args.end_ts)

    # dump to json file
    out = args.endpoint.split("/")
    out = f"{out[0]}~{'-'.join(out[1].split('_'))}"
    out = f"data_{out}_{int(args.start_ts)}_{int(args.end_ts)}.json"
    with open(out, "w") as f:
        json.dump(data, f)

    # TODO: add support for filtering output by spec_id


if __name__ == "__main__":
    main()
