import argparse
import sys; sys.path.append("..")
from emeval.input.spec_details import ServerSpecDetails
from emeval.input.phone_view import PhoneView


SPEC_USER = "shankari@eecs.berkeley.edu"


def retrieve_data_from_server(datastore_url, user, key, start_ts, end_ts, spec_id=None):
    """
    Standalone function similar to ServerSpecDetails's implementation of retrieve_data.
    Used when --key, --user, --start-ts, and --end-ts are specified.
    """
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
        data = response.json()["phone_data"]
    except Exception as e:
        print(f"Got {type(e).__name__}: {e}, retrying...")
        time.sleep(10)
        response = requests.post(f"{datastore_url}/datastreams/find_entries/timestamp", json=post_body)
        print(f"{response=}")
        response.raise_for_status()
        data = response.json()["phone_data"]

    for e in data:
        e["data"]["write_ts"] = e["metadata"]["write_ts"]

    # dump all API calls to JSON files
    spec_id_str = "all" if not spec_id else spec_id.replace("_", "-")
    key_str = key.replace("/", "~").replace("_", "-")
    user_str = user.replace("@", "#").replace(".", "^")
    out_file = f"data_{spec_id_str}_{key_str}_{user_str}_{int(start_ts)}_{int(end_ts)}.json"

    with open(out_file, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Found {len(data)} entries")
    return data


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--datastore-url", type=str, default="http://localhost:8080")
    spec_args = parser.add_mutually_exclusive_group(required=True)
    spec_args.add_argument("--spec-id", type=str)
    spec_args.add_argument("--all", action="store_true")

    # if one of these arguments is specified, the others in this group must also be specified
    if any(arg in sys.argv for arg in ["--key", "--user", "--start-ts", "--end-ts"]):
        parser.add_argument("--key", type=str, required=True)
        parser.add_argument("--user", type=str, required=True)
        parser.add_argument("--start-ts", type=float, required=True)
        parser.add_argument("--end-ts", type=float, required=True)
    
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.spec_id:
        if "--key" not in sys.argv:
            spec_details = ServerSpecDetails(args.datastore_url, "shankari@eecs.berkeley.edu", args.spec_id)
            phone_view = PhoneView(spec_details)
