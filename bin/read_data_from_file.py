import requests
import time
import sys

from config import USER, DATASTORE_URL


def retrieve_data_from_server(user, key, start_ts, end_ts):
    post_body = {
        "user": user,
        "key_list": key,
        "start_time": start_ts,
        "end_time": end_ts
    }

    print(f"Retrieving data for: {post_body=}")
    try:
        response = requests.post(f"{DATASTORE_URL}/datastreams/find_entries/timestamp", json=post_body)
        print(f"{response=}")
        response.raise_for_status()
        ret_list = response.json()["phone_data"]
    except Exception as e:
        print(f"Got {type(e).__name__}: {e}, retrying...")
        time.sleep(10)
        response = requests.post(f"{DATASTORE_URL}/datastreams/find_entries/timestamp", json=post_body)
        print(f"{response=}")
        response.raise_for_status()
        ret_list = response.json()["phone_data"]

    for e in ret_list:
        e["data"]["write_ts"] = e["metadata"]["write_ts"]

    print(f"Found {len(ret_list)} entries")



def main():
    # parse input file
    in_file = sys.argv[1]
    server_args = in_file.split(".")[0].split("_")
    assert server_args[0] == "data" and len(server_args) == 5, "Invalid input file."
    server_args = server_args[1:]

    key = f"{server_args[0]}/{'_'.join(server_args[1].split('-'))}"
    
    start_ts = server_args[2]
    end_ts = server_args[3]

    print(retrieve_data_from_server(USER, key, start_ts, end_ts))


if __name__ == "__main__":
    main()