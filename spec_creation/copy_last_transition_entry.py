import requests
import json
import argparse
import logging
import time
import copy
import arrow

def retrieve_data_from_server(datastore_url, user_label, key_list, start_ts, end_ts):
    post_msg = {
        "user": user_label,
        "key_list": key_list,
        "start_time": start_ts,
        "end_time": end_ts
    }
    # print("About to retrieve messages using %s" % post_msg)
    response = requests.post(datastore_url+"/datastreams/find_entries/timestamp", json=post_msg)
    # print("response = %s" % response)
    response.raise_for_status()
    ret_list = response.json()["phone_data"]
    # print("Found %d entries" % len(ret_list))
    return ret_list

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("server_url",
        help="host and port of the server that will store and collect data")
    parser.add_argument("from_phone_label",
        help="the phone that we will copy the transition from")
    parser.add_argument("to_phone_label",
        help="the phone that we will copy the transition to")
    parser.add_argument("transition_type",
        help="the type of transition that we will copy")
    parser.add_argument("trip_id",
        help="the trip id to copy")
    parser.add_argument("spec_id",
        help="the spec_id to match")
    parser.add_argument("-x", "--cross-platform", action="store_true",
        help="whether the transition is across platforms")
    parser.add_argument("-v", "--verbose", action='store_true',
        help="whether we should use verbose logging or not.")

    args = parser.parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    from_phone_transitions = retrieve_data_from_server(args.server_url,
        args.from_phone_label,
        ["manual/evaluation_transition"], 0, arrow.get().timestamp)

    type_check = lambda t: t["data"]["transition"] == args.transition_type
    trip_check = lambda t: t["data"]["trip_id"] == args.trip_id
    spec_check = lambda t: t["data"]["spec_id"] == args.spec_id
    matching_transitions = [t for t in from_phone_transitions if type_check(t) and trip_check(t) and spec_check(t)]

    logging.debug("Filtered %d total => %d %s transitions" %
        (len(from_phone_transitions), len(matching_transitions), args.transition_type))

    if (len(matching_transitions) == 0):
        print("No matching transitions found, aborting")
        exit(1)

    last_matching_transition = matching_transitions[-1]
    print("Found entry %s for user %s" %
        (last_matching_transition["_id"], last_matching_transition["user_id"]))
    del last_matching_transition["_id"]
    del last_matching_transition["user_id"]
    print("After trimming, entry is %s" % last_matching_transition)

    if args.cross_platform:
        to_phone_transitions = retrieve_data_from_server(args.server_url,
            args.to_phone_label,
            ["manual/evaluation_transition"], 0, arrow.get().timestamp)
        sample_dest = to_phone_transitions[0]
        last_matching_transition["metadata"]["platform"] = sample_dest["metadata"]["platform"]
        for k in sample_dest["data"]:
            if k.startswith("device"):
                last_matching_transition["data"][k] = sample_dest["data"][k]
        print("After cross-platforming, entry is %s" % last_matching_transition)

    post_putone_msg = {
        "user": args.to_phone_label,
        "the_entry": last_matching_transition
    }
    print("About to post data %s to server %s" % (post_putone_msg, args.server_url))
    requests.post(args.server_url+"/usercache/putone", json=post_putone_msg)
