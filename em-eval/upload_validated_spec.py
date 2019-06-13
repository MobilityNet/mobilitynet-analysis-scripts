import requests
import json
import argparse
import logging
import time
import copy

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("server_url",
        help="host and port of the server that will store and collect data")
    parser.add_argument("user_email",
        help="specify the user email that can be used for follow-ups/questions")
    parser.add_argument("validated_spec_file",
        help="the name of the file that validated spec. Make sure that the spec is valid by using the 'Validate spec before upload.ipynb' notebook")
    parser.add_argument("-v", "--verbose", action='store_true',
        help="whether we should use verbose logging or not.")

    args = parser.parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    post_register_msg = {
        "user": args.user_email
    }

    logging.debug("About to post data %s to server %s" % (post_register_msg, args.server_url))
    requests.post(args.server_url+"/profile/create", json=post_register_msg)

    validated_spec_json = json.load(open(args.validated_spec_file))
    md = {
        "key": "config/evaluation_spec",
        "platform": "script",
        "write_ts": time.time(),
        "time_zone": validated_spec_json["region"]["timezone"],
        "type": "message"
    }

    validated_label = copy.copy(validated_spec_json)
    del validated_label["start_ts"]
    del validated_label["end_ts"]
    post_putone_msg = {
        "user": args.user_email,
        "the_entry": {
            "data": {
                "start_ts": validated_spec_json["start_ts"],
                "end_ts": validated_spec_json["end_ts"],
                "label": validated_label,
            },
            "metadata": md
        }
    }
    logging.debug("About to post data %s to server %s" % (post_putone_msg, args.server_url))
    requests.post(args.server_url+"/usercache/putone", json=post_putone_msg)
