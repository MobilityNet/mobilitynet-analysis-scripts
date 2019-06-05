import requests
import json
import argparse
import logging

import emission.core.wrapper.metadata as ecwm

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
    md = ecwm.Metadata.create_metadata_for_result("config/evaluation_spec")
    md["type"] = "message"
    post_putone_msg = {
        "user": args.user_email,
        "the_entry": {
            "data": validated_spec_json,
            "metadata": md
        }
    }
    logging.debug("About to post data %s to server %s" % (post_putone_msg, args.server_url))
    requests.post(args.server_url+"/usercache/putone", json=post_putone_msg)
