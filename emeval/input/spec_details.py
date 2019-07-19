# Read and parse the spec details
# More details in __init__.py

import arrow
import requests

class SpecDetails:
    def __init__(self, datastore_url, author_email, spec_id):
        self.DATASTORE_URL = datastore_url
        self.AUTHOR_EMAIL = author_email
        self.CURR_SPEC_ID = spec_id
        self.curr_spec_entry = self.get_current_spec()
        self.populate_spec_details(self.curr_spec_entry)

    def retrieve_data_from_server(self, user_label, key_list, start_ts, end_ts):
        post_msg = {
            "user": user_label,
            "key_list": key_list,
            "start_time": start_ts,
            "end_time": end_ts
        }
        print("About to retrieve messages using %s" % post_msg)
        response = requests.post(self.DATASTORE_URL+"/datastreams/find_entries/timestamp", json=post_msg)
        print("response = %s" % response)
        response.raise_for_status()
        ret_list = response.json()["phone_data"]
        # write_ts may not be the same as data.ts, specially in the case of
        # transitions, where we first generate the data.ts in javascript and
        # then pass it down to the native code to store
        # normally, this doesn't matter because it is a microsecond difference, but
        # it does matter in this case because we store several entries in quick
        # succession and we want to find the entries within a particular range.
        # Putting it into the "data" object makes the write_ts accessible in the
        # subsequent dataframes, etc
        for e in ret_list:
            e["data"]["write_ts"] = e["metadata"]["write_ts"]
        print("Found %d entries" % len(ret_list))
        return ret_list

    def retrieve_all_data_from_server(self, user_label, key_list):
        return self.retrieve_data_from_server(user_label, key_list, 0,
            arrow.get().timestamp)

    def get_current_spec(self):
        all_spec_entry_list = self.retrieve_all_data_from_server(self.AUTHOR_EMAIL, ["config/evaluation_spec"])
        curr_spec_entry = None
        for s in all_spec_entry_list:
            if s["data"]["label"]["id"] == self.CURR_SPEC_ID:
                curr_spec_entry = s
        print("After iterating over %d entries, entry %s" %
            (len(all_spec_entry_list),
            "found" if curr_spec_entry is not None else "not found"))
        return curr_spec_entry

    def populate_spec_details(self, curr_spec_entry):
        self.curr_spec_wrapper = self.curr_spec_entry["data"]
        self.curr_spec = self.curr_spec_wrapper["label"]
        print("Found spec = %s" % self.curr_spec["name"])
        self.eval_start_ts = self.curr_spec_wrapper["start_ts"]
        self.eval_end_ts = self.curr_spec_wrapper["end_ts"]
        self.eval_tz = self.curr_spec["region"]["timezone"]
        print("Evaluation ran from %s -> %s" %
            (arrow.get(self.eval_start_ts).to(self.eval_tz),
             arrow.get(self.eval_end_ts).to(self.eval_tz)))
        self.phone_labels = self.curr_spec["phones"]

