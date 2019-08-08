import argparse

import emeval.input.phone_view as eipv
import emeval.input.spec_details as eisd
import emeval.input.server_stub as eiss

def load_from_spec_id(datastore_url, email, spec_id):
    sd = eisd.SpecDetails(datastore_url, email, spec_id)
    pv = eipv.PhoneView(sd)
    return pv

def save_to_db(db_url, phone_view):
    sd = phone_view.spec_details
    for phone_os, phone_map in pv.map().items():
        print(15 * "=*")
        print(phone_os, phone_map.keys())
        for phone_label, phone_detail_map in phone_map.items():
            print(4 * ' ', 15 * "-*")
            print(4 * ' ', phone_label, phone_detail_map.keys())
            phone_uuid = eiss.register_label(db_url, phone_label)
            print("mapped %s -> %s" % (phone_label, phone_uuid))
            # this spec does not have any calibration ranges, but evaluation ranges are actually cooler
            for r in phone_detail_map["evaluation_ranges"]:
                print(8 * ' ', 30 * "=")
                print(8 * ' ',r.keys())
                print(8 * ' ',r["trip_id"], r["eval_common_trip_id"], r["eval_role"], len(r["evaluation_trip_ranges"]))
                all_entries = r["location_entries"]  + r["filtered_location_entries"] + r["motion_activity_entries"] + r["transition_entries"]
                print("Combining %d locations, %d background locations, %d motion activity, %d transitions -> %d" %
                    (len(r["location_entries"]), len(r["background_location_entries"]), len(r["motion_activity_entries"]), len(r["transition_entries"]), len(all_entries)))
                eiss.post_entries(db_url, phone_label, all_entries)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("datastore_url",
        help="host and port of the public datastore to download data from")
    parser.add_argument("experiment_email",
        help="the email of the experimenter, used to retrieve the specs from the public datastore")
    parser.add_argument("spec_list", metavar='S', type=str, nargs='+',
        help="the list of timeline specs that we should load the data from")
    parser.add_argument("--db_url", default="http://localhost:8080",
        help="database to store the entries to")

    args = parser.parse_args()

    pv_list = [load_from_spec_id(args.datastore_url, args.experiment_email, sid)
        for sid in args.spec_list]
    for pv in pv_list:
        print("Copying data for spec %s" % pv.spec_details.CURR_SPEC_ID)
        save_to_db(args.db_url, pv)
