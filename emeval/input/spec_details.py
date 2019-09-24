# Read and parse the spec details
# More details in __init__.py

import arrow
import time
import requests
import shapely as shp
import geojson as gj

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
        try:
            response = requests.post(self.DATASTORE_URL+"/datastreams/find_entries/timestamp", json=post_msg)
            print("response = %s" % response)
            response.raise_for_status()
            ret_list = response.json()["phone_data"]
        except Exception as e:
            print("Got %s error %s, retrying" % (type(e).__name__, e))
            time.sleep(10)
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

    def fmt(self, ts, fmt_string):
        return arrow.get(ts).to(self.eval_tz).format(fmt_string)

    def get_ground_truth_for_trip(self, trip_id):
        tl = [t for t in self.curr_spec_entry["data"]["label"]["evaluation_trips"] if t["id"] == trip_id]
        print(trip_id, len(tl), [t["id"] for t in tl])
        assert len(tl) == 1
        return tl[0]

    @staticmethod
    def get_concat_trajectories(trip):
        coords_list = []
        modes_list = []
        for l in trip["legs"]:
            if l["type"] == "TRAVEL":
                coords_list.extend(l["route_coords"]["geometry"]["coordinates"])
                modes_list.append(l["mode"])
        return gj.Feature(geometry=gj.LineString(coords_list),
            properties={"modes": modes_list})

    def get_ground_truth_for_leg(self, trip_id, leg_id):
        for t in self.curr_spec_entry["data"]["label"]["evaluation_trips"]:
            if t["id"] == trip_id:
                ll = [l for l in t["legs"] if l["id"] == leg_id]
                # print(leg_id, len(ll), [l["id"] for l in ll])
                if len(ll) == 1:
                    return ll[0]

    @staticmethod
    def get_shapes_for_leg(gt_leg):
        if gt_leg["type"] == "TRAVEL":
            return {
                "start_loc": shp.geometry.shape(gt_leg["start_loc"]["geometry"]),
                "end_loc": shp.geometry.shape(gt_leg["end_loc"]["geometry"]),
                "route": shp.geometry.shape(gt_leg['route_coords']['geometry'])
            }
        else:
            return {"loc": shp.geometry.shape(gt_leg["loc"]["geometry"])}


    @classmethod
    def get_geojson_for_leg(cls, gt_leg):
        if gt_leg["type"] == "TRAVEL":
            gt_leg["route_coords"]["properties"]["style"] = {"color": "green"}
            gt_leg["start_loc"]["properties"]["style"] = {"color": "LightGreen", "fillColor": "LightGreen"}
            gt_leg["end_loc"]["properties"]["style"] = {"color": "red", "fillColor": "red"}
            return gj.FeatureCollection([gt_leg["start_loc"], gt_leg["end_loc"],
                gt_leg["route_coords"]])
        else:
            gt_leg["loc"]["properties"]["style"] = {"color": "purple", "fillColor": "purple"}
            return gt_leg["loc"]
        
