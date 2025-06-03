# Read and parse the spec details
# More details in __init__.py

import arrow
import time
import requests
import shapely as shp
import geojson as gj
from abc import ABC, abstractmethod
import os
import json
import sys
import math


class SpecDetails(ABC):
    def __init__(self, datastore_loc, author_email, spec_id=None):
        self.DATASTORE_LOC = datastore_loc
        self.AUTHOR_EMAIL = author_email
        # make spec_id optional if instance is only being used to call retrieve_data
        if spec_id:
            self.CURR_SPEC_ID = spec_id
            self.curr_spec_entry = self.get_current_spec()
            self.populate_spec_details(self.curr_spec_entry)

    @abstractmethod
    def get_all_spec_ids(self):
        pass

    @abstractmethod
    def retrieve_data(self, user, key_list, start_ts, end_ts):
        pass

    def retrieve_all_data(self, user, key_list):
        # sys.maxsize is used for end_ts as opposed to arrow.get().timestamp (the current timestamp)
        # as it is a constant. This is to ensure that a FileSpecDetails instance can find data with
        # the largest specified time range.
        return self.retrieve_data(user, key_list, 0, sys.maxsize)

    def get_current_spec(self):
        all_spec_entry_list = self.retrieve_all_data(self.AUTHOR_EMAIL, ["config/evaluation_spec"])
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

    def get_ground_truth_for_trip(self, trip_id, start_ts, end_ts):
        tl = [t for t in self.curr_spec_entry["data"]["label"]["evaluation_trips"]
              if t["id"] == trip_id]
        assert len(tl) == 1
        tl = tl[0]

        for leg in tl["legs"]:
            for key in ["loc", "start_loc", "end_loc", "route_coords"]:
                if key in leg and isinstance(leg[key], list):
                    within_ts = [x for x in leg[key]
                                 if start_ts >= x["properties"]["valid_start_ts"]
                                 and end_ts <= x["properties"]["valid_end_ts"]]
                    assert len(within_ts) == 1, f"Invalid amount of {key} info for {leg['id']} between timestamps {start_ts} -> {end_ts}"
                    leg[key] = within_ts[0]

        return tl

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

    def get_ground_truth_for_leg(self, trip_id, leg_id, start_ts, end_ts):
        # print(f"GT_FOR_LEG: Called `get_ground_truth_for_leg` with {trip_id}, {leg_id}, {start_ts}, {end_ts}")
        for t in self.curr_spec_entry["data"]["label"]["evaluation_trips"]:
            if t["id"] == trip_id:
                ll = [l for l in t["legs"] if l["id"] == leg_id]
                # print("GT_FOR_LEG:", leg_id, len(ll), [l["id"] for l in ll])
                if len(ll) == 1:
                    sel_leg = ll[0]
                    ret_leg = sel_leg.copy()
                    # print(f"GT_FOR_LEG: {sel_leg['id']=}")
                    for key in ["loc", "start_loc", "end_loc", "route_coords"]:
                        # print(f"GT_FOR_LEG: Checking {key=} in object {sel_leg.keys()=} = {key in sel_leg}")
                        # if key in sel_leg:
                        #     print(f"GT_FOR_LEG: Checking key type = {isinstance(sel_leg[key], list)}")
                        if key in sel_leg and isinstance(sel_leg[key], list):
                            # print(f"GT_FOR_LEG: Found matching list of size {len(sel_leg[key])} for {key} in {sel_leg['id']}")
                            within_ts = [x for x in sel_leg[key]
                                         if start_ts >= x["properties"]["valid_start_ts"]
                                         and end_ts <= x["properties"]["valid_end_ts"]]
                            assert len(within_ts) == 1, f"Invalid amount of {key} info for {sel_leg['id']} between timestamps {start_ts} -> {end_ts}"
                            # we want to copy before returning because
                            # otherwise the first call to this function
                            # overwrites the ground truth in the full spec and
                            # makes it a entry instead of a list.
                            # we can never get the second ground truth in that case
                            ret_leg[key] = within_ts[0]
                    return ret_leg

    @staticmethod
    def get_shapes_for_leg(gt_leg):
        if gt_leg["type"] == "TRAVEL":
            return {
                "start_loc": shp.geometry.shape(gt_leg["start_loc"]["geometry"]),
                "end_loc": shp.geometry.shape(gt_leg["end_loc"]["geometry"]),
                "route": shp.geometry.shape(gt_leg["route_coords"]["geometry"])
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


class ServerSpecDetails(SpecDetails):
    def get_all_spec_ids(self):
        spec_data = self.retrieve_one_batch(self.AUTHOR_EMAIL,
            ["config/evaluation_spec"],
            0, sys.maxsize)
        spec_ids = [s["data"]["label"]["id"] for s in spec_data]

        return set(spec_ids)

    def retrieve_one_batch(self, user, key_list, start_ts, end_ts, key_time="metadata.write_ts"):
        post_body = {
            "user": user,
            "key_list": key_list,
            "key_time": key_time,
            "start_time": start_ts,
            "end_time": end_ts
        }

        print(f"Retrieving data for: {post_body=}")
        try:
            response = requests.post(f"{self.DATASTORE_LOC}/datastreams/find_entries/timestamp", json=post_body)
            print(f"{response=}")
            response.raise_for_status()
            data = response.json()["phone_data"]
        except Exception as e:
            print(f"Got {type(e).__name__}: {e}, retrying...")
            time.sleep(10)
            response = requests.post(f"{self.DATASTORE_LOC}/datastreams/find_entries/timestamp", json=post_body)
            print(f"{response=}")
            response.raise_for_status()
            data = response.json()["phone_data"]

        for e in data:
            e["data"]["write_ts"] = e["metadata"]["write_ts"]

        print(f"Found {len(data)} entries")
        return data

    def retrieve_data(self, user, key_list, start_ts, end_ts, key_time="metadata.write_ts"):
        all_done = False
        location_entries = []
        curr_start_ts = start_ts
        prev_retrieved_count = 0

        while not all_done:
            print("Retrieving data for %s from %s -> %s" % (user, curr_start_ts, end_ts))
            curr_location_entries = self.retrieve_one_batch(user, key_list, curr_start_ts, end_ts, key_time)
            #print("Retrieved %d entries with timestamps %s..." % (len(curr_location_entries), [cle["data"]["ts"] for cle in curr_location_entries[0:10]]))
            if len(curr_location_entries) == 0 or len(curr_location_entries) == 1:
                # we have only one entry in response to the query
                # so we set the location entries to it
                # otherwise, we will not return anything in this case
                # https://github.com/MobilityNet/mobilitynet.github.io/issues/31#issuecomment-1345965784
                if len(location_entries) == 0 and len(curr_location_entries) == 1:
                    location_entries = curr_location_entries
                all_done = True
            else:
                location_entries.extend(curr_location_entries)
                key_time_split = key_time.split(".")
                new_start_ts = curr_location_entries[-1][key_time_split[0]][key_time_split[1]]
                assert new_start_ts > curr_start_ts
                curr_start_ts = new_start_ts
                prev_retrieved_count = len(curr_location_entries)
        return location_entries


class FileSpecDetails(SpecDetails):
    def get_all_spec_ids(self):
        spec_dir = os.path.join(os.getcwd(), self.DATASTORE_LOC, self.AUTHOR_EMAIL)
        all_specs = os.listdir(spec_dir) 
        print(all_specs)
        return all_specs

    def retrieve_data(self, user, key_list, start_ts, end_ts):
        data = []
        for key in key_list:
            data_file = os.path.join(
                os.getcwd(),
                self.DATASTORE_LOC,
                f"{user}/{self.CURR_SPEC_ID}/{key.replace('/', '~')}/{math.floor(start_ts)}_{math.ceil(end_ts)}.json")
            assert os.path.isfile(data_file), f"not found: {data_file=}"
            with open(data_file, "r") as f:
                d = json.load(f)
                if isinstance(d, list):
                    data.extend(d)
                else:
                    data.append(d)
        return data
