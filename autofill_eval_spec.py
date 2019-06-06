import argparse
import logging
import json
import copy
import arrow
import requests
import osmapi
import re

import emission.core.wrapper.modeprediction as ecwp
import emission.net.ext_service.routing.osrm as osrm

sensing_configs = json.load(open("sensing_regimes.all.specs.json"))

def validate_and_fill_datetime(current_spec):
    ret_spec = copy.copy(current_spec)
    timezone = current_spec["region"]["timezone"]
    ret_spec["start_ts"] = arrow.get(current_spec["start_fmt_date"], tzinfo=timezone).timestamp
    ret_spec["end_ts"] = arrow.get(current_spec["end_fmt_date"], tzinfo=timezone).timestamp
    return ret_spec

def node_to_geojson_coords(node_id):
    osm = osmapi.OsmApi()
    node_details = osm.NodeGet(node_id)
    return [node_details["lon"], node_details["lat"]]

def get_route_coords(mode, waypoint_coords):
    if mode == ecwp.PredictedModeTypes.CAR \
      or mode == ecwp.PredictedModeTypes.WALKING \
      or mode == ecwp.PredictedModeTypes.BICYCLING:
        # Use OSRM
        overview_geometry_params = {"overview": "full",
            "geometries": "polyline", "steps": "false"}
        route_coords = osrm.get_route_points(mode, waypoint_coords, overview_geometry_params)
        return route_coords
    else:
        raise NotImplementedError("OSRM does not support transit modes at this time")

def _fill_coords_from_id(loc):
    if loc is None:
        return None
    loc["coordinates"] = node_to_geojson_coords(loc["osm_id"])
    return loc["coordinates"]

def validate_and_fill_calibration_tests(curr_spec):
    modified_spec = copy.copy(curr_spec)
    calibration_tests = modified_spec["calibration_tests"]
    for t in calibration_tests:
        _fill_coords_from_id(t["start_loc"])
        _fill_coords_from_id(t["end_loc"])
        t["config"] = sensing_configs[t["config"]["id"]]
    return modified_spec

def coords_swap(lon_lat):
    return list(reversed(lon_lat))

def validate_and_fill_eval_trips(curr_spec):
    modified_spec = copy.copy(curr_spec)
    eval_trips = modified_spec["evaluation_trips"]
    for t in eval_trips:
        start_coords = _fill_coords_from_id(t["start_loc"])
        end_coords = _fill_coords_from_id(t["end_loc"])
        waypoints = t["route_waypoints"]
        waypoint_coords = [node_to_geojson_coords(node_id) for node_id in waypoints]
        logging.debug("waypoint_coords = %s..." % waypoint_coords[0:3])
        route_coords = get_route_coords(ecwp.PredictedModeTypes[t["mode"]],
            [start_coords] + waypoint_coords + [end_coords])
        t["route_coords"] = [coords_swap(rc) for rc in route_coords]
    return modified_spec

def validate_and_fill_sensing_settings(curr_spec):
    modified_spec = copy.copy(curr_spec)
    for ss in modified_spec["sensing_settings"]:
        compare_list = ss["compare"]
        ss["name"] = " v/s ".join(compare_list)
        ss["sensing_configs"] = [sensing_configs[cr] for cr in compare_list]
    return modified_spec

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(prog="autofill_eval_spec")

    parser.add_argument("in_spec_file", help="file to autofill")
    parser.add_argument("out_spec_file", help="autofilled version of in_spec_file")

    args = parser.parse_args()

    print("Reading input from %s" % args.in_spec_file) 
    current_spec = json.load(open(args.in_spec_file))

    dt_spec = validate_and_fill_datetime(current_spec)
    calib_spec = validate_and_fill_calibration_tests(dt_spec)
    eval_spec = validate_and_fill_eval_trips(calib_spec)
    settings_spec = validate_and_fill_sensing_settings(eval_spec)
   
    print("Writing output to %s" % args.out_spec_file) 
    json.dump(settings_spec, open(args.out_spec_file, "w"), indent=2)
