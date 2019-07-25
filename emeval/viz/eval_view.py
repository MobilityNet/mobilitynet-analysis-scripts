import pandas as pd
import re

import folium
import folium
import folium.features as fof
import folium.plugins as fpl
import folium.utilities as ful

def get_row_count(n_maps, cols):
    rows = int(n_maps / cols)
    if (n_maps % cols != 0):
        rows = rows + 1
    return rows

def plot_separate_power_drain_multiple_runs(fig, ncols, eval_map, trip_id_pattern):
    nRows = get_row_count(len(eval_map.keys()), ncols)
    all_handles = []
    all_labels = []
    for i, (curr_calibrate, curr_calibrate_trip_map) in enumerate(eval_map.items()): # high_accuracy_train_AO
        # print(curr_calibrate_trip_map.keys())
        if trip_id_pattern not in curr_calibrate:
            print("curr_calibrate = %s, not matching pattern %s, skipping" % (curr_calibrate, trip_id_pattern))
            continue
        ax = fig.add_subplot(nRows, ncols, i+1, title=curr_calibrate)
        for curr_cal_run, cal_phone_map in curr_calibrate_trip_map.items():
            print("Handling data for run %s" % (curr_cal_run))
            # print("Handling data for run %s, %s" % (curr_cal_run, cal_phone_map))
            for phone_label, phone_data_map in cal_phone_map.items():
                # print("Extracting data for %s from map with keys %s" % (phone_label, phone_data_map.keys()))
                battery_df = phone_data_map["battery_df"]
                if len(battery_df) > 0:
                    battery_df.plot(x="hr", y="battery_level_pct", ax=ax, label="%s_%s" % (curr_cal_run.split("_")[-1], phone_label), ylim=(0,100), sharex=True, sharey=True)
                else:
                    print("no battery data found for %s %s, skipping" % (curr_eval, curr_eval_trip_id))

def plot_separate_power_drain_single_run(fig, ncols, eval_map, trip_id_pattern):
    nRows = get_row_count(len(eval_map.keys()), ncols)
    for i, (curr_calibrate, curr_calibrate_trip_map) in enumerate(eval_map.items()): # high_accuracy_train_AO
        if trip_id_pattern not in curr_calibrate:
            print("curr_calibrate = %s, not matching pattern %s, skipping" % (curr_calibrate, trip_id_pattern))
            continue
        ax = fig.add_subplot(nRows, ncols, i+1, title=curr_calibrate)
        for phone_label, phone_data_map in curr_calibrate_trip_map.items():
            print("Extracting data for %s from map with keys %s" % (phone_label, phone_data_map.keys()))
            battery_df = phone_data_map["battery_df"]
            if len(battery_df) > 0:
                battery_df.plot(x="hr", y="battery_level_pct", ax=ax, label=phone_label, ylim=(0,100), sharey=True)
            else:
                print("no battery data found for %s %s, skipping" % (curr_eval, curr_eval_trip_id))

def get_map_list_multiple_runs(eval_view, range_key, trip_id_pattern):
    map_list = []
    color_list = ['blue', 'red', 'purple', 'orange']
    for phoneOS, phone_map in eval_view.map("calibration").items():
        print("Processing data for %s phones" % phoneOS)
        for curr_calibrate, curr_calibrate_trip_map in phone_map.items():
            curr_map = folium.Map()
            all_points = []
            for curr_cal_run, cal_phone_map in curr_calibrate_trip_map.items():
                for i, (phone_label, phone_data_map) in enumerate(cal_phone_map.items()):
                    location_df = phone_data_map["location_df"]
                    latlng_route_coords = list(zip(location_df.latitude, location_df.longitude))
                    all_points.extend(latlng_route_coords)
                    # print(latlng_route_coords[0:10])
                    if len(latlng_route_coords) > 0:
                        print("Processing %s, %s, found %d locations, adding to map" %
                            (curr_calibrate, phone_label, len(latlng_route_coords)))
                        pl = folium.PolyLine(latlng_route_coords,
                            popup="%s" % (phone_label), color=color_list[i])
                        pl.add_to(curr_map)
                    else:
                        print("Processing %s, %s, found %d locations, skipping" %
                            (curr_calibrate, phone_label, len(latlng_route_coords)))
            curr_bounds = ful.get_bounds(all_points)
            print(curr_bounds)
            top_lat = curr_bounds[0][0]
            mid_lng = (curr_bounds[0][1] + curr_bounds[1][1])/2
            print("for trip %s with %d points, midpoint = %s, %s, plotting at %s, %s" %
                    (curr_calibrate, len(all_points), top_lat,mid_lng, top_lat, mid_lng))
            folium.map.Marker(
                [top_lat, mid_lng],
                icon=fof.DivIcon(
                    icon_size=(200,36),
                    html='<div style="font-size: 12pt; color: green;">%s: %s</div>' % (phoneOS, curr_calibrate))
            ).add_to(curr_map)
            curr_map.fit_bounds(pl.get_bounds())
            map_list.append(curr_map)
    return map_list

def get_map_list_single_run(eval_view, range_key, trip_id_pattern):
    map_list = []
    color_list = ['blue', 'red', 'purple', 'orange']
    for phoneOS, phone_map in eval_view.map("calibration").items():
        print("Processing data for %s phones" % phoneOS)
        for curr_calibrate, curr_calibrate_trip_map in phone_map.items():
            if trip_id_pattern not in curr_calibrate:
                print("curr_calibrate = %s, not matching pattern %s, skipping" % (curr_calibrate, trip_id_pattern))
                continue
            curr_map = folium.Map()
            all_points = []
            for i, (phone_label, phone_data_map) in enumerate(curr_calibrate_trip_map.items()):
                print("%d, %s, %s" % (i, phone_label, phone_data_map.keys()))
                location_df = phone_data_map["location_df"]
                latlng_route_coords = list(zip(location_df.latitude, location_df.longitude))
                all_points.extend(latlng_route_coords)
                # print(latlng_route_coords[0:10])
                if len(latlng_route_coords) > 0:
                    print("Processing %s, %s, found %d locations, adding to map" %
                        (curr_calibrate, phone_label, len(latlng_route_coords)))
                    pl = folium.PolyLine(latlng_route_coords,
                        popup="%s" % (phone_label), color=color_list[i])
                    pl.add_to(curr_map)
                else:
                    print("Processing %s, %s, found %d locations, skipping" %
                        (curr_calibrate, phone_label, len(latlng_route_coords)))
            curr_bounds = ful.get_bounds(all_points)
            print(curr_bounds)
            top_lat = curr_bounds[0][0]
            mid_lng = (curr_bounds[0][1] + curr_bounds[1][1])/2
            print("for trip %s with %d points, midpoint = %s, %s, plotting at %s, %s" %
                    (curr_calibrate, len(all_points), top_lat,mid_lng, top_lat, mid_lng))
            folium.map.Marker(
                [top_lat, mid_lng],
                icon=fof.DivIcon(
                    icon_size=(200,36),
                    html='<div style="font-size: 12pt; color: green;">%s: %s</div>' % (phoneOS, curr_calibrate))
            ).add_to(curr_map)
            curr_map.fit_bounds(pl.get_bounds())
            map_list.append(curr_map)
    return map_list

# The compare pattern is a regular expression so that you can do
# HAHFDC|HAMFDC. Others are basic strings, at least for now

def get_map_list_eval_trips(eval_view, os_pattern, trip_id_pattern, compare_pattern):
    compare_pattern_re = re.compile("("+compare_pattern + ")|accuracy_control")
    print(compare_pattern_re)
    map_list = []
    color_list = ['blue', 'red', 'purple', 'orange', "green", "magenta", "gray", "yellow"]
    for phoneOS, phone_map in eval_view.map("evaluation").items():
        print("Processing data for %s phones" % phoneOS)
        if os_pattern not in phoneOS:
            print("pattern %s not found in %s, skipping" % (os_pattern, phoneOS))
            continue

        for curr_eval, curr_eval_trip_map in phone_map.items():
            print("curr_eval = %s" % curr_eval)
            for curr_eval_trip_id, eval_trip_compare_map in curr_eval_trip_map.items():
                if trip_id_pattern not in curr_eval_trip_id:
                    print("pattern %s not found in %s, skipping" %
                        (trip_id_pattern, curr_eval_trip_id))
                    continue
                print("curr_eval_trip_id = %s, creating new map" % curr_eval)
                curr_map = folium.Map()
                all_points = []
                for i, (compare_id, compare_tr) in enumerate(eval_trip_compare_map.items()):
                    # print(i, len(eval_trip_compare_map.items()))
                    # print(compare_pattern_re.search(compare_id))
                    if compare_pattern_re.search(compare_id) is None:
                        print("compare_id = %s, not matching pattern %s, skipping" % (compare_id, compare_pattern))
                        continue
                    if "power_control" in compare_id:
                        print("Skipping the last item (power_control)")
                        continue
                    location_df = compare_tr["location_df"]
                    print("Found %d locations for %s, %s, %s" %
                        (len(location_df), curr_eval, curr_eval_trip_id, compare_id))
                    latlng_route_coords = list(zip(location_df.latitude, location_df.longitude))
                    all_points.extend(latlng_route_coords)
                    # print(latlng_route_coords[0:10])
                    if len(latlng_route_coords) > 0:
                        print("Processing %s, %s, %s, found %d locations, adding to map with color %s" %
                          (curr_eval, curr_eval_trip_id, compare_id, len(latlng_route_coords), color_list[i]))
                        pl = folium.PolyLine(latlng_route_coords,
                            popup="%s" % (compare_id), color=color_list[i])
                        pl.add_to(curr_map)
                    else:
                        print("Processing %s, %s, %s, found %d locations, skipping" %
                          (curr_eval, curr_eval_trip_id, compare_id, len(latlng_route_coords)))

                if len(all_points) > 0:
                    curr_bounds = ful.get_bounds(all_points)
                    print(curr_bounds)
                    top_lat = curr_bounds[0][0]
                    mid_lng = (curr_bounds[0][1] + curr_bounds[1][1])/2
                    print("for trip %s with %d points, midpoint = %s, %s, plotting at %s, %s" %
                          (curr_eval_trip_id, len(all_points), top_lat,mid_lng, top_lat, mid_lng))
                    folium.map.Marker(
                        [top_lat, mid_lng],
                        icon=fof.DivIcon(
                            icon_size=(200,36),
                            html='<div style="font-size: 12pt; color: green;">%s: %s</div>' % (phoneOS, curr_eval_trip_id))
                    ).add_to(curr_map)
                    curr_map.fit_bounds(pl.get_bounds())
                map_list.append(curr_map)
    print("Returning %s" % map_list)
    return map_list
