import pandas as pd

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
        if trip_id_pattern not in curr_calibrate:
            print("curr_calibrate = %s, not matching pattern %s, skipping" % (curr_calibrate, trip_id_pattern))
            continue
        ax = fig.add_subplot(nRows, ncols, i+1, title=curr_calibrate)
        for curr_cal_run, cal_phone_map in curr_calibrate_trip_map.items():
            # print("Handling data for run %s" % (curr_cal_run))
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
    for phoneOS, phone_map in eval_view.map().items():
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
    for phoneOS, phone_map in eval_view.map().items():
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
