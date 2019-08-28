import pandas as pd
import numpy as np

import arrow
import geojson as gj

import matplotlib.cm as mcm
import matplotlib.pyplot as plt

import folium
import folium
import folium.features as fof
import folium.plugins as fpl
import folium.utilities as ful

def lonlat_swap(lon_lat):
    return list(reversed(lon_lat))

def get_row_count(n_maps, cols):
    rows = int(n_maps / cols)
    if (n_maps % cols != 0):
        rows = rows + 1
    return rows

"""
Inputs:
ax: matplotlib axes to display the curves
phone_map for a particular OS: value for "android" or "ios" from phone view
range_key: "calibration"/"evaluation"
trip_id_pattern: "stationary" for stationary calibration, "AO" for moving calibration,...
"""
def plot_all_power_drain(ax, phone_map, range_key, trip_id_pattern):
    for phone_label in phone_map:
        curr_calibration_ranges = phone_map[phone_label]["{}_ranges".format(range_key)]
        sel_calibration_ranges = [cr for cr in curr_calibration_ranges if trip_id_pattern in cr["trip_id"]]
        for r in sel_calibration_ranges:
            battery_df = r["battery_df"]
            ret_axes = battery_df.plot(x="hr", y="battery_level_pct", ax=ax, label=phone_label+"_"+r["trip_id"], ylim=(0,100), sharey=True)

def plot_collapsed_all_power_drain(ax, phone_map, range_key, trip_id_pattern,
    curr_color_map, curr_legend_map):
    for phone_label in phone_map:
        curr_calibration_ranges = phone_map[phone_label]["{}_ranges".format(range_key)]
        sel_calibration_ranges = [cr for cr in curr_calibration_ranges if trip_id_pattern in cr["trip_id"]]
        for r in sel_calibration_ranges:
            battery_df = r["battery_df"]
            tidb = r["trip_id_base"]
            if tidb not in curr_color_map:
                # assign to the next color
                curr_color_map[tidb] = mcm.tab10.colors[len(curr_color_map)]
                new_type = True

            ret_line = battery_df.plot(x="hr", y="battery_level_pct",
                ax=ax, legend=False, ylim=(0,100),
                color=curr_color_map[tidb], sharey=True).lines[-1]

            if tidb not in curr_legend_map:
                curr_legend_map[tidb] = ret_line

    return (curr_color_map, curr_legend_map)


def plot_separate_power_drain(fig, phone_map, ncols, range_key, trip_id_pattern):
    nRows = get_row_count(len(phone_map.keys()), ncols)
    print("Printing %d nRows for %s, pattern %s" % (nRows, range_key, trip_id_pattern))
    for i, phone_label in enumerate(phone_map.keys()):
        ax = fig.add_subplot(nRows, ncols, i+1, title=phone_label)
        curr_calibration_ranges = phone_map[phone_label]["{}_ranges".format(range_key)]
        sel_calibration_ranges = [cr for cr in curr_calibration_ranges if trip_id_pattern in cr["trip_id"]]
        for r in sel_calibration_ranges:
            battery_df = r["battery_df"]
            # print(battery_df.battery_level_pct.tolist())
            battery_df.plot(x="hr", y="battery_level_pct", ax=ax, label=r["trip_id"], ylim=(0,100), sharex=True, sharey=True, legend=False)
    # from https://stackoverflow.com/a/46921590/4040267
    handles, labels = ax.get_legend_handles_labels()
    fig.legend(handles, labels, loc='lower left', mode="expand", ncol=2, bbox_to_anchor=(0,1.02,1,0.2))

# Get the counts for the various calibration ranges
def get_count_df(phone_view):
    count_map = {}
    for phoneOS, phone_map in phone_view.map().items():
        print("Processing data for %s phones" % phoneOS)
        for phone_label in phone_map:
            curr_phone_count_map = {}
            curr_calibration_ranges = phone_map[phone_label]["calibration_ranges"]
            for r in curr_calibration_ranges:
                curr_phone_count_map[r["trip_id"]] = len(r["location_df"])
            count_map[phoneOS+"_"+phone_label] = curr_phone_count_map
            
    count_df = pd.DataFrame(count_map).transpose()
    return count_df

def get_location_density_df(phone_map, range_key):
    density_map = {}
    for phone_label in phone_map:
        curr_phone_density_map = {}
        curr_calibration_ranges = phone_map[phone_label]["{}_ranges".format(range_key)]
        for r in curr_calibration_ranges:
            if "hr" in r["location_df"]:
                density_map[phone_label+"_"+r["trip_id"]] = r["location_df"].hr
        
    density_df = pd.DataFrame(density_map)
    filtered_density_df = density_df.loc[:,~density_df.columns.str.contains("POWER_CONTROL")]
    return filtered_density_df

"""
Filter out invalid locations that mess up the density dataframe
Invalid is defined as points way before the related range.
Returns a filtered density dataframe
"""

def filter_density_df(density_df, invalid_threshold = 0):
    invalid_point_mask = np.full(len(density_df), False)
    for col in density_df:
        col_series = density_df[col]
        before_points = col_series < invalid_threshold # 6 minutes
        if np.count_nonzero(np.array(before_points)) > 0:
            print(col, col_series[before_points])
            invalid_point_mask = invalid_point_mask | before_points
    filtered_df = density_df[np.logical_not(invalid_point_mask)]
    for col in filtered_df:
        col_series = filtered_df[col]
        # print("nonna length for %s = %d" % (col, len(col_series.dropna())))
        if len(col_series.dropna()) <= 3: # we need at least two points to show density
            filtered_df = filtered_df.drop(col, axis=1)
    return filtered_df

def plot_separate_density_curves(fig, phone_map, ncols, range_key, trip_id_pattern):
    nRows = get_row_count(len(phone_map.keys()), ncols)
    print("Printing %d nRows for %s, pattern %s" % (nRows, range_key, trip_id_pattern))
    for i, phone_label in enumerate(phone_map.keys()):
        ax = fig.add_subplot(nRows, ncols, i+1, title=phone_label)
        curr_calibration_ranges = phone_map[phone_label]["{}_ranges".format(range_key)]
        sel_calibration_ranges = [cr for cr in curr_calibration_ranges if trip_id_pattern in cr["trip_id"]]
        for r in sel_calibration_ranges:
            location_df = r["location_df"]
            # print(battery_df.battery_level_pct.tolist())
            location_df.hr.plot(kind='density', ax=ax, label=r["trip_id"], sharex=True, sharey=True)
            # ax.legend()
    # from https://stackoverflow.com/a/46921590/4040267
    handles, labels = ax.get_legend_handles_labels()
    fig.legend(handles, labels, loc='lower left', mode="expand", ncol=4, bbox_to_anchor=(0,1.02,1,0.2))

def get_map_list(phone_view, range_key, trip_id_pattern):
    map_list = []
    for phoneOS, phone_map in phone_view.map().items():
        print("Processing data for %s phones" % phoneOS)
        for phone_label in phone_map:
            curr_calibration_ranges = phone_map[phone_label]["{}_ranges".format(range_key)]
            sel_calibration_ranges = [cr for cr in curr_calibration_ranges if trip_id_pattern in cr["trip_id"]]
            for r in sel_calibration_ranges:
                curr_map = folium.Map()
                location_df = r["location_df"]
                latlng_route_coords = list(zip(location_df.latitude, location_df.longitude))
                # print(latlng_route_coords[0:10])
                pl = folium.PolyLine(latlng_route_coords,
                    popup="%s: %s" % (phoneOS, phone_label))
                pl.add_to(curr_map)
                curr_bounds = pl.get_bounds()
                print(curr_bounds)
                top_lat = curr_bounds[0][0]
                mid_lng = (curr_bounds[0][1] + curr_bounds[1][1])/2
                print("midpoint = %s, %s, plotting at %s, %s" % (top_lat,mid_lng, top_lat, mid_lng))
                folium.map.Marker(
                    [top_lat, mid_lng],
                    icon=fof.DivIcon(
                        icon_size=(200,36),
                        html='<div style="font-size: 12pt; color: green;">%s %s</div>' % (phone_label, r["trip_id"]))
                ).add_to(curr_map)
                curr_map.fit_bounds(pl.get_bounds())
                map_list.append(curr_map)
    return map_list

def display_map_detail_from_df(sel_location_df, tz="UTC", sticky_popups=False):
    curr_map = folium.Map()
    latlng_route_coords = list(zip(sel_location_df.latitude, sel_location_df.longitude))
    # print(latlng_route_coords)
    pl = folium.PolyLine(latlng_route_coords)
    pl.add_to(curr_map)
    for i, c in enumerate(latlng_route_coords):
        sl = sel_location_df.iloc[i]
        if sticky_popups:
            cm = folium.CircleMarker(c, radius=5)
            folium.Popup("%s: %s accuracy: %s" % (sl.name, arrow.get(sl.ts).to(tz).format("YYYY-MMM-DD HH:mm"), sl.accuracy), show=True, sticky=True).add_to(cm)
        else:
            cm = folium.CircleMarker(c, radius=5, popup="%d: index: %s" % (i, sl[["fmt_time"]]))
        cm.add_to(curr_map)
    curr_map.fit_bounds(pl.get_bounds())
    return curr_map

def get_geojson_for_leg(sensed_section, color="red"):
    location_df = sensed_section["location_df"]
    lonlat_route_coords = list(zip(location_df.longitude, location_df.latitude))
    return gj.Feature(geometry=gj.LineString(lonlat_route_coords),
        properties={"style": {"color": color}, "ts": list(location_df.ts)})

def get_point_markers(linestring_gj, name="points", tz="UTC", **kwargs):
    fg = folium.FeatureGroup(name)
    if "properties" in linestring_gj and "ts" in linestring_gj["properties"]:
        ts_list = linestring_gj["properties"]["ts"]
    else:
        ts_list = None
    for i, c in enumerate(linestring_gj["geometry"]["coordinates"]):
        if ts_list is not None:
            ts = ts_list[i]
            folium.CircleMarker(lonlat_swap(c), radius=5, popup="%d: %s, %s" % (i, c, arrow.get(ts).to(tz).format("YYYY-MM-DD HH:mm:ss")), **kwargs).add_to(fg)
        else:
            folium.CircleMarker(lonlat_swap(c), radius=5, popup="%d: %s" % (i, c), **kwargs).add_to(fg)
    return fg

def print_entry(e, metadata_field_list, data_field_list, tz):
    entry_display = []
    for mf in metadata_field_list:
        if mf == "fmt_time":
            entry_display.append(arrow.get(e["metadata"]["write_ts"]).to(tz))
        else:
            entry_display.append(e["metadata"][mf])
    for df in data_field_list:
        if df == "fmt_time":
            entry_display.append(arrow.get(e["data"]["ts"]).to(tz))
        else:
            entry_display.append(e["data"][df])
    return entry_display

def display_unprocessed_android_activity_transitions(phone_view, ax, range_key, trip_id_pattern):
    for phone_label, phone_map in phone_view.map()["android"].items():
        for r in phone_map["{}_ranges".format(range_key)]:
            if trip_id_pattern not in r["trip_id"]:
                # print("%s does not match pattern %s, skipping" % (r["trip_id"], trip_id_pattern))
                continue
            # print(20 * "-", phone_label, r["trip_id"], 20 * "-")
            ma_valid_motion = r["motion_activity_df"].query("zzbhB not in [3,4,5]")
            # print("For %s, %s, filtered %d -> %d" % (phone_label, r["trip_id"], len(r["motion_activity_df"]), len(ma_valid_motion)))
            # ma_changes = ma_valid_motion.zzbhB.diff() != 0;
            # ma_transitions = ma_valid_motion[ma_changes]
            ma_valid_motion.plot(x="hr", y="zzbhB", ax=ax, label="%s_%s" % (phone_label, r["trip_id"]))

def display_unprocessed_ios_activity_transitions(phone_view, ax, range_key, trip_id_pattern):
    for phone_label, phone_map in phone_view.map()["ios"].items():
        for r in phone_map["{}_ranges".format(range_key)]:
            if trip_id_pattern not in r["trip_id"]:
                # print("%s does not match pattern %s, skipping" % (r["trip_id"], trip_id_pattern))
                continue
            # print(20 * "-", phone_label, r["trip_id"], 20 * "-")
            ma_valid_motion = r["motion_activity_df"].query("automotive == True | cycling == True | running == True | walking == True")
            # print("For %s, %s, filtered %d -> %d" % (phone_label, r["trip_id"], len(r["motion_activity_df"]), len(ma_valid_motion)))
            ma_valid_motion.plot(x="hr", y="automotive", ax=ax, label="%s_%s" % (phone_label, r["trip_id"]))

