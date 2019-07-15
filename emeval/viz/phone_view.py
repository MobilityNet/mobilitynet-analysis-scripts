import pandas as pd

import arrow

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
    fig.legend(handles, labels, loc='lower left', mode="expand", ncol=4, bbox_to_anchor=(0,1.02,1,0.2))

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

def get_location_density_df(phone_map):
    density_map = {}
    for phone_label in phone_map:
        curr_phone_density_map = {}
        curr_calibration_ranges = phone_map[phone_label]["calibration_ranges"]
        for r in curr_calibration_ranges:
            density_map[phone_label+"_"+r["trip_id"]] = r["location_df"].hr
        
    density_df = pd.DataFrame(density_map)
    return density_df

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

def display_map_detail_from_df(sel_location_df):
    curr_map = folium.Map()
    latlng_route_coords = list(zip(sel_location_df.latitude, sel_location_df.longitude))
    # print(latlng_route_coords)
    pl = folium.PolyLine(latlng_route_coords)
    pl.add_to(curr_map)
    for i, c in enumerate(latlng_route_coords):
        sl = sel_location_df.iloc[i]
        folium.CircleMarker(c, radius=5, popup="%d: index: %s" % (i, sl[["fmt_time"]])).add_to(curr_map)
    curr_map.fit_bounds(pl.get_bounds())
    return curr_map

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

