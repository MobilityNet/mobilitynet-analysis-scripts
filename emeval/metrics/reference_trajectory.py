import scipy.interpolate as sci
import geopandas as gpd
import shapely as shp
import random as random
import math
import arrow
import pandas as pd
import functools
import traceback

import emeval.metrics.dist_calculations as emd
import emeval.input.spec_details as eisd

random.seed(1)

####
# BEGIN: Building blocks of the final implementations
####

####
# BEGIN: NORMALIZATION
####

# In addition to filtering the sensed values in the polygons, we should also
# really filter the ground truth values in the polygons, since there is no
# ground truth within the polygon However, ground truth points are not known to
# be dense, and in some cases (e.g. commuter_rail_aboveground), there is a
# small gap between the polygon border and the first point outside it. We
# currently ignore this distance

def fill_gt_linestring(e):
    section_gt_shapes = gpd.GeoSeries(eisd.SpecDetails.get_shapes_for_leg(e["ground_truth"]["leg"]))
    e["ground_truth"]["gt_shapes"] = section_gt_shapes
    e["ground_truth"]["linestring"] = emd.filter_ground_truth_linestring(e["ground_truth"]["gt_shapes"])
    e["ground_truth"]["utm_gt_shapes"] = section_gt_shapes.apply(lambda s: shp.ops.transform(emd.to_utm_coords, s))
    e["ground_truth"]["utm_linestring"] = emd.filter_ground_truth_linestring(e["ground_truth"]["utm_gt_shapes"])

def to_gpdf(location_df):
    return gpd.GeoDataFrame(
        location_df, geometry=location_df.apply(
            lambda lr: shp.geometry.Point(lr.longitude, lr.latitude), axis=1))

def get_int_aligned_trajectory(location_df, tz="UTC"):
    lat_fn = sci.interp1d(x=location_df.ts, y=location_df.latitude)
    lon_fn = sci.interp1d(x=location_df.ts, y=location_df.longitude)
    # In order to avoid extrapolation, we use ceil for the first int and floor
    # for the last int
    first_int_ts = math.ceil(location_df.ts.iloc[0])
    last_int_ts = math.floor(location_df.ts.iloc[-1])
    new_ts_range = [float(ts) for ts in range(first_int_ts, last_int_ts, 1)]
    new_fmt_time_range = [arrow.get(ts).to(tz) for ts in new_ts_range]
    new_lat = lat_fn(new_ts_range)
    new_lng = lon_fn(new_ts_range)
    new_gpdf = gpd.GeoDataFrame({
        "latitude": new_lat,
        "longitude": new_lng,
        "ts": new_ts_range,
        "fmt_time": new_fmt_time_range,
        "geometry": [shp.geometry.Point(x, y) for x, y in zip(new_lng, new_lat)]
    })
    return new_gpdf

####
# END: NORMALIZATION
####

####
# BEGIN: DISTANCE CALCULATION
####

def add_gt_error_projection(location_gpdf, gt_linestring):
    location_gpdf["gt_distance"] = location_gpdf.distance(gt_linestring)
    location_gpdf["gt_projection"] = location_gpdf.geometry.apply(
        lambda p: gt_linestring.project(p))

def add_t_error(location_gpdf_a, location_gpdf_b):
    location_gpdf_a["t_distance"] = location_gpdf_a.distance(location_gpdf_b)
    location_gpdf_b["t_distance"] = location_gpdf_a.t_distance

def add_self_project(location_gpdf_a):
    loc_linestring = shp.geometry.LineString(coordinates=list(zip(
        location_gpdf.longitude, location_gdpf.latitude)))
    location_gpdf["s_projection"] = location_gpdf.geometry.apply(
        lambda p: loc_linestring.project(p))

####
# END: DISTANCE CALCULATION
####

####
# BEGIN: MERGE
####

# Assumes both entries exist
def b_merge_midpoint(loc_row):
    # print("merging %s" % loc_row)
    assert not pd.isnull(loc_row.geometry_i) and not pd.isnull(loc_row.geometry_a)
    midpoint = shp.geometry.LineString(coordinates=[loc_row.geometry_a, loc_row.geometry_i]).interpolate(0.5, normalized=True)
    # print(midpoint)
    final_geom = (midpoint, "midpoint")
    return final_geom

def b_merge_random(loc_row):
    # print("merging %s" % loc_row)
    assert not pd.isnull(loc_row.geometry_i) and not pd.isnull(loc_row.geometry_a)
    r_idx = random.choice(["geometry_a","geometry_i"])
    rp = loc_row[r_idx]
    # print(midpoint)
    final_geom = (rp, r_idx)
    return final_geom

def b_merge_closer_gt_dist(loc_row):
    # print("merging %s" % loc_row)
    assert not pd.isnull(loc_row.geometry_i) and not pd.isnull(loc_row.geometry_a)
    if loc_row.gt_distance_a < loc_row.gt_distance_i:
        final_geom = (loc_row.geometry_a, "android")
    else:
        final_geom = (loc_row.geometry_i, "ios")
    return final_geom

def b_merge_closer_gt_proj(loc_row):
    # print("merging %s" % loc_row)
    assert not pd.isnull(loc_row.geometry_i) and not pd.isnull(loc_row.geometry_a)
    if loc_row.gt_projection_a < loc_row.gt_projection_i:
        final_geom = (loc_row.geometry_a, "android")
    else:
        final_geom = (loc_row.geometry_i, "ios")
    return final_geom

def collapse_inner_join(loc_row, b_merge_fn):
    """
    Collapse a merged row. The merge was through inner join so both sides are
    known to exist
    """
    final_geom, source = b_merge_fn(loc_row)
    return {
        "ts": loc_row.ts,
        "longitude": final_geom.x,
        "latitude": final_geom.y,
        "geometry": final_geom,
        "source": source
    }

def collapse_outer_join_stateless(loc_row, b_merge_fn):
    """
    Collapse a merged row through outer join. This means that we can have
    either the left side or the right side, or both.
    - If only one side exists, we use it.
    - If both sides exist, we merge using `b_merge_fn`
    """
    source = None
    if pd.isnull(loc_row.geometry_i):
        assert not pd.isnull(loc_row.geometry_a)
        final_geom = loc_row.geometry_a
        source = "android"
    elif pd.isnull(loc_row.geometry_a):
        assert not pd.isnull(loc_row.geometry_i)
        final_geom = loc_row.geometry_i
        source = "ios"
    else:
        final_geom, source = b_merge_fn(loc_row)
    return {
        "ts": loc_row.ts,
        "longitude": final_geom.x,
        "latitude": final_geom.y,
        "geometry": final_geom,
        "source": source
    }

def collapse_outer_join_dist_so_far(loc_row, more_details_fn = None):
    """
    Collapse a merged row through outer join. This means that we can have
    either the left side or the right side, or both. In this case, we also
    want to make sure that the trajectory state is "progressing". In this only
    current implementation, we check that the distance along the ground truth
    trajectory is progressively increasing.  Since this can be complex to debug,
    the `more_details` function returns `True` for rows for which we need more
    details of the computation.
    """
    global distance_so_far

    source = None
    more_details = False
    EMPTY_POINT = shp.geometry.Point()

    if more_details_fn is not None and more_details_fn(loc_row):
        more_details = True

    if more_details:
        print(loc_row.gt_projection_a, loc_row.gt_projection_i)
    if pd.isnull(loc_row.geometry_i):
        assert not pd.isnull(loc_row.geometry_a)
        if loc_row.gt_projection_a > distance_so_far:
            final_geom = loc_row.geometry_a
            source = "android"
        else:
            final_geom = EMPTY_POINT
    elif pd.isnull(loc_row.geometry_a):
        assert not pd.isnull(loc_row.geometry_i)
        if loc_row.gt_projection_i > distance_so_far:
            final_geom = loc_row.geometry_i
            source = "ios"
        else:
            final_geom = EMPTY_POINT
    else:
        assert not pd.isnull(loc_row.geometry_i) and not pd.isnull(loc_row.geometry_a)
        choice_series = gpd.GeoSeries([loc_row.geometry_a, loc_row.geometry_i])
        gt_projection_line_series = gpd.GeoSeries([loc_row.gt_projection_a, loc_row.gt_projection_i])
        if more_details:
            print("gt_projection_line = %s" % gt_projection_line_series)
        distance_from_last_series = gt_projection_line_series.apply(lambda d: d - distance_so_far)
        if more_details:
            print("distance_from_last_series = %s" % distance_from_last_series)

        # assert not (distance_from_last_series < 0).all(), "distance_so_far = %s, distance_from_last = %s" % (distance_so_far, distance_from_last_series)
        if (distance_from_last_series < 0).all():
            if more_details:
                print("all distances are negative, skipping...")
            final_geom = EMPTY_POINT
        else:
            if (distance_from_last_series < 0).any():
                # avoid going backwards along the linestring (wonder how this works with San Jose u-turn)
                closer_idx = distance_from_last_series.idxmax()
                if more_details:
                    print("one distance is going backwards, found closer_idx = %d" % closer_idx)

            else:
                distance_from_gt_series = gpd.GeoSeries([loc_row.gt_distance_a, loc_row.gt_distance_i])
                if more_details:
                    print("distance_from_gt_series = %s" % distance_from_gt_series)
                closer_idx = distance_from_gt_series.idxmin()
                if more_details:
                    print("both distances are positive, found closer_idx = %d" % closer_idx)

            if closer_idx == 0:
                source = "android"
            else:
                source = "ios"
            final_geom = choice_series.loc[closer_idx]

    if final_geom != EMPTY_POINT:
        if source == "android":
            distance_so_far = loc_row.gt_projection_a
        else:
            assert source == "ios"
            distance_so_far = loc_row.gt_projection_i
        
    if more_details:
        print("final_geom = %s, new_distance_so_far = %s" % (final_geom, distance_so_far))
    if final_geom == EMPTY_POINT:
        return {
            "ts": loc_row.ts,
            "longitude": pd.np.nan,
            "latitude": pd.np.nan,
            "geometry": EMPTY_POINT,
            "source": source
        }
    else:
        return {
            "ts": loc_row.ts,
            "longitude": final_geom.x,
            "latitude": final_geom.y,
            "geometry": final_geom,
            "source": source
        }

####
# END: MERGE
####

####
# END: Building blocks of the final implementations
####

####
# BEGIN: Combining into actual reference constructions
####

def ref_ends(e, dist_threshold, tz="UTC"):
    # This is only called from ref_ct_general and ref_gt_general, so 
    # the emd filter method adds the `outside_polygon` field to the input dataframe.
    # when we look for separate input and output polygons, we don't want to
    # mess up the other `outside_polygon` fields, so let's make copies
    # everywhere

    utm_gt_linestring = e["ground_truth"]["utm_linestring"]
    section_gt_shapes = e["ground_truth"]["gt_shapes"]

    def _get_filtered_loc(gt_key):
        unfiltered_loc_a_df = emd.to_geo_df(e["temporal_control"]["android"]["location_df"]).copy()
        emd.filter_geo_df(unfiltered_loc_a_df, section_gt_shapes.filter([gt_key]))
        loc_df_a = unfiltered_loc_a_df.query("outside_polygons==False")

        unfiltered_loc_b_df = emd.to_geo_df(e["temporal_control"]["ios"]["location_df"]).copy()
        emd.filter_geo_df(unfiltered_loc_b_df, section_gt_shapes.filter([gt_key]))
        loc_df_b = unfiltered_loc_b_df.query("outside_polygons==False")

        print(f"START_END: for threshold {dist_threshold}, before merging, for key {gt_key}, android: {len(unfiltered_loc_a_df)=} -> {len(loc_df_a)=}, ios: {len(unfiltered_loc_b_df)=} -> {len(loc_df_b)=}")

        return (loc_df_a, loc_df_b)

    start_loc_df_a, start_loc_df_b = _get_filtered_loc("start_loc")
    end_loc_df_a, end_loc_df_b = _get_filtered_loc("end_loc")
        
    merge_fn = functools.partial(collapse_inner_join, b_merge_fn=b_merge_midpoint)

    def _match_single_to_gt(filtered_loc_df, dist_threshold):
        new_location_df = get_int_aligned_trajectory(filtered_loc_df, tz)

        new_location_df_u = emd.to_utm_df(new_location_df)

        add_gt_error_projection(new_location_df_u, utm_gt_linestring)

        new_location_df["gt_distance"] = new_location_df_u.gt_distance
        new_location_df["gt_projection"] = new_location_df_u.gt_projection

        filtered_location_df = new_location_df.query("gt_distance < @dist_threshold")
        filtered_location_df['source'] = ['match_gt'] * len(filtered_location_df)
        # filtered_location_df.drop(columns=["gt_distance", "])
        return gpd.GeoDataFrame(filtered_location_df)

    def _align_and_merge(loc_df_a, loc_df_b, dist_threshold):
        # if this is exactly one, 
        # bus trip with e-scooter access city_escooter 3 and include_ends=True
        # fails with
        # x and y arrays must have at least 2 entries
        if len(loc_df_a) > 1 and len(loc_df_b) > 1:
            new_location_df_a = get_int_aligned_trajectory(loc_df_a, tz)
            new_location_df_i = get_int_aligned_trajectory(loc_df_b, tz)

            merged_df = pd.merge(new_location_df_a, new_location_df_i, on="ts",
                how="inner", suffixes=("_a", "_i")).sort_values(by="ts", axis="index")
            merged_df["t_distance"] = emd.to_utm_series(gpd.GeoSeries(merged_df.geometry_a)).distance(emd.to_utm_series(gpd.GeoSeries(merged_df.geometry_i)))
            filtered_merged_df = merged_df.query("t_distance < @dist_threshold")
            print("START_END: After filtering the merged dataframe, retained %d of %d (%s)" %
                  (len(filtered_merged_df), max(len(new_location_df_a), len(new_location_df_i)),
                    (len(filtered_merged_df)/max(len(new_location_df_a), len(new_location_df_i)))))
            ret_val = gpd.GeoDataFrame(list(filtered_merged_df.apply(merge_fn, axis=1)))
            if len(filtered_merged_df) == 0:
                print(f"CHECKME: {len(merged_df)=}, {len(filtered_merged_df)=}, START_END: after merging, {merged_df.head()=}")
                return gpd.GeoDataFrame([])
            else:
                return ret_val
#         elif len(loc_df_a) > 0 and len(loc_df_b) == 0:
#             return _match_single_to_gt(loc_df_a, dist_threshold)
#         elif len(loc_df_a) == 0 and len(loc_df_b) > 0:
#             return _match_single_to_gt(loc_df_b, dist_threshold)
        else:
            return gpd.GeoDataFrame([])

    start_initial_ends_gpdf = _align_and_merge(start_loc_df_a, start_loc_df_b, dist_threshold)
    end_initial_ends_gpdf = _align_and_merge(end_loc_df_a, end_loc_df_b, dist_threshold)

    return [start_initial_ends_gpdf, end_initial_ends_gpdf]

def ref_ct_general(e, b_merge_fn, dist_threshold, tz="UTC", include_ends=False):
    fill_gt_linestring(e)
    section_gt_shapes = e["ground_truth"]["gt_shapes"]
    # print("In ref_ct_general, %s" % section_gt_shapes.filter(items=["start_loc","end_loc"]))
    filtered_loc_df_a = emd.filter_geo_df(
        emd.to_geo_df(e["temporal_control"]["android"]["location_df"]),
        section_gt_shapes.filter(["start_loc","end_loc"]))
    filtered_loc_df_b = emd.filter_geo_df(
        emd.to_geo_df(e["temporal_control"]["ios"]["location_df"]),
        section_gt_shapes.filter(["start_loc","end_loc"]))
    print(f"MATCH TRAJECTORY: {len(filtered_loc_df_a)=}, {len(filtered_loc_df_b)=}")
    new_location_df_a = get_int_aligned_trajectory(filtered_loc_df_a, tz)
    new_location_df_i = get_int_aligned_trajectory(filtered_loc_df_b, tz)
    merged_df = pd.merge(new_location_df_a, new_location_df_i, on="ts",
        how="inner", suffixes=("_a", "_i")).sort_values(by="ts", axis="index")
    merged_df["t_distance"] = emd.to_utm_series(gpd.GeoSeries(merged_df.geometry_a)).distance(emd.to_utm_series(gpd.GeoSeries(merged_df.geometry_i)))
    filtered_merged_df = merged_df.query("t_distance < @dist_threshold")
    print("After filtering, retained %d of %d (%s)" %
          (len(filtered_merged_df), max(len(new_location_df_a), len(new_location_df_i)),
            (len(filtered_merged_df)/max(len(new_location_df_a), len(new_location_df_i)))))

    merge_fn = functools.partial(collapse_inner_join, b_merge_fn=b_merge_fn)
    initial_reference_gpdf = gpd.GeoDataFrame(list(filtered_merged_df.apply(merge_fn, axis=1)))
    if include_ends:
        [start_initial_ends_gpdf, end_initial_ends_gpdf] = ref_ends(e, dist_threshold, tz)
        print(f"CONCAT: {include_ends=}, before concatenating {len(start_initial_ends_gpdf)=}, {len(initial_reference_gpdf)=}, {len(end_initial_ends_gpdf)=}")
        initial_reference_gpdf = pd.concat([start_initial_ends_gpdf, initial_reference_gpdf, end_initial_ends_gpdf], axis=0).sort_values(by="ts").reset_index(drop=True)
        print(f"CONCAT: {include_ends=}, after concatenating {len(initial_reference_gpdf)=}")
    # print(end_initial_ends_gpdf)
    # print(initial_reference_gpdf.columns)
    # print(initial_reference_gpdf[initial_reference_gpdf.ts.isna()])
    if len(initial_reference_gpdf.columns) > 1:
        initial_reference_gpdf["fmt_time"] = initial_reference_gpdf.ts.apply(lambda ts: arrow.get(ts).to(tz))
        assert len(initial_reference_gpdf[initial_reference_gpdf.latitude.isnull()]) == 0, "Found %d null entries out of %d total" % (len(initial_reference_gpdf.latitude.isnull()), len(initial_reference_gpdf))
        # print(initial_reference_gpdf.head())
        return initial_reference_gpdf
    else:
        return gpd.GeoDataFrame()

def ref_gt_general(e, b_merge_fn, dist_threshold, tz="UTC", include_ends=False):
    fill_gt_linestring(e)
    utm_gt_linestring = e["ground_truth"]["utm_linestring"]
    section_gt_shapes = e["ground_truth"]["gt_shapes"]
    filtered_loc_df_a = emd.filter_geo_df(
        emd.to_geo_df(e["temporal_control"]["android"]["location_df"]),
        section_gt_shapes.filter(["start_loc","end_loc"]))
    filtered_loc_df_b = emd.filter_geo_df(
        emd.to_geo_df(e["temporal_control"]["ios"]["location_df"]),
        section_gt_shapes.filter(["start_loc","end_loc"]))
    new_location_df_a = get_int_aligned_trajectory(filtered_loc_df_a, tz)
    new_location_df_i = get_int_aligned_trajectory(filtered_loc_df_b, tz)

    new_location_df_ua = emd.to_utm_df(new_location_df_a)
    new_location_df_ui = emd.to_utm_df(new_location_df_i)

    add_gt_error_projection(new_location_df_ua, utm_gt_linestring)
    add_gt_error_projection(new_location_df_ui, utm_gt_linestring)

    new_location_df_a["gt_distance"] = new_location_df_ua.gt_distance
    new_location_df_a["gt_projection"] = new_location_df_ua.gt_projection

    new_location_df_i["gt_distance"] = new_location_df_ui.gt_distance
    new_location_df_i["gt_projection"] = new_location_df_ui.gt_projection

    filtered_location_df_a = new_location_df_a.query("gt_distance < @dist_threshold")
    filtered_location_df_i = new_location_df_i.query("gt_distance < @dist_threshold")
    print("After filtering, %d of %d (%s) for android and %d of %d (%s) for ios" %
          (len(filtered_location_df_a), len(new_location_df_a), (len(filtered_location_df_a)/len(new_location_df_a)),
           len(filtered_location_df_i), len(new_location_df_i), (len(filtered_location_df_i)/len(new_location_df_i))))
    merged_df = pd.merge(filtered_location_df_a, filtered_location_df_i, on="ts",
        how="outer", suffixes=("_a", "_i")).sort_values(by="ts", axis="index")
    merge_fn = functools.partial(collapse_outer_join_stateless, b_merge_fn=b_merge_fn)
    initial_reference_gpdf = gpd.GeoDataFrame(list(merged_df.apply(merge_fn, axis=1)))
    if include_ends:
        [start_initial_ends_gpdf, end_initial_ends_gpdf] = ref_ends(e, dist_threshold, tz)
        initial_reference_gpdf = pd.concat([start_initial_ends_gpdf, initial_reference_gpdf, end_initial_ends_gpdf], axis=0).sort_values(by="ts").reset_index(drop=True)
    if len(initial_reference_gpdf.columns) > 1:
        initial_reference_gpdf["fmt_time"] = initial_reference_gpdf.ts.apply(lambda ts: arrow.get(ts).to(tz))
        print("After merging, found %d of android %d (%s), ios %d (%s)" %
              (len(initial_reference_gpdf), len(new_location_df_a), (len(initial_reference_gpdf)/len(new_location_df_a)),
               len(new_location_df_i), (len(initial_reference_gpdf)/len(new_location_df_i))))
        assert len(initial_reference_gpdf[initial_reference_gpdf.latitude.isnull()]) == 0, "Found %d null entries out of %d total" % (len(initial_reference_gpdf.latitude.isnull()), len(initial_reference_gpdf))
        return initial_reference_gpdf
    else:
        return gpd.GeoDataFrame()

def ref_travel_forward(e, dist_threshold, tz="UTC", include_ends=False):
    # This function needs a global variable
    global distance_so_far
    distance_so_far = 0
    fill_gt_linestring(e)
    section_gt_shapes = e["ground_truth"]["gt_shapes"]
    # print(f"GEO_DF: before filtering, {len(e['temporal_control']['android']['location_df'])=} and {len(e['temporal_control']['ios']['location_df'])=}")
    filtered_utm_loc_df_a = emd.filter_geo_df(
        emd.to_geo_df(e["temporal_control"]["android"]["location_df"]),
        section_gt_shapes.filter(["start_loc","end_loc"]))
    filtered_utm_loc_df_b = emd.filter_geo_df(
        emd.to_geo_df(e["temporal_control"]["ios"]["location_df"]),
        section_gt_shapes.filter(["start_loc","end_loc"]))
    # print(f"GEO_DF: after filtering, {len(filtered_utm_loc_df_a)=} and {len(filtered_utm_loc_df_b)=}")
    new_location_df_a = get_int_aligned_trajectory(filtered_utm_loc_df_a, tz)
    new_location_df_i = get_int_aligned_trajectory(filtered_utm_loc_df_b, tz)

    utm_gt_linestring = e["ground_truth"]["utm_linestring"]

    new_location_df_ua = emd.to_utm_df(new_location_df_a)
    new_location_df_ui = emd.to_utm_df(new_location_df_i)

    add_gt_error_projection(new_location_df_ua, utm_gt_linestring)
    add_gt_error_projection(new_location_df_ui, utm_gt_linestring)

    new_location_df_a["gt_distance"] = new_location_df_ua.gt_distance
    new_location_df_a["gt_projection"] = new_location_df_ua.gt_projection

    new_location_df_i["gt_distance"] = new_location_df_ui.gt_distance
    new_location_df_i["gt_projection"] = new_location_df_ui.gt_projection

    new_location_df_a["gt_cum_proj"] = new_location_df_a.gt_projection.cumsum()
    new_location_df_i["gt_cum_proj"] = new_location_df_i.gt_projection.cumsum()

    filtered_location_df_a = new_location_df_a.query("gt_distance < @dist_threshold")
    filtered_location_df_i = new_location_df_i.query("gt_distance < @dist_threshold")
    print("After filtering, %d of %d (%s) for android and %d of %d (%s) for ios" %
          (len(filtered_location_df_a), len(new_location_df_a), (len(filtered_location_df_a)/len(new_location_df_a)),
           len(filtered_location_df_i), len(new_location_df_i), (len(filtered_location_df_i)/len(new_location_df_i))))
    merged_df = pd.merge(filtered_location_df_a, filtered_location_df_i, on="ts",
        how="outer", suffixes=("_a", "_i")).sort_values(by="ts", axis="index")
    merge_fn = functools.partial(collapse_outer_join_dist_so_far, more_details_fn = None)
    initial_reference_gpdf = gpd.GeoDataFrame(list(merged_df.apply(merge_fn, axis=1)))
    if include_ends:
        [start_initial_ends_gpdf, end_initial_ends_gpdf] = ref_ends(e, dist_threshold, tz)
        print(f"CONCAT: {include_ends=}, before concatenating {len(start_initial_ends_gpdf)=}, {len(initial_reference_gpdf)=}, {len(end_initial_ends_gpdf)=}")
        initial_reference_gpdf = pd.concat([start_initial_ends_gpdf, initial_reference_gpdf, end_initial_ends_gpdf], axis=0).sort_values(by="ts").reset_index(drop=True)
        print(f"CONCAT: {include_ends=}, after concatenating {len(initial_reference_gpdf)=}")
    if len(initial_reference_gpdf.columns) > 1:
        initial_reference_gpdf["fmt_time"] = initial_reference_gpdf.ts.apply(lambda ts: arrow.get(ts).to(tz))
        reference_gpdf = initial_reference_gpdf[initial_reference_gpdf.latitude.notnull()]
        print("After merging, found %d / %d of android %d (%s), ios %d (%s)" %
              (len(reference_gpdf), len(initial_reference_gpdf), len(new_location_df_a), (len(reference_gpdf)/len(new_location_df_a)),
               len(new_location_df_i), (len(reference_gpdf)/len(new_location_df_i))))
        assert len(reference_gpdf[reference_gpdf.latitude.isnull()]) == 0, "Found %d null entries out of %d total" % (len(reference_gpdf[reference_gpdf.latitude.isnull()]), len(initial_reference_gpdf))
        return reference_gpdf
    else:
        return gpd.GeoDataFrame()


####
# END: Combining into actual reference constructions
####


####
# BEGIN: Final ensemble reference construction that uses ground truth
# - if the ground truth is simple, use the `travel_forward`
# - if the ground truth is complex, use trajectory-only with midpoint
# - we leave the threshold as a parameter, defaulting to 25, which seems to
# work pretty well in the evaluation
####

coverage_density = lambda df, sr: len(df)/(sr["end_ts"] - sr["start_ts"])
coverage_time = lambda df, sr: (df.ts.iloc[-1] - df.ts.iloc[0])/(sr["end_ts"] - sr["start_ts"])
coverage_max_gap = lambda df, sr: df.ts.diff().max()/(sr["end_ts"] - sr["start_ts"])

def final_ref_ensemble(e, dist_threshold=25, tz="UTC", include_ends=False):
    fill_gt_linestring(e)
    gt_linestring = e["ground_truth"]["linestring"]
    try:
        tf_ref_df = ref_travel_forward(e, dist_threshold, tz, include_ends)
        tf_stats = {
            "coverage_density": coverage_density(tf_ref_df, e),
            "coverage_time": coverage_time(tf_ref_df, e),
            "coverage_max_gap": coverage_max_gap(tf_ref_df, e)
        }
        print("Validated tf, stats are %s" % tf_stats)
    except Exception as exp_tf:
        print("Found exception %s while computing tf_ref_df, skipping" % exp_tf)
        traceback.print_exc()
        tf_stats = None

    try:
        ct_ref_df = ref_ct_general(e, b_merge_midpoint, dist_threshold, tz, include_ends)
        ct_stats = {
            "coverage_density": coverage_density(ct_ref_df, e),
            "coverage_time": coverage_time(ct_ref_df, e),
            "coverage_max_gap": coverage_max_gap(ct_ref_df, e)
        }
        print("Validated ct, stats are %s" % ct_stats)
    except Exception as exp_ct:
        print("Found exception %s while computing ct_ref_df, skipping" % exp_ct)
        traceback.print_exc()
        ct_stats = None

    if tf_stats is None and ct_stats is None:
        assert False, "Neither method works!"
    elif tf_stats is None and ct_stats is not None:
        return ("ct", ct_ref_df)
    elif tf_stats is not None and ct_stats is None:
        return ("tf", tf_ref_df)

    assert tf_stats is not None and ct_stats is not None

    if tf_stats["coverage_max_gap"] > ct_stats["coverage_max_gap"] and\
        tf_stats["coverage_density"] < ct_stats["coverage_density"]:
        print("max_gap for tf = %s > ct = %s and density %s < %s, returning ct len = %d not tf len = %d" %
            (tf_stats["coverage_max_gap"], ct_stats["coverage_max_gap"],
             tf_stats["coverage_density"], ct_stats["coverage_density"],
             len(ct_ref_df), len(tf_ref_df)))
        return ("ct", ct_ref_df)
    else:
        print("for tf = %s v/s ct = %s, density %s v/s %s, returning tf len = %d not cf len = %d" %
            (tf_stats["coverage_max_gap"], ct_stats["coverage_max_gap"],
             tf_stats["coverage_density"], ct_stats["coverage_density"],
             len(tf_ref_df), len(ct_ref_df)))
        return ("tf", tf_ref_df)

####
# END: Final ensemble reference construction that uses ground truth
####
