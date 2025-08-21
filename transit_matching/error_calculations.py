import numpy as np
from pyproj import Geod
from rtree import index

import utm
import pyproj
import geopandas as gpd
import shapely as shp
import pandas as pd
import math

g = pyproj.Geod(ellps='clrk66')

def calc_error(path, locations):
    locations_gdf = gpd.GeoDataFrame(
        locations,
        geometry=gpd.points_from_xy(locations.longitude, locations.latitude)
    )

    path_linestring = shp.LineString([(lon, lat) for lat, lon in path])

    return dist_using_projection_adjusted(locations_gdf, path_linestring)


def _point_to_segment_distance(loc, p1, p2, geod):
    loc_lat, loc_lon = loc
    p1_lat, p1_lon = p1
    p2_lat, p2_lon = p2

    if np.allclose(p1, p2):
        _, _, dist = geod.inv(p1_lon, p1_lat, loc_lon, loc_lat)
        return dist

    az13, _, dist13 = geod.inv(p1_lon, p1_lat, loc_lon, loc_lat)
    az12, _, dist12 = geod.inv(p1_lon, p1_lat, p2_lon, p2_lat)
    
    if dist12 < 1e-9:
        return dist13

    angle_diff_rad = np.radians(az13 - az12)
    along_track_dist = dist13 * np.cos(angle_diff_rad)

    if along_track_dist < 0:
        return dist13
    elif along_track_dist > dist12:
        _, _, dist23 = geod.inv(p2_lon, p2_lat, loc_lon, loc_lat)
        return dist23
    else:
        return abs(dist13 * np.sin(angle_diff_rad))


def geodesic_squared_error(path, locations, k_neighbors=7):
    if len(path) < 2 or len(locations) == 0:
        return 0.0

    geod = Geod(ellps='WGS84')
    p1_pts = path[:-1]
    p2_pts = path[1:]
    num_segments = len(p1_pts)

    idx = index.Index()
    for i in range(num_segments):
        p1 = p1_pts[i]
        p2 = p2_pts[i]
        bbox = (
            min(p1[1], p2[1]),
            min(p1[0], p2[0]),
            max(p1[1], p2[1]),
            max(p1[0], p2[0]),
        )
        idx.insert(i, bbox)

    total_squared_error = 0.0
    
    for loc in locations:
        loc_lon, loc_lat = loc[1], loc[0]
        
        num_candidates = min(k_neighbors, num_segments)
        candidate_indices = list(idx.nearest((loc_lon, loc_lat, loc_lon, loc_lat), num_candidates))
        
        min_dist_for_loc = float('inf')
        for i in candidate_indices:
            dist = _point_to_segment_distance(loc, p1_pts[i], p2_pts[i], geod)
            if dist < min_dist_for_loc:
                min_dist_for_loc = dist
        
        total_squared_error += min_dist_for_loc ** 2

    return total_squared_error

def dist_using_projection_adjusted(location_gpdf, gt_linestring):
    lat = gt_linestring.centroid.y
    adjusted_points = [shp.geometry.Point(coord[0] / math.cos(math.radians(lat)), coord[1]) for coord in list(gt_linestring.coords)]
    unadjusted_points = [shp.geometry.Point(coord[0], coord[1]) for coord in list(gt_linestring.coords)]
    
    adjusted_data = []
    unadjusted_data = []
    for p in zip(adjusted_points, unadjusted_points):
        row = {}
        row["geometry"] = p[0]
        adjusted_data.append(row)
        row2 = {}
        row2["geometry"] = p[1]
        unadjusted_data.append(row2)

    adjusted_gt_gpdf = gpd.GeoDataFrame(adjusted_data, geometry="geometry")
    unadjusted_gt_gpdf = gpd.GeoDataFrame(unadjusted_data, geometry="geometry")
    
    adjusted_location_linestring = shp.geometry.LineString([shp.geometry.Point(coord.x / math.cos(math.radians(lat)), coord.y) for coord in list(location_gpdf.geometry)])
    projections = adjusted_gt_gpdf.geometry.apply(lambda p: adjusted_location_linestring.project(p))
    proj_points = projections.apply(lambda d:adjusted_location_linestring.interpolate(d))

    project_x = proj_points.apply(lambda p: p.x * math.cos(math.radians(lat)))
    project_y = proj_points.apply(lambda p: p.y)
    return pd.Series(g.inv(list(unadjusted_gt_gpdf.geometry.x), list(unadjusted_gt_gpdf.geometry.y),
                 list(project_x), list(project_y))[2], index=unadjusted_gt_gpdf.index)