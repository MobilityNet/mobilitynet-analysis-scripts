import utm
import pyproj
import geopandas as gpd
import shapely as shp
import pandas as pd


C = 40075017
g = pyproj.Geod(ellps='clrk66')

def dist_using_circumference(location_gpdf, gt_linestring):
    return location_gpdf.distance(gt_linestring) * (C/360)

# Note that this does not work, since epsg: 4329 is (lat,lon), not (lon,lat)
# there is currently no epsg code for (lon, lat)
# https://gis.stackexchange.com/questions/124077/is-there-an-epsg-code-to-specify-wgs84-in-lon-lat-order-instead-of-lat-lon
# from the same answer, there is OGC string for (lon, lat) but it is not
# supported by geopandas
# Leaving this here in case this changes in the future

def dist_using_crs_change(location_gpdf, gt_linestring):
    location_gpdf.crs = {'init' :'epsg:4326'}
    utm_gpdf = location_gpdf.to_crs(epsg=26918)
    gt_linestring_series = gpd.GeoSeries([gt_linestring])
    gt_linestring_series.crs = {'init' :'epsg:4326'}
    utm_linestring_series = gt_linestring_series.to_crs(epsg=26819)
    utm_linestring = utm_linestring_series.iloc[0]
    return utm_gpdf.distance(utm_linestring)

to_utm_coords = lambda x, y, z=None: utm.from_latlon(y, x)[0:2]
nop = lambda x, y, z=None: (x, y)

def to_extended_utm_coords(p):
    utm_coords = utm.from_latlon(p.y, p.x)
    pu = shp.geometry.Point(utm_coords[0:2])
    pu.zno = utm_coords[2]
    pu.zletter = utm_coords[3]
    return pu

def to_utm_df_extended(location_gpdf):
    utm_gpdf = location_gpdf.copy()
    # Copy the lonlat geometry just in case
    utm_gpdf["geometry_lonlat"] = utm_gpdf.geometry
    utm_gpdf.geometry = utm_gpdf.geometry.apply(lambda p: to_extended_utm_coords(p))
    utm_gpdf.longitude = utm_gpdf.geometry.apply(lambda p: p.x)
    utm_gpdf.latitude = utm_gpdf.geometry.apply(lambda p: p.y)
    utm_gpdf.zno = utm_gpdf.geometry.apply(lambda p: p.zno)
    utm_gpdf.zletter = utm_gpdf.geometry.apply(lambda p: p.zletter)
    return utm_gpdf

def to_utm_series(location_series):
    return location_series.apply(lambda p: shp.ops.transform(to_utm_coords, p))

def to_utm_df(location_gpdf):
    utm_gpdf = location_gpdf.copy()
    utm_gpdf.geometry = utm_gpdf.geometry.apply(lambda p: shp.ops.transform(to_utm_coords, p))
    utm_gpdf.longitude = utm_gpdf.geometry.apply(lambda p: p.x)
    utm_gpdf.latitude = utm_gpdf.geometry.apply(lambda p: p.y)
    return utm_gpdf

def to_utm_line(gt_linestring):
    return shp.ops.transform(to_utm_coords, gt_linestring)

def dist_using_manual_utm_change(location_gpdf, gt_linestring):
    utm_gpdf = to_utm_df(location_gpdf)
    # print(utm_gpdf.geometry.head())
    utm_gt_linestring = to_utm_line(gt_linestring)
    # print(utm_gt_linestring)
    return utm_gpdf.distance(utm_gt_linestring)

def dist_using_projection(location_gpdf, gt_linestring):
    projections = location_gpdf.geometry.apply(lambda p: gt_linestring.project(p))
    # print(projections.head())
    proj_points = projections.apply(lambda d:gt_linestring.interpolate(d))
    # print(proj_points.head())
    project_x = proj_points.apply(lambda p: p.x)
    project_y = proj_points.apply(lambda p: p.y)
    return pd.Series(g.inv(list(location_gpdf.longitude), list(location_gpdf.latitude),
                 list(project_x), list(project_y))[2], index=location_gpdf.index)

def to_geo_df(loc_df):
    return gpd.GeoDataFrame(loc_df, geometry=loc_df.apply(lambda lr: 
        shp.geometry.Point(lr.longitude, lr.latitude), axis=1))

def to_loc_df(geo_df):
    loc_df = geo_df.copy()
    loc_df["longitude"] = geo_df.geometry.apply(lambda p: p.x)
    loc_df["latitude"] = geo_df.geometry.apply(lambda p: p.y)
    return loc_df

def linestring_to_geo_df(linestring):
    xy = linestring.xy
    return gpd.GeoDataFrame({"longitude": xy[0], "latitude": xy[1],
                      "geometry": [shp.geometry.Point((x, y))
                            for x, y in zip(xy[0], xy[1])]})

def geo_df_to_linestring(geo_df):
    return shp.geometry.LineString(geo_df.geometry.tolist())

def is_point_outside_polygons(loc_row, polygons):
    """
    Utility function to check if a point represented by a row in a location dataframe
    is contained within a series of Shapely polygons
    """
    # print(loc_row)
    point = loc_row.geometry
    inside_polygons = polygons.contains(point)
    return not inside_polygons.any()

def filter_geo_df(geo_df, polygons):
    prepped_polygons = polygons.apply(lambda p: shp.prepared.prep(p))
    geo_df["outside_polygons"] = geo_df.apply(lambda r: is_point_outside_polygons(r, polygons), axis=1)
    # return a slice instead of setting a column value
    print("After filtering against polygons %s, %s -> %s" %
        (len(polygons), len(geo_df), len(geo_df.query("outside_polygons==True"))))
    return geo_df.query("outside_polygons==True")

def filter_ground_truth_linestring(gt_shapes):
    gt_geo_df = linestring_to_geo_df(gt_shapes.loc["route"])
    # print("gt_geo_df = %d" % len(gt_geo_df))
    filtered_gt_geo_df = filter_geo_df(gt_geo_df, gt_shapes.filter(["start_loc","end_loc"]))
    # print("filtered_gt_geo_df = %d" % len(filtered_gt_geo_df))
    start_intersection = gt_shapes.loc["route"].intersection(gt_shapes.loc["start_loc"])
    end_intersection = gt_shapes.loc["route"].intersection(gt_shapes.loc["end_loc"])
    # print(start_intersection, end_intersection)
    # final_first_point = shp.geometry.Point(start_intersection.coords[-1])
    # final_last_point = shp.geometry.Point(end_intersection.coords[0])
    return geo_df_to_linestring(filtered_gt_geo_df)

