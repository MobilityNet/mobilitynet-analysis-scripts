import geojson as gj
import folium

def get_geojson_for_linestring(linestring, color="blue"):
    return gj.Feature(geometry=gj.LineString(linestring.coords),
        properties={"style": {"color": color}})

def get_geojson_for_loc_df(location_df, color="red"):
    lonlat_route_coords = list(zip(location_df.longitude, location_df.latitude))
    return gj.Feature(geometry=gj.LineString(lonlat_route_coords),
        properties={"style": {"color": color}, "ts": list(location_df.ts)})

def get_geojson_for_section(sensed_section, color="red"):
    location_df = sensed_section["location_df"]
    return get_geojson_for_loc_df(location_df, color)

def lonlat_swap(lon_lat):
    return list(reversed(lon_lat))

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

def get_fg_for_loc_df(loc_df, name="points", popupfn=None, stickyfn=None, **kwargs):
    fg = folium.FeatureGroup(name)
    if popupfn is None:
        popupfn = lambda lr: "%d: %s, %s"  % (lr["index"], lr["longitude"], lr["latitude"])
    if stickyfn is None:
        stickyfn = lambda lr: False
    for i, (idx, lr) in enumerate(loc_df.to_dict(orient='index').items()):
        lr["index"] = i
        lr["df_idx"] = idx
        cm = folium.CircleMarker((lr["latitude"], lr["longitude"]), radius=5, **kwargs).add_to(fg)
        folium.Popup(popupfn(lr), show=stickyfn(lr), sticky=stickyfn(lr)).add_to(cm)
    return fg

def get_map_for_geojson(gj, **kwargs):
    curr_map = folium.Map()
    gj_layer = folium.GeoJson(gj, **kwargs)
    curr_map.add_child(gj_layer)
    curr_map.fit_bounds(gj_layer.get_bounds())
    return curr_map
