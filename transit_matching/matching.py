import numpy as np
from mappymatch.constructs.trace import Trace
from mappymatch.matchers.lcss.lcss import LCSSMatcher
from pyproj import Geod, Transformer

from transit_matching.error_calculations import calc_error
from transit_matching.overpass_parser import parse_relation

def get_graph_match(df, nx_map):
    trace = Trace.from_dataframe(df, lat_column="latitude", lon_column="longitude", xy=True)

    matcher = LCSSMatcher(nx_map)

    match_result = matcher.match_trace(trace)

    transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326")

    if len(match_result.path) > 0:
        path = []

        for road in match_result.path:
            for i in range(len(road.geom.coords.xy[0]) - 1):
                path.append(transformer.transform(road.geom.coords.xy[0][i], road.geom.coords.xy[1][i]))
        
        path.append(transformer.transform(match_result.path[-1].geom.coords.xy[0][1], match_result.path[-1].geom.coords.xy[1][1]))
    
        path = np.array(path)

        distances = calc_error(path, df)
        error = distances.mean()

        return (error, path, distances)
    else:
        return (float('inf'), [], [])

    
    
def get_transit_match(df, data):
    locations = df[['latitude', 'longitude']].to_numpy()
    
    relations = data['relations']
    edges = data['edges']
    stops = data['stops']
    geometry = data['geometry']

    geod = Geod(ellps='WGS84')

    best_path = []
    best_error = float('inf')
    best_mode = ""
    best_distances = []
    # best_relation = -1

    for relation in relations.keys():
        
        stop_order = [n for n in relations[relation]['nodes'] if n in stops]
        
        firstNode = [-1, float('inf'), -1]
        lastNode = [-1, float('inf'), -1]

        index = 0

        for stop in stop_order:
            node_loc = geometry[stop]
            _, _, distance_first = geod.inv(locations[0][1], locations[0][0], node_loc[1], node_loc[0])
            _, _, distance_last = geod.inv(locations[-1][1], locations[-1][0], node_loc[1], node_loc[0])
            if distance_first < firstNode[1]:
                firstNode = [stop, distance_first, index]
            if distance_last < lastNode[1]:
                lastNode = [stop, distance_last, index]
            
            index += 1
        
        # The start point has to be before the end point
        if firstNode[2] >= lastNode[2]:
            continue

        passed_stops = stop_order[firstNode[2]:(lastNode[2]+1)]

        path = []

        stop_edges = parse_relation(relation, relations, edges, stops, geometry)

        valid_route = True
        for e in zip(passed_stops, passed_stops[1:]):
            if e not in stop_edges:
                valid_route = False
                break
            path.extend(stop_edges[e])

        if not valid_route:
            continue

        path = np.array(path)

        distances = calc_error(path, df)
        error = distances.mean()
        if error < best_error:
            best_error = error
            best_path = path
            best_mode = relations[relation]['mode']
            best_distances = distances
            # best_relation = relation

    return (best_error, best_path, best_mode, best_distances)


