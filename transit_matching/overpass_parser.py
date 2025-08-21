import requests
import time
from shapely.geometry import MultiLineString, LineString
import datetime
from mappymatch.maps.nx.nx_map import NxMap, NetworkType
from mappymatch.constructs.trace import Trace
from mappymatch.constructs.geofence import Geofence

overpass_url = "https://overpass-api.de/api/interpreter"
network_types = [NetworkType.DRIVE, NetworkType.BIKE, NetworkType.WALK]
route_types = ["bus", "train", "subway", "light_rail"]

def overpass_query(query):
    payload = {
        "data": query
    }

    try:
        response = requests.post(overpass_url, data=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'error in request: {e}, retrying in 1 minute')
        time.sleep(60)
        return overpass_query(query)


def make_query(route_types, geofence, timestamp=None):
    route_type = "|".join(route_types)

    boundary_obj = geofence.geometry.boundary
    all_points = []
    if isinstance(boundary_obj, MultiLineString):
        for line in boundary_obj.geoms:
            all_points.extend(line.coords)
    elif isinstance(boundary_obj, LineString):
        all_points.extend(boundary_obj.coords)

    boundary = [(point[1], point[0]) for point in all_points]
    flat_list = [coord for point in boundary for coord in point]
    poly_string = " ".join(map(str, flat_list))

    if timestamp is None:
        overpass_date_setting = ""
    else:
        dt_object_utc = datetime.datetime.fromtimestamp(
            timestamp,
            tz=datetime.timezone.utc
        )
        overpass_time_string = dt_object_utc.isoformat().replace('+00:00', 'Z')
        overpass_date_setting = f'[date:"{overpass_time_string}"]'

    data = overpass_query(f"""
        [out:json]
        [timeout:90]
        {overpass_date_setting};
        
        relation["route"~"^({route_type})$"](poly:"{poly_string}") -> .routes;
        .routes;
        out body;

        (
            way(r.routes)(poly:"{poly_string}");
            >;
        );
        
        out geom;
    """)['elements']

    edges = {}
    geometry = {}
    stops = {}

    relations = {}

    for n in data:
        if n['type'] == 'way':
            nodes = n['nodes']
            edges[n['id']] = set(zip(nodes, nodes[1:]))
        if n['type'] == 'relation':
            relations[n['id']] = {'mode': n['tags']['route'], 'ways': [w['ref'] for w in n['members'] if w['type'] == 'way'], 'nodes': [w['ref'] for w in n['members'] if w['type'] == 'node']}
        if n['type'] == 'node':
            geometry[n['id']] = (n['lat'], n['lon'])
            if 'tags' in n and 'public_transport' in n['tags']:
                # Maybe check for what the public transport tag actually is
                stops[n['id']] = (n['lat'], n['lon'])
    
    return {'edges': edges, 'geometry': geometry, 'stops': stops, 'relations': relations}

def parse_relation(relation, relations, edges, stops, geometry):
    pairs = set()

    for way in relations[relation]['ways']:
        if way in edges:
            pairs = pairs.union(edges[way])
    
    e = {}
    for a, b in pairs:
        if a not in e:
            e[a] = []
        if b not in e:
            e[b] = []
        
        e[a].append(b)
        e[b].append(a)
    
    relation_stops = set()
    for n, _ in e.items():
        if n in stops and n in relations[relation]['nodes']:
            relation_stops.add(n)

    stop_edges = {} # (a, b) : geom

    for stop in relation_stops:
        queue = [(stop, set([stop]), [geometry[stop]])]
        while len(queue) > 0:
            node, visited, geom = queue.pop(0)

            connections = e[node]

            for connection in connections:
                if connection in visited:
                    continue

                new_geom = geom[:]
                new_geom.append(geometry[connection])
                if connection in relation_stops:
                    stop_edges[(stop, connection)] = new_geom
                    continue
                
                queue.append((connection, visited.union([connection]), new_geom))
    
    return stop_edges

def build_maps(df):
    ts = df.iloc[0]['ts']
    trace = Trace.from_dataframe(df, lat_column="latitude", lon_column="longitude", xy=True)
    geofence = Geofence.from_trace(trace, padding=3e3)

    nx_maps = [NxMap.from_geofence(geofence, network_type=n) for n in network_types]

    return {
        'ts': ts,
        'geofence': geofence,
        'nx_maps': nx_maps,
        'historic': make_query(route_types, geofence, timestamp=ts),
        'modern': make_query(route_types, geofence)
    }