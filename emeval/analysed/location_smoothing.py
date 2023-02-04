import math
import pandas as pd

def add_dist(points_df):
    # type: (pandas.DataFrame) -> pandas.DataFrame
    """
    Returns a new dataframe with an added "speed" column.
    The speed column has the speed between each point and its previous point.
    The first row has a speed of zero.
    """
    point_list = [row for row in points_df.to_dict('records')]
    zipped_points_list = list(zip(point_list, point_list[1:]))

    distances = [calDistance(p1, p2) for (p1, p2) in zipped_points_list]
    distances.insert(0, 0)
    with_distances_df = pd.concat([points_df, pd.Series(distances, name="distance")], axis=1)
    return with_distances_df


def calDistance(point1, point2):
    earthRadius = 6371000  # meters
    point1 = [point1['longitude'], point1['latitude']]
    point2 = [point2['longitude'], point2['latitude']]
        
    dLat = math.radians(point1[1]-point2[1])
    dLon = math.radians(point1[0]-point2[0])
    lat1 = math.radians(point1[1])
    lat2 = math.radians(point2[1])

    a = (math.sin(dLat/2) ** 2) + ((math.sin(dLon/2) ** 2) * math.cos(lat1) * math.cos(lat2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = earthRadius * c
    return d
