def add_dist_heading_speed(points_df):
    # type: (pandas.DataFrame) -> pandas.DataFrame
    """
    Returns a new dataframe with an added "speed" column.
    The speed column has the speed between each point and its previous point.
    The first row has a speed of zero.
    """
    point_list = [ad.AttrDict(row) for row in points_df.to_dict('records')]
    zipped_points_list = list(zip(point_list, point_list[1:]))

    distances = [pf.calDistance(p1, p2) for (p1, p2) in zipped_points_list]
    distances.insert(0, 0)
    speeds = [pf.calSpeed(p1, p2) for (p1, p2) in zipped_points_list]
    speeds.insert(0, 0)
    headings = [pf.calHeading(p1, p2) for (p1, p2) in zipped_points_list]
    headings.insert(0, 0)

    with_distances_df = pd.concat([points_df, pd.Series(distances, name="distance")], axis=1)
    with_speeds_df = pd.concat([with_distances_df, pd.Series(speeds, name="speed")], axis=1)
    if "heading" in with_speeds_df.columns:
        with_speeds_df.drop("heading", axis=1, inplace=True)
    with_headings_df = pd.concat([with_speeds_df, pd.Series(headings, name="heading")], axis=1)
    return with_headings_df