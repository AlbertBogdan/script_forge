from shapely.geometry import Polygon, LineString, Point

def find_box_below(current_box, all_boxes):
    """
    Find the first box below the given box, intersecting with the downward ray.

    :param current_box: Dictionary with keys 'box' (list of points), 'text', etc.
    :param all_boxes: List of dictionaries, each containing 'box' and other keys.
    :return: The first intersecting box dictionary or None if no box is found.
    """
    current_polygon = Polygon(current_box['box'])

    # Calculate the midpoint of the bottom edge of the current box
    bottom_edge = LineString([current_box['box'][2], current_box['box'][3]])
    midpoint = bottom_edge.interpolate(0.5, normalized=True)

    # Create a downward ray starting from the midpoint
    ray = LineString([midpoint, Point(midpoint.x, midpoint.y + 1e6)])

    # Iterate through all boxes to find the first intersecting box
    intersections = []
    for box in all_boxes:
        box_polygon = Polygon(box['box'])
        if ray.intersects(box_polygon):
            # Check if the box is not the current box by comparing their areas
            if not current_polygon.equals(box_polygon):
                intersection = ray.intersection(box_polygon)
                intersections.append((intersection.bounds[1], box))  # Store Y-coordinate for sorting

    # Sort intersections by Y-coordinate (lowest first) and return the first one
    if intersections:
        intersections.sort(key=lambda x: x[0])  # Sort by Y-coordinate
        return intersections[0][1]  # Return the box dictionary

    return None  # No intersecting box found
