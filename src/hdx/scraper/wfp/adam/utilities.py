import re


def get_latitude_longitude(coord_string: str) -> tuple[float | None, float | None]:
    # This regex finds numbers (including negatives and decimals)
    numbers = re.findall(r"[-+]?\d*\.\d+|\d+", coord_string)

    if len(numbers) >= 2:
        # Remember: Point(Long Lat)
        longitude = float(numbers[1])
        latitude = float(numbers[2])
        return latitude, longitude
    return None, None
