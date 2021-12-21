"""
Script demonstrating how to convert longitude and latitude into an address.

It assumes that the database has been initialised and populated at least the metadata.
"""

import requests

from citypulse_etl.database import Session
from citypulse_etl.models import TrafficSensor

def get_address_from_longitude_latitude(lon, lat):
    """
    Uses the Nominatim API to do a reverse geocode on a longitude and latitude
    then returns a human readable string of the address.
    """
    r = requests.get(
        'https://nominatim.openstreetmap.org/reverse?',
        params=dict(lon=lon, lat=lat, format='json'),
        )
    d = r.json().get('address')

    if d is None:
        return 'No address found'

    road = d.get('road')
    town_or_city = d.get('town', d.get('city'))
    postcode = d.get('postcode')
    country = d.get('country')
    address = f"{road}, {town_or_city} {postcode}, {country}"

    return address

def print_traffic_sensor_points_in_human_readable_format(ts: TrafficSensor):
    """
    Prints the addresses of the two points associated with a TrafficSensor.
    Note, this could simply be added as a method on the TrafficSensor class.
    """
    point_1_address = get_address_from_longitude_latitude(
        ts.point_1_longitude,
        ts.point_1_latitude,
    )

    point_2_address = get_address_from_longitude_latitude(
        ts.point_2_longitude,
        ts.point_2_latitude,
    )

    print(f"Traffic Sensor #{ts.id} measures traffic between:")
    print(f"    {point_1_address}")
    print(f"    and")
    print(f"    {point_2_address}")


def main():
    session = Session()

    for ts in session.query(TrafficSensor).limit(5).all():
        print_traffic_sensor_points_in_human_readable_format(ts)

    session.close()


if __name__ == '__main__':
    main()
