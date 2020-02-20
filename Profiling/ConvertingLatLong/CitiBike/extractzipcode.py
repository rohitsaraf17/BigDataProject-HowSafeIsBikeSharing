
from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="mn2643@nyu.edu")
from geopy.exc import GeocoderTimedOut

locations = set()

def geocode_me(loc):
    try:
        return geolocator.reverse(loc)
    except GeocoderTimedOut:
        return geocode_me(loc)

with open("postcode","a+") as f:
    with open("start_lat_long_unique","r") as file:
        lines =  file.readlines()
        for line in lines:
            words = line.split(",")
            if words[0] not in locations:
                locations.add(words[0])
                lat = str(words[1])
                latlong = str(",".join(words[1:3]))
                #print(words[0])
                address = geocode_me(latlong).raw['address']
                #address = geolocator.reverse(latlong).raw['address']
                if 'postcode' in address:
                    postcode = address['postcode'].encode("utf-8")
                    f.write(words[0]+","+postcode+"\n")
                else:
                    print(words[0])
