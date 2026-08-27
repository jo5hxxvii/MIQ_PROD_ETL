import json
import time

import googlemaps
import pandas as pd
import requests
from functions.config import _config
from openai import OpenAI

GMAP_API_KEY = _config.GMAP_API_KEY
OPENAI_API_KEY = _config.OPENAI_API_KEY
gmaps = googlemaps.Client(key=GMAP_API_KEY)
openai_client = OpenAI(api_key=OPENAI_API_KEY)


def generate_location_report_gmap(search_query):
    # 1. Geocode via the library
    geo_res = gmaps.geocode(search_query)
    if not geo_res:
        return {"error": "Building not found"}

    first = geo_res[0]
    lat, lng = (
        first["geometry"]["location"]["lat"],
        first["geometry"]["location"]["lng"],
    )

    # Extract Address Info (City, State, Country)
    addr_comps = first.get("address_components", [])
    city = next((c["long_name"] for c in addr_comps if "locality" in c["types"]), "")
    state = next(
        (
            c["long_name"]
            for c in addr_comps
            if "administrative_area_level_1" in c["types"]
        ),
        "",
    )
    country = next((c["long_name"] for c in addr_comps if "country" in c["types"]), "")
    area = next(
        (c["short_name"] for c in addr_comps if "neighborhood" in c["types"]), "JVC"
    )

    # 2. Setup Categories for Places (New)
    # Types use the new naming convention (e.g., 'subway_station' for Metro)
    categories = {
        "Tourist Attractions": {"type": "tourist_attraction", "radius": 15000},
        "Schools": {"type": "school", "radius": 10000},
        "Hospitals": {"type": "hospital", "radius": 10000},
        "Supermarkets": {"type": "supermarket", "radius": 5000},
        "Malls": {"type": "shopping_mall", "radius": 10000},
        "Restaurants": {"type": "restaurant", "radius": 5000},
        "Parks": {"type": "park", "radius": 8000},
        "Metro Stations": {"type": "subway_station", "radius": 10000},  # Metro
        "Metro Stations": {"type": "train_station", "radius": 10000},  # Metro
        "Airports": {"type": "airport", "radius": 45000},  # DXB/DWC
    }

    report = {
        "location": search_query.split(",")[0],
        "area": area,
        "city": city,
        "state": state,
        "country": country,
        "amenities": {},
    }

    # 3. Call Places API (New) for each category
    url = "https://places.googleapis.com/v1/places:searchNearby"

    # Header FieldMask is CRITICAL in V1 to get the data you want
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GMAP_API_KEY,
        "X-Goog-FieldMask": "places.displayName,routingSummaries.legs.distanceMeters",
    }

    for key, cfg in categories.items():
        payload = {
            "includedTypes": [cfg["type"]],
            "maxResultCount": 10,
            "locationRestriction": {
                "circle": {
                    "center": {"latitude": lat, "longitude": lng},
                    "radius": float(cfg["radius"]),
                }
            },
            "routingParameters": {
                "origin": {"latitude": lat, "longitude": lng},
                "travelMode": "DRIVE",
            },
        }

        response = requests.post(url, json=payload, headers=headers).json()

        # Process parallel arrays of Places and RoutingSummaries
        amenity_list = []
        places = response.get("places", [])
        routings = response.get("routingSummaries", [])

        for i in range(len(places)):
            try:
                name = places[i]["displayName"]["text"]
                # RoutingSummaries might be missing if no road path exists
                meters = routings[i]["legs"][0]["distanceMeters"]
                amenity_list.append({"n": name, "d": round(meters / 1000, 1)})
            except (KeyError, IndexError):
                # Fallback: if routing fails, we skip or use straight-line
                continue

        report["amenities"][key] = amenity_list

    return report


def generate_location_report_openai(entity_name):
    prompt = """
                Extract amenities and return structured JSON.

                Schema (use exact keys):
                {
                  "loc": "",
                  "ar": "",
                  "ct": "",
                  "lm": [{"n":"","t":"","d":0}],
                  "sch": [{"n":"","d":0}],
                  "hc": [{"n":"","d":0}],
                  "sup": [{"n":"","d":0}],
                  "mal": [{"n":"","d":0}],
                  "res": [{"n":"","d":0}],
                  "pk": [{"n":"","d":0}],
                  "tr": {"rd":[],"mt":false}
                }

                Key map:
                loc=location_name, ar=area, ct=city
                lm=landmarks, sch=schools, hc=healthcare, sup=supermarkets
                mal=malls, res=restaurants, pk=parks
                n=name, t=type, d=distance_km
                rd=major_roads, mt=metro_access

                Rules:
                - JSON only
                - max5 per list
                - distances in km (approx)
                - unknown=null
            """
    response = openai_client.responses.create(
        model="gpt-5.2",
        input=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": entity_name},
        ],
        temperature=0.2,
        # ,response_format={"type": "json_object"}
    )

    # Extract clean JSON string
    result = response.output[0].content[0].text

    # Convert to Python dict
    return json.loads(result), response
    # return response
