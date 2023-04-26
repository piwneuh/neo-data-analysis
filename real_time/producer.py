import json, urllib.request, time

today = time.strftime('%Y-%m-%d', time.gmtime())
print("Date: " + today)

#Retrieve data about asteroids approaching planet Earth.
url = "https://api.nasa.gov/neo/rest/v1/feed?start_date=" + today + "&end_date=" + today + "&api_key=vbdXqVkpa9sPqau3R1UWjzFeeHA6iZuD0cKivpFO"

response = urllib.request.urlopen(url)
result = json.loads(response.read())

print("Today " + str(result["element_count"]) + " asteroids will be passing close to planet Earth:\n")
asteroids = result["near_earth_objects"]

#Parsing all the JSON data:
for asteroid in asteroids:
    for field in asteroids[asteroid]:
        print("Asteroid Name: " + field["name"])
        print("Estimated Diameter: " + str(round((field["estimated_diameter"]["meters"]["estimated_diameter_min"]+field["estimated_diameter"]["meters"]["estimated_diameter_max"])/2, 0)))
        print("Close Approach Date & Time: " + field["close_approach_data"][0]["close_approach_date_full"])
        print("Velocity: " + str(field["close_approach_data"][0]["relative_velocity"]["kilometers_per_hour"]) + " km/h") 
        print("Distance to Earth: " + str(field["close_approach_data"][0]["miss_distance"]["kilometers"]) + " km") 
   
        if field["is_potentially_hazardous_asteroid"]:   
          print ("This asteroid could be dangerous to planet Earth!")
        else:
          print ("This asteroid poses not threat to planet Earth!")
        print("--------------------")
