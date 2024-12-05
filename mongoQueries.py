from pymongo import MongoClient
from datetime import datetime
import sys

client = MongoClient("mongodb://localhost:27017/")

db = client.project
collection = db.vehicle_processed_data

# Define the start time and end time
start_time = datetime.strptime(sys.argv[1], "%d/%m/%Y %H:%M:%S")
end_time = datetime.strptime(sys.argv[2], "%d/%m/%Y %H:%M:%S")

# Common stages for both pipelines
common_stages = [
    # Match documents within the initial time range
    {
        "$match": {
            "time": {
                "$gte": start_time.strftime("%d/%m/%Y %H:%M:%S"),
                "$lte": end_time.strftime("%d/%m/%Y %H:%M:%S")
            }
        }
    },
    # Convert the time field to Date type
    {
        "$addFields": {
            "time": {
                "$dateFromString": {
                    "dateString": "$time",
                    "format": "%d/%m/%Y %H:%M:%S"
                }
            }
        }
    },
    # Match documents within the converted time range
    {
        "$match": {
            "time": {
                "$gte": start_time,
                "$lte": end_time
            }
        }
    }
]

# Pipeline to find the link with the least number of vehicles
pipeline1 = common_stages + [
    # Group by link and sum the vehicle count
    {
        "$group": {
            "_id": "$link",
            "totalVehicles": {"$sum": {"$toLong": "$vcount"}}
        }
    },
    # Sort by totalVehicles in ascending order
    {
        "$sort": {"totalVehicles": 1}
    },
    # Limit to the link with the least vehicles
    {
        "$limit": 1
    }
]

result1 = list(collection.aggregate(pipeline1))
if result1:
    print(f"Link with the least number of vehicles: {result1[0]['_id']}")
    print(f"Total vehicles: {result1[0]['totalVehicles']}\n")
else:
    print("No data found for the specified time range.")

# Pipeline to find the link with the highest average speed
pipeline2 = common_stages + [
    # Group by link and calculate the average speed
    {
        "$group": {
            "_id": "$link",
            "averageSpeed": {"$avg": "$vspeed"}
        }
    },
    # Sort by averageSpeed in descending order
    {
        "$sort": {"averageSpeed": -1}
    },
    # Limit to the link with the highest average speed
    {
        "$limit": 1
    }
]

result2 = list(collection.aggregate(pipeline2))
if result2:
    print(f"Link with the highest average speed: {result2[0]['_id']}")
    print(f"Average speed: {result2[0]['averageSpeed']}\n")
else:
    print("No data found for the specified time range.")

collection = db.vehicle_raw_data

# Pipeline to calculate the longest journey for each vehicle
pipeline3 = common_stages + [
    # Convert position to a number
    {
        "$addFields": {
            "position": {"$toDouble": "$position"}
        }
    },
    # Group by vehicle to calculate the cumulative distance traveled
    {
        "$group": {
            "_id": "$name",
            "startPosition": {"$first": "$position"},
            "endPosition": {"$last": "$position"}
        }
    },
    # Project the journey length as the difference between start and end positions
    {
        "$project": {
            "_id": 1,
            "journeyLength": {"$subtract": ["$endPosition", "$startPosition"]}
        }
    },
    # Sort by journey length in descending order
    {
        "$sort": {"journeyLength": -1}
    }
]

# Execute the aggregation pipeline
result = list(collection.aggregate(pipeline3))

# Print the result
if result:
    print("Longest Journey:")
    print(f"Vehicle: {result[0]['_id']}, Longest Journey Length: {result[0]['journeyLength']}")
else:
    print("No data found for the specified time range.")