import requests, json
from datetime import datetime
from influxdb import InfluxDBClient

url = "https://corona.lmao.ninja/all"
json_data = json.loads(requests.get(url).text)

influxdb_database = "covid"
influxdb_url = "localhost"
influxdb_port = "8086"
measurement = "world_todays"
client = InfluxDBClient(influxdb_url, influxdb_port, "root","root" , influxdb_database)


d =	[{
    "measurement": measurement,
    "time": datetime.now(),
    "fields": { 
                "death": json_data['deaths'],
                "recovered": json_data['recovered'],
                "confirmed": json_data['cases']
                },
    }]
client.write_points(d)


url = "https://corona.lmao.ninja/countries"
json_data = json.loads(requests.get(url).text)
india_data = None
measurement = "india"

for i in json_data:
	if i['country'] == "India":
		india_data = i

d =	[{
    "measurement": measurement,
    "time": datetime.now(),
    "fields": {
    	"cases":india_data["cases"],
    	"todayCases":india_data["todayCases"],
    	"deaths":india_data["deaths"],
    	"todayDeaths":india_data["todayDeaths"],
    	"recovered":india_data["recovered"],
    	"active":india_data["active"],
    	"critical":india_data["critical"],
    },
    }]
client.write_points(d)
