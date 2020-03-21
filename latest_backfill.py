from datetime import datetime, timedelta
from influxdb import InfluxDBClient
import csv
import urllib2

all_files_name = []

start_date = datetime(day=22,month=01,year=2020)
end_date = datetime.now() - timedelta(days=1)

base_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/"
while start_date < end_date:
	all_files_name.append(start_date.strftime("%m-%d-%Y") + ".csv")
	start_date = start_date + timedelta(days=1)


influxdb_database = "covid"
influxdb_url = "localhost"
influxdb_port = "8086"
measurement = "world"

client = InfluxDBClient(influxdb_url, influxdb_port, "root","root" , influxdb_database)

for _file in all_files_name: 
	# print(_file)
	json_body = []
	url = base_url + _file
	response = urllib2.urlopen(url)

	cr = csv.reader(response)
	cr.next()
	for i in cr:
		state = i[0].strip() if i[0] else "Remaining"
		state = state.split(",")[0].replace(" ","_")
		country = i[1].strip().replace(" ","_")
		last_updated = i[2].split(" ")[0].strip().split("T")[0].strip()
		confirmed = int(i[3]) if i[3] else 0
		death = int(i[4]) if i[4] else 0
		recovered = int(i[5]) if i[5] else 0
		print(last_updated)
		if len(str(last_updated.split("/")[-1])) == 4:
			last_updated = datetime.strptime(last_updated, "%m/%d/%Y")
		elif len(str(last_updated.split("-")[0])) == 4:
			try:
				last_updated = datetime.strptime(last_updated, "%Y-%d-%m")
			except:
				last_updated = datetime.strptime(last_updated, "%Y-%m-%d")
		else:
			last_updated = datetime.strptime(last_updated, "%m/%d/%y")


		d =	{
            "measurement": measurement,
            "time": last_updated,
            "fields": { 
                        "death": death,
                        "recovered": recovered,
                        "confirmed": confirmed
                        },
            "tags":{
	            "state": state,
                "country": country ,
	            }
            }
		json_body.append(d.copy())
	client.write_points(json_body)