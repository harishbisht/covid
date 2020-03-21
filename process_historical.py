from datetime import datetime
from influxdb import InfluxDBClient 
import csv
import urllib2

all_files_name = ["01-21-2020_2200.csv","01-22-2020_1200.csv","01-23-2020_1200.csv",
"01-24-2020_0000.csv","01-24-2020_1200.csv","01-25-2020_0000.csv",
"01-25-2020_1200.csv","01-25-2020_2200.csv","01-26-2020_1100.csv",
"01-26-2020_2300.csv","01-27-2020_0900.csv","01-27-2020_1900.csv",
"01-27-2020_2030.csv","01-28-2020_1300.csv","01-28-2020_1800.csv",
"01-28-2020_2300.csv","01-29-2020_1330.csv","01-29-2020_1430.csv",
"01-29-2020_2100.csv","01-30-2020_1100.csv","01-30-2020_2130.csv",
"01-31-2020_1400.csv","02-01-2020_1000.csv","02-01-2020_1800.csv",
"02-01-2020_2300.csv","02-02-2020_0500.csv","02-02-2020_1945.csv",
"02-02-2020_2100.csv","02-03-2020_1230.csv","02-03-2020_2140.csv",
"02-04-2020_0800.csv","02-04-2020_1150.csv","02-04-2020_2200.csv",
"02-05-2020_1220.csv","02-06-2020_1318.csv","02-06-2020_2005.csv",
"02-07-2020_2024.csv","02-08-2020_1024.csv","02-08-2020_2304.csv",
"02-09-2020_1030.csv","02-09-2020_2320.csv","02-10-2020_1030.csv",
"02-10-2020_1930.csv","02-11-2020_1050.csv","02-11-2020_2044.csv",
"02-12-2020_1020.csv"]

base_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/archived_data/archived_daily_case_updates/"

influxdb_database = "covid"
influxdb_url = "localhost"
influxdb_port = "8086"
measurement = "world"

client = InfluxDBClient(influxdb_url, influxdb_port, "root","root" , influxdb_database)

for _file in all_files_name: 
	json_body = []
	response = urllib2.urlopen(base_url + _file)
	cr = csv.reader(response)
	cr.next()
	for i in cr:
		state = i[0].strip() if i[0] else "Remaining"
		state = state.split(",")[0].replace(" ","_")
		country = i[1].strip().replace(" ","_")
		last_updated = i[2].split(" ")[0].strip()
		confirmed = int(i[3]) if i[3] else 0
		death = int(i[4]) if i[4] else 0
		recovered = int(i[5]) if i[5] else 0
		print(confirmed)
		#now we have 3 types of date format
		# 2020/02/11 case1
		# 2020-02-09 case 2
		# 2/4/20 case3
		# 2/3/2020 case4
		if len(str(last_updated.split("/")[0])) == 4:
			# case 1
			last_updated = datetime.strptime(last_updated, "%Y/%m/%d")

		elif len(str(last_updated.split("-")[0])) == 4:
			# case 2
			last_updated = datetime.strptime(last_updated, "%Y-%m-%d")

		elif len(str(last_updated.split("/")[-1])) == 4:
			# case 3
			last_updated = datetime.strptime(last_updated, "%m/%d/%Y")
		else:
			# case 4
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