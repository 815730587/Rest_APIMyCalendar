# flask project configuration file
from datetime import datetime

import requests
#
# lat = "-33.865143"
# lng = "151.209900&"
# with open("georef-australia-state-suburb.csv", 'r', encoding="utf-8") as p:
#     data = p.readlines()
#     for line in data[1:]:
#         temp = line.split(";")
#         temp_res = temp[0].strip() + "," + temp[4].strip() + "," + temp[8].strip()
#         if len(temp_res.split(",")) == 4 and temp_res.split(",")[2] == "New South Wales" and temp_res.split(",")[3].find("Kensington") >=0:
#             lat = temp_res.split(",")[0]
#             lng = temp_res.split(",")[1]
#             break
# Weathe = f"https://www.7timer.info/bin/civil.php?lat={lat}&lng={lng}.9631&ac=1&unit=metric&output=json&product=two"
holdiay = f"https://date.nager.at/api/v2/publicholidays/2021/AU"
holdiay_res = requests.get(holdiay)
# weather_res = requests.get(Weathe)
# print(lat)
# for i in weather_res.json()["dataseries"]:
#         print(i)

# for i in holdiay_res.json():
#     print(i)

date="08-04-2023"
date=datetime.strptime(date,"%Y-%m-%d")
print(date)