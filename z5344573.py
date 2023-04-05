import calendar
from datetime import datetime, timedelta
from math import ceil
import requests
from flask import Flask, request, send_file, make_response
from flask_restx import Api, fields, reqparse, Resource
from flask_sqlalchemy import SQLAlchemy
import geopandas as gpd
from matplotlib import pyplot as plt
from sqlalchemy import or_, and_, func
import os
import sys
au_csv=sys.argv[2]
sub=sys.argv[1]
basedir = os.path.abspath(os.path.dirname(__file__))
class Config(object):
    SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL") or "sqlite:///" + os.path.join(basedir, "app.db")
    SQLALCHEMY_TRACK_MODIFICATIONS = False


app = Flask(__name__)
app.config.from_object(Config)
api = Api(app, title="Event API", description="COMP9321 Ass2")
db = SQLAlchemy(app)

Post_event = reqparse.RequestParser()
Post_event.add_argument("name", type=str)
Post_event.add_argument("date", type=str)
Post_event.add_argument("from", type=str)
Post_event.add_argument("to", type=str)
Post_event.add_argument("street", type=str)
Post_event.add_argument("suburb", type=str)
Post_event.add_argument("state", type=str)
Post_event.add_argument("post-code", type=str)
Post_event.add_argument("description", type=str)

Patch_event = reqparse.RequestParser()
Patch_event.add_argument("update name", type=str)
Patch_event.add_argument("update date", type=str)
Patch_event.add_argument("update from", type=str)
Patch_event.add_argument("update to", type=str)
Patch_event.add_argument("update street", type=str)
Patch_event.add_argument("update suburb", type=str)
Patch_event.add_argument("update state", type=str)
Patch_event.add_argument("update post-code", type=str)
Patch_event.add_argument("update description", type=str)

Event_retrieve = reqparse.RequestParser()
Event_retrieve.add_argument("order", type=str, default=["+id"], action="split")
Event_retrieve.add_argument("page", type=int, default=1)
Event_retrieve.add_argument("size", type=int, default=10)
Event_retrieve.add_argument("filter", type=str, default=["id", "name"], action="split")
#
Event_statistic = reqparse.RequestParser()
Event_statistic.add_argument("format", type=str, choices=("format", "image"))

Weather = reqparse.RequestParser()
Weather.add_argument("date", type=str)

base_url = "http://127.0.0.1:5000/events"
last_update_formate = "%y-%m-%d %H:%M:%S"


class DB_event_Model(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80), unique=True, nullable=False)
    date = db.Column(db.Date, nullable=True)
    from_time = db.Column(db.String(10), nullable=True)
    int_from_time = db.Column(db.Integer, nullable=True)
    to_time = db.Column(db.String(10), nullable=True)
    int_to_time = db.Column(db.Integer, nullable=True)
    street = db.Column(db.String(100), nullable=True)
    suburb = db.Column(db.String(100), nullable=True)
    state = db.Column(db.String(100), nullable=True)
    post_code = db.Column(db.String(100), nullable=True)
    description = db.Column(db.String(1000), nullable=True)
    last_update = db.Column(db.DateTime, default=datetime.now)

    def __repr__(self):
        return "<Event %r>" % self.name


@api.route("/envents")
class Question_1_5(Resource):
    @api.expect(Post_event)
    def post(self):
        args = Post_event.parse_args()
        event_name = args["name"]
        date = args["date"]
        from_time = args["from"]
        to_time = args["to"]
        street = args["street"]
        suburb = args["suburb"]
        state = args["state"]
        post_code = args["post-code"]
        description = args["description"]
        # print((datetime.strptime(to_time, "%H:%M").date()-datetime.strptime(from_time, "%H:%M").date()).days)
        # print(to_time)
        from_ = int(from_time.split(":")[0]) * 60 + int(from_time.split(":")[1])
        int_from_time = from_
        to_ = int(to_time.split(":")[0]) * 60 + int(to_time.split(":")[1])
        int_to_time = to_
        if to_ - from_ <= 0:
            return "Typo error: Event start time must be greater than end time."
        date = datetime.strptime(date, "%d-%m-%Y")
        date_temp = date.strftime("%Y-%m-%d")

        temp = DB_event_Model.query.filter(DB_event_Model.date == date_temp,
                                           or_(
                                               and_(from_ >= DB_event_Model.int_from_time,
                                                    from_ < DB_event_Model.int_to_time),
                                               and_(int_from_time < DB_event_Model.int_from_time,
                                                    int_to_time >= DB_event_Model.int_to_time),
                                               and_(int_to_time < DB_event_Model.int_to_time,
                                                    int_to_time > DB_event_Model.int_from_time)
                                           )).first()
        if temp:
            return "Typo error: The next event start time must be after the previous event end time"

        event = DB_event_Model(name=event_name, date=date, from_time=from_time,
                               int_from_time=int_from_time,
                               to_time=to_time, int_to_time=int_to_time, street=street, suburb=suburb,
                               state=state, post_code=post_code,
                               description=description)
        db.session.add(event)
        db.session.commit()
        event = DB_event_Model.query.filter_by(name=event_name).first()
        aid = event.id
        last_update = event.last_update.strftime(last_update_formate)
        return {"id": aid,
                "last-update": last_update,
                "_links": {
                    "self": {
                        "href": base_url + "/" + str(aid)
                    },
                }
                }, 201

    @api.expect(Event_retrieve)
    def get(self):
        args = Event_retrieve.parse_args()
        order = args["order"]
        page = args["page"]
        size = args["size"]
        filter_by = args["filter"]
        command = []
        filter_command = []
        for i in filter_by:
            if i in ["from", "to"]:
                filter_command.append(eval("DB_event_Model." + i + "_time"))
            else:
                filter_command.append(eval("DB_event_Model." + i))
        for temp in order:
            if temp[1:] in ["from", "to"]:
                if temp[:1] == "+":
                    command.append(eval("DB_event_Model." + "int_" + temp[1:] + "_time" + ".asc()"))
                else:
                    command.append(eval("DB_event_Model." + "int_" + temp[1:] + "_time" + ".desc()"))
            else:
                if temp[:1] == "+":
                    command.append(eval("DB_event_Model." + temp[1:] + ".asc()"))
                else:
                    command.append(eval("DB_event_Model." + temp[1:] + ".desc()"))
        event = db.session.query(*filter_command).order_by(*command).paginate(page=int(page), per_page=int(size))
        res_josion = {"page": page, "page-size": size, "events": [], "_links": {}}
        for item in event.items:
            temp_item = {}
            location = {}
            for i in filter_by:
                if i in ["from", "to"]:
                    temp_item[i] = eval("item." + i + "_time")
                elif i == "date":
                    temp_item[i] = eval("item." + i).strftime("%d-%m-%Y")
                elif i in ["street", "suburb", "state", "postcode"]:
                    location[i] = eval("item." + i)
                else:
                    temp_item[i] = eval("item." + i)
            if len(location) > 0:
                temp_item["location"] = location
            res_josion["events"].append(temp_item)
        order = ",".join(order)
        filter_by = ",".join(filter_by)
        res_josion["_links"]["self"] = {
            "href": base_url + "?" + f"order={order}&page={page}&size={size}&filter={filter_by}"}
        if page > 1:
            res_josion["_links"]["previous"] = {
                "href": base_url + "?" + f"order={order}&page={page - 1}&size={size}&filter={filter_by}"}
        elif page + 1 <= ceil(event.total / size):
            res_josion["_links"]["next"] = {
                "href": base_url + "?" + f"order={order}&page={page + 1}&size={size}&filter={filter_by}"}

        return res_josion, 200


def find_weather(time, state, suburb):
    lat = None
    lng = None
    state_dic = {"NSW": "New South Wales", "QLD": "Queensland", "SA": "South Australia", "TAS": "Tasmania",
                 "VIC": "Victoria", "WA": "Western Australia", "ACT": "Australian Capital Territory",
                 "NT": "Northern Territory"}
    if state in state_dic:
        state = state_dic[state]
    with open(sub, 'r', encoding="utf-8") as p:
        data = p.readlines()
        for line in data[1:]:
            temp = line.split(";")
            temp_res = temp[0].strip() + "," + temp[4].strip() + "," + temp[8].strip()
            if len(temp_res.split(",")) == 4 and temp_res.split(",")[2] == state and temp_res.split(",")[
                3].find(suburb) >= 0:
                lat = temp_res.split(",")[0]
                lng = temp_res.split(",")[1]
                break
    if not lat and not lng:
        return None
    Weathe = f"https://www.7timer.info/bin/civil.php?lat={lat}&lng={lng}.9631&ac=1&unit=metric&output=json&product=two"
    weather_res = requests.get(Weathe)
    print(lat)
    print(lng)
    for i in weather_res.json()["dataseries"]:
        if time < i['timepoint']:
            return i


def find_holdiay(date):
    date_temp = date
    weekend = "false"
    if date_temp.weekday() == 5 or date_temp.weekday() == 6:
        weekend = "true"
    holdiay = f"https://date.nager.at/api/v2/publicholidays/{str(date_temp.year)}/AU"
    holdiay_res = requests.get(holdiay)
    date = date.strftime("%d-%m-%Y")
    day, month, year = date.split("-")
    date = year + "-" + month + "-" + day
    holdiay_name = ""
    for i in holdiay_res.json():
        if i['date'] == date:
            holdiay_name = i['localName']
            break

    return weekend, holdiay_name


@api.route("/events/<int:event_id>")
@api.doc(params={"event_id": "Please Input Event ID"})
class Question_2_3_4(Resource):
    def get(self, event_id):
        event = DB_event_Model.query.get_or_404(event_id)
        prev_id = DB_event_Model.query.order_by(DB_event_Model.date.desc(), DB_event_Model.int_from_time.desc()) \
            .filter(
            or_(DB_event_Model.date < event.date, and_(DB_event_Model.date == event.date,DB_event_Model.int_from_time < event.int_from_time))).first()
        next_id = DB_event_Model.query.order_by(DB_event_Model.date.asc(), DB_event_Model.int_from_time.asc()) \
            .filter(
            or_(DB_event_Model.date > event.date, and_(DB_event_Model.date == event.date,DB_event_Model.int_from_time > event.int_from_time))).first()
        last_update = event.last_update
        last_update = last_update.strftime(last_update_formate)
        return_json = {"id": event_id, "last_update": last_update, "name": event.name,
                       "date": event.date.strftime("%d-%m-%Y"),
                       "from": event.from_time, "to": event.to_time,
                       "location": {"street": event.street, "suburb": event.suburb, "state": event.state,
                                    "post-code": event.post_code},
                       "description": event.description, "_metadate": {}, "_links": {}}
        return_json["_links"]["self"] = {"href": base_url + "/" + str(event_id)}
        if prev_id:
            return_json["_links"]["previous"] = {"href": base_url + "/" + str(prev_id.id)}
        if next_id:
            return_json["_links"]["next"] = {"href": base_url + "/" + str(next_id.id)}
        if (event.date - datetime.now().date()).days <= 8:
            temp_json = find_weather(
                (event.date - datetime.now().date()).days * 24, event.state,
                event.suburb)
            if temp_json:
                return_json["_metadate"]["wind-speed"] = str(temp_json['wind10m']['speed']) + " km"
                return_json["_metadate"]["weather"] = temp_json['weather']
                return_json["_metadate"]["humidity"] = temp_json['rh2m']
                return_json["_metadate"]["temperature"] = str(temp_json['temp2m']) + " C"
        weekend, holdiay_name = find_holdiay(event.date)
        return_json["_metadate"]["holiday"] = holdiay_name
        return_json["_metadate"]["weekend"] = weekend

        return return_json, 200

    def delete(self, event_id):
        event = DB_event_Model.query.get_or_404(event_id)
        return_json = {"message": "The event with id " + str(event.id) + " was removed from the database!",
                       "id": str(event.id)}
        db.session.delete(event)
        db.session.commit()
        return return_json, 200

    @api.expect(Patch_event)
    def patch(self, event_id):
        event = DB_event_Model.query.get_or_404(event_id)
        args = Patch_event.parse_args()
        event_name = args["update name"]
        date = args["update date"]
        from_time = args["update from"]
        to_time = args["update to"]
        street = args["update street"]
        suburb = args["update suburb"]
        state = args["update state"]
        post_code = args["update post-code"]
        description = args["update description"]
        if event_name:
            event.name = event_name
        if date:
            event.date = datetime.strptime(date, "%d-%m-%Y")
        if from_time:
            event.from_time = from_time
            from_ = int(from_time.split(":")[0]) * 60 + int(from_time.split(":")[1])
            event.int_from_time = from_
        if to_time:
            event.to_time = to_time
            to_ = int(to_time.split(":")[0]) * 60 + int(to_time.split(":")[1])
            event.int_to_time = to_
        if street:
            event.street = street
        if suburb:
            event.suburb = suburb
        if state:
            event.state = state
        if post_code:
            event.post_code = post_code
        if description:
            event.description = description
        if from_time or to_time or date:
            if event.int_to_time - event.int_from_time <= 0:
                return "Typo error: Event start time must be greater than end time."
            date_temp = event.date.strftime("%Y-%m-%d")
            temp = DB_event_Model.query.filter(
                and_(DB_event_Model.date == date_temp, DB_event_Model.id != event.id),
                or_(
                    and_(event.int_from_time >= DB_event_Model.int_from_time,
                         event.int_from_time < DB_event_Model.int_to_time),
                    and_(event.int_from_time < DB_event_Model.int_from_time,
                         event.int_to_time >= DB_event_Model.int_to_time),
                    and_(event.int_to_time < DB_event_Model.int_to_time,
                         event.int_to_time > DB_event_Model.int_from_time)
                )).first()
            if temp:
                return "Typo error: The next event start time must be after the previous event end time"
        last_update = datetime.now()
        event.last_update = last_update
        last_update = event.last_update.strftime(last_update_formate)
        db.session.commit()

        return {
            "id": event.id,
            "last-update": last_update,
            "_links": {
                "self": {
                    "href": base_url + "/" + str(event.id)
                }
            }
        }


@api.route("/envents/statistics")
class Question_6(Resource):
    @api.expect(Event_statistic)
    def get(self):
        args = Event_statistic.parse_args()
        res_format = args["format"]
        event = db.session.query(DB_event_Model).order_by(DB_event_Model.date.asc(),
                                                          DB_event_Model.int_from_time.asc()).all()
        group_date = db.session.query(DB_event_Model.date, func.count("*")).group_by(DB_event_Model.date)
        start_week_date = datetime.now().date() - timedelta(days=datetime.now().weekday())
        end_week_date = start_week_date + timedelta(days=7)
        start_month_date = datetime.now().date() - timedelta(days=datetime.now().day - 1)
        end_month_date = start_month_date + timedelta(
            days=calendar.monthrange(datetime.now().year, datetime.now().month)[1] - 1)
        cur_week = db.session.query(func.count(DB_event_Model.id)).where(DB_event_Model.date >= start_week_date,
                                                                         DB_event_Model.date <= end_week_date)
        cur_month = db.session.query(func.count(DB_event_Model.id)).where(DB_event_Model.date >= start_month_date,
                                                                          DB_event_Model.date <= end_month_date)
        total = len(event)
        for i in cur_week:
            print(i)
        res_json = {"total": total, "total-current-week": cur_week[0][0], "total-current-month": cur_month[0][0],
                    "per-days": {}}
        for i in group_date:
            res_json["per-days"][str(i[0])] = i[1]
        if res_format == "image":
            date_x = []
            events_y = []
            for i in group_date:
                date_x.append(str(i[0]))
                events_y.append(i[1])
            plt.figure()
            plt.legend()
            plt.xlabel("Event Date")
            plt.ylabel("Event Count")
            plt.bar(date_x, events_y)
            plt.yticks(range(max(events_y) + 1))
            plt.savefig("Statistic.png")
            res = send_file(os.path.join(basedir, "Statistic.png"))
            return make_response(res, 200)

        return res_json


@api.route("/weather")
class Question_7(Resource):
    @api.expect(Weather)
    def get(self):
        args = Weather.parse_args()
        date = args["date"]
        date = datetime.strptime(date, "%d-%m-%Y")
        date = date.strftime("%Y-%m-%d")
        date = datetime.strptime(date, "%Y-%m-%d")
        if (date.date() - datetime.now().date()).days > 8:
            return f"There is no weather forecast for {date.date()}"
        min_city = ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide", "Gold Coast", "Canberra", "Cranbourne","Newcastle","Wollongong"]
        cit_dict = {}
        with open(au_csv, 'r') as au:
            city = au.readlines()
            count = 0
            for i in city[1:]:
                data = i.split(",")
                if data[0] in min_city:
                    count += 1
                    cit_dict[data[0]] = {"lat": data[1], "lng": data[2]}
                if count == 10:
                    break
        for j in cit_dict.keys():
            lat = cit_dict[j]["lat"]
            lng = cit_dict[j]["lng"]
            weather_api = f"https://www.7timer.info/bin/civil.php?lat={lat}&lng={lng}.9631&ac=1&unit=metric&output=json&product=two"
            weather_res = requests.get(weather_api)

            for i in weather_res.json()["dataseries"]:
                if ((date.date() - datetime.now().date()).days * 24) < i['timepoint']:
                    cit_dict[j]["temperature"] = i["temp2m"]
                    cit_dict[j]["weather"] = i["weather"]
                    break
        world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        plt.rcParams['figure.figsize'] = (12.8, 7.2)
        plt.rcParams['figure.facecolor'] = 'lightskyblue'
        world.plot(color='green')
        plt.xlim(90, 170)
        plt.ylim(-45, -5)
        y = [float(cit_dict[i]["lat"]) for i in cit_dict.keys()]
        x = [float(cit_dict[i]["lng"]) for i in cit_dict.keys()]
        # ["Sydney","Melbourne","Brisbane","Perth","Adelaide","Gold Coast","Canberra","Cranbourne"]
        for i, j in zip(x, y):
            xx, yy = 0, 0
            for m in cit_dict.keys():
                if float(cit_dict[m]["lat"]) == j and float(cit_dict[m]["lng"]) == i:
                    if m == "Adelaide":
                        xx, yy = -200, 50
                    elif m == "Cranbourne":
                        xx, yy = -200, -50
                    elif m == "Perth":
                        xx, yy = -200, -0
                    elif m == "Melbourne":
                        xx, yy = -50, 50
                    elif m == "Brisbane":
                        xx, yy = 0, 50
                    elif m == "Gold Coast":
                        xx, yy = -200, -1
                    elif m == "Canberra":
                        xx, yy = 50, -100
                    elif m == "Sydney":
                        xx, yy = 50, -10
                    elif m == "Newcastle":
                        xx, yy = 50, 50
                    elif m == "Wollongong":
                        xx, yy = 50, -50
                    plt.annotate(
                        m + "\nTemperature: " + str(cit_dict[m]["temperature"]) + " C" + "\nWeather: " + cit_dict[m][
                            "weather"], xy=[i, j], xycoords='data', xytext=(xx, yy), textcoords='offset points',
                        fontsize=10, arrowprops=dict(arrowstyle="->", color='blue'), color='white')
                    break
        plt.axis('off')
        plt.title(f"Weather forecast for major cities in Australia on {date.date()}")
        plt.scatter(x, y, edgecolors="r")
        plt.savefig("Weather.png")
        res = send_file(os.path.join(basedir, "Weather.png"))
        return make_response(res, 200)


if __name__ == '__main__':
    with app.app_context():
        # db.drop_all()
        db.create_all()
    app.run(debug=True)
