import cherrypy
import json
import redis
import uuid
from datetime import datetime
from redis.commands.json.path import Path

REDIS_HOST = 'redis-10930.c1.us-east1-2.gce.cloud.redislabs.com'
REDIS_PORT = 10930
REDIS_USERNAME = 'default'
REDIS_PASSWORD = 'team4password'

# Connect to Redis server
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, username=REDIS_USERNAME, password=REDIS_PASSWORD)
is_connected = redis_client.ping()
print('Redis Connected:', is_connected)


# endpoint /status
class Status(object):
    exposed = True

    def GET(self, *path, **query):
        response_dict = {
            'status': 'online'
        }
        response = json.dumps(response_dict)

        return response


# ENDPOINTS:

# /devices

class retrieveDevices(object):
    exposed = True

    def GET(self, *path, **query):

        keys_battery = redis_client.keys('*:battery')
        keys_plugged = redis_client.keys('*:power')
        items_battery = {}
        items_power = {}
        items = []

        threshold = query.get('blt', None)
        if threshold is not None:
            threshold = int(threshold)

        plugged = query.get('plugged', None)

        if plugged is not None:
            plugged = int(plugged)

        for key in keys_battery:
            mac_address = key.decode('utf-8').split(':')[0]
            # Ottieni l'ultima entry della serie
            latest_entry = redis_client.ts().get(key)
            battery_level = latest_entry[1]
            items_battery[mac_address] = battery_level

        for key in keys_plugged:
            mac_address = key.decode('utf-8').split(':')[0]

            # Ottieni l'ultima entry della serie
            latest_entry = redis_client.ts().get(key)
            power = latest_entry[1]
            items_power[mac_address] = power

        if threshold is None and plugged is None:
            items = list(set(items_battery.keys()).union(set(items_power.keys())))

        if threshold is not None and plugged is None:
            filtered_battery = {key: value for key, value in items_battery.items() if value <= threshold}
            items = list(filtered_battery.keys())

        if plugged is not None and threshold is None:
            filtered_power = {key: value for key, value in items_power.items() if value == plugged}
            items = list(filtered_power.keys())

        if threshold is not None and plugged is not None:
            filtered_battery = {key: value for key, value in items_battery.items() if value <= threshold}
            filtered_power = {key: value for key, value in items_power.items() if value == plugged}
            for key in filtered_battery.keys():
                if key in filtered_power.keys():
                    items.append(key)

        json_data = {
            "mac_addresses": items
        }

        response = json.dumps(json_data)
        return response

def is_iso_format(date_str):
        try:
            # Try to parse the string as ISO format
            datetime.fromisoformat(date_str)
            return True
        except ValueError:
            return False
            
class StatusDevice(object):
    exposed = True

    def GET(self, mac_address, **query):

        if len(mac_address) < 1:
            raise cherrypy.HTTPError(400, 'Bad Request: missing MAC address.')

        bat = ":battery"
        pow = ":power"
        id_battery = mac_address + bat
        id_power = mac_address + pow           

        start_date = query.get('start_date', None)
        end_date = query.get('end_date', None)

        if start_date is not None:
            if is_iso_format(start_date):
                # Convert ISO string to datetime object
                iso_datetime = datetime.fromisoformat(start_date)
                # Get the timestamp in milliseconds
                start_timestamp = int(iso_datetime.timestamp() * 1000)
            else:
                raise cherrypy.HTTPError(400, 'Bad Request: wrong format for start date.')   

        else:
            raise cherrypy.HTTPError(400, 'Bad Request: missing start date.')

        if end_date is not None:
            if is_iso_format(end_date):
                # Convert ISO string to datetime object
                iso_datetime = datetime.fromisoformat(end_date)
                # Get the timestamp in milliseconds
                end_timestamp = int(iso_datetime.timestamp() * 1000)
            else: 
                raise cherrypy.HTTPError(400, 'Bad Request: wrong format for end date.')

        else:
            raise cherrypy.HTTPError(400, 'Bad Request: missing end date.')
        

        if end_timestamp <= start_timestamp:
            raise cherrypy.HTTPError(400, 'Bad Request: end date smaller or equal than start date.')


        
        if redis_client.exists(id_battery) and redis_client.exists(id_power):
            # Retrieve entries within the specified date range
            entries_battery = redis_client.ts().range(id_battery, '-', '+')
            entries_power = redis_client.ts().range(id_power, '-', '+')
        else:
            raise cherrypy.HTTPError(404, 'Not Found: invalid MAC address.')
        
        # Extract timestamps, battery_levels, and power_plugged from the entries
        timestamps = [entry[0] for entry in entries_battery]
        battery_levels = [entry[1] for entry in entries_battery]
        power_plugged = [entry[1] for entry in entries_power]

        # Create the response dictionary
        response_dict = {
            'mac_address': mac_address,
            'timestamps': timestamps,
            'battery_levels': battery_levels,
            'power_plugged': power_plugged
        }

        filtered_dict = {
            'mac_address': response_dict['mac_address'],
            'timestamps': [ts for ts in response_dict['timestamps'] if ts >= start_timestamp and ts <= end_timestamp],
            'battery_levels': [bl for ts, bl in zip(response_dict['timestamps'], response_dict['battery_levels']) if
                               ts >= start_timestamp and ts <= end_timestamp],
            'power_plugged': [pp for ts, pp in zip(response_dict['timestamps'], response_dict['power_plugged']) if
                              ts >= start_timestamp and ts <= end_timestamp],
        }

        # Convert the dictionary to a JSON string
        response = json.dumps(filtered_dict)

        return response

class DeleteDevice(object):
    exposed = True
    _cp_config = {"request.methods_with_bodies": ('POST', 'PUT', 'PATCH', 'DELETE')}

    @cherrypy.expose
    def DELETE(self, mac_address):

        print("we")
        bat = ":battery"
        pow = ":power"
        id_battery = mac_address + bat
        id_power = mac_address + pow

        if len(mac_address) < 1:
            raise cherrypy.HTTPError(400, 'Bad Request: missing MAC address.')

        series1 = redis_client.delete(id_battery)
        series2 = redis_client.delete(id_power)
        
        if series1 == 0 or series2 == 0:
            raise cherrypy.HTTPError(404, 'Not Found: invalid MAC address.')

        return 'ok'


if __name__ == '__main__':
    conf = {'/': {'request.dispatch': cherrypy.dispatch.MethodDispatcher()}}
    cherrypy.tree.mount(Status(), '/BatteryStatus', conf)
    cherrypy.tree.mount(retrieveDevices(), '/devices', conf)
    cherrypy.tree.mount(StatusDevice(), '/device', conf)
    cherrypy.tree.mount(DeleteDevice(), '/device/{mac_address}/', conf)
    cherrypy.config.update({'server.socket_host': '0.0.0.0'})
    cherrypy.config.update({'server.socket_port': 8080})
    cherrypy.engine.start()
    cherrypy.engine.block()
