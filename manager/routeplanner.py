import logging
import urllib.parse
import urllib.request
import json

class RoutePlanner(object):
    def __init__(self, host, port):
        self.url = 'http://{}:{}'.format(host, port)

    def get_route(self, fromcity, tocity):
        """returns itinerary and total distance for route fromcity to tocity"""
        resp = urllib.request.urlopen(
            self.url + urllib.parse.quote(f'/route/{fromcity}/{tocity}'))
        if resp.getcode() == 200:
            r = json.loads(resp.read().decode())
            try:
                return dict(itinerary=[i['from'] for i in r] +[r[-1]['to']],
                            total=sum([l['weight'] for l in r]))
            except:
                pass
        return dict()  # no route found

    def get_locations(self, itinerary):
        """returns list of (lon,lat) pairs of cities in itinerary"""
        loc = []
        for c in itinerary:
            resp = urllib.request.urlopen(self.url+urllib.parse.quote(f'/city/{c}'))
            if resp.getcode() == 200:
                r = json.loads(resp.read().decode())
                loc.append((r['location']['x'], r['location']['y']))
            else:
                logging.warn(resp)
        return loc

    def get_cities(self):
        """returns a list of countries and cities"""
        d = dict()
        resp = urllib.request.urlopen(self.url+'/countries')
        if resp.getcode() == 200:
            for c in json.loads(resp.read().decode()):
                country = urllib.parse.quote(c)
                resp = urllib.request.urlopen(
                    self.url+urllib.parse.quote(f'/cities/{country}'))
                if resp.getcode() == 200:
                    d[c] = json.loads(resp.read().decode())
        return d