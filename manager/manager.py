import logging
import pyproj  # generic coordinate transformation of geospatial coordinates
from .vehicle import Vehicle
import time
import zmq
import uuid
import threading

geod = pyproj.Geod(ellps='WGS84')
event_url = "inproc://events"
context = zmq.Context().instance()


def get_waypts(a, b):
    """return km spaced list of (lon/lat) pairs between a and b
    Args:
      a: start location
      b: end location"""
    dist = geod.inv(*a, *b)[-1]
    return [a] + geod.npts(*a, *b, 1 + int(dist / 1000))


class FleetManager(object):

    def __init__(self, route_planner,
                 kafka_host='localhost'):
        self.route_planner = route_planner
        self.event_receiver = context.socket(zmq.PULL)
        self.event_receiver.bind(event_url)

        self.vehicles = dict()
        self.kafka_host = kafka_host

    def add_vehicle(self, speed, city):
        loc = self.route_planner.get_locations([city])[0]
        v = Vehicle(str(uuid.uuid4())[:8],
                    speed,
                    loc,
                    event_url, self.kafka_host)
        self.vehicles[v.id] = dict(vehicle=v, city=city)
        return v.id

    def set_job(self, fromcity, tocity):
        """assigns delivery job to vehicle closest to fromcity"""
        cand = []
        for k in self.vehicles:
            if not self.vehicles[k]['vehicle'].is_running:
                if self.vehicles[k]['city'] == fromcity:
                    cand = [(self.vehicles[k],
                             dict(itinerary=[], total=0))]
                    break
                pickup = self.route_planner.get_route(
                    self.vehicles[k]['city'], fromcity)
                if pickup:
                    cand.append((self.vehicles[k], pickup))

        try:
            v, pickup = sorted(cand, key=lambda k: k[1]['total'])[0]
            r = self.route_planner.get_route(fromcity, tocity)
            ity = self.route_planner.get_locations(pickup['itinerary'] + r['itinerary'])
            logging.info("  itinerary %s", ity)
            wp = [x for y in zip(ity, ity[1:])
                  for x in get_waypts(y[0], y[1])]

            v['vehicle'].start(wp + [ity[-1]])

            v['city'] = tocity
            return dict(vehicle=v['vehicle'].id,
                        dist=r['total'], pickup=pickup['total'])
        except Exception:
            logging.exception("no route found")
        return dict()

    def start(self):
        self.thread = threading.Thread(target=self.handle_events,
                                       daemon=True)
        self.__is_active = True
        self.thread.start()

    def stop(self):
        """stop all running vehicles"""
        for k in self.vehicles.keys():
            self.vehicles[k]['vehicle'].stop()

    def handle_events(self):
        while self.__is_active:
            msg = self.event_receiver.recv()
            logging.info(msg)
        # clean up
        self.stop()
        self.event_receiver.close()
        context.term()

    def ready(self):
        return "Yes"

    def join(self):
        try:
            self.thread.join()
        except KeyboardInterrupt:
            logging.info("Terminate")
        self.__is_active = False
        self.thread.join()


if __name__ == '__main__':
    import random
    from .routeplanner import RoutePlanner

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(message)s')

    route_planner = RoutePlanner('localhost', 8080)

    s = FleetManager(route_planner)
    v = [s.add_vehicle(random.randrange(5, 30), 'Basel'),
         s.add_vehicle(random.randrange(5, 30), 'Aarau')]

    s.start()
    logging.info("Ready")
    time.sleep(4)

    #
    vid, d = s.set_job('Basel', 'Aarau')#Lieferauftrag
    logging.info("Assigned %s total dist %f", vid, d)
    time.sleep(1)
    vid, d = s.set_job('Basel', 'Aarau') #weiterer Lieferauftrag
    logging.info("Assigned %s total dist %d", vid, d)

    s.join()