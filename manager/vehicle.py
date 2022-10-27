import time
import logging
import threading
import json
import zmq
import kafka

context = zmq.Context().instance()


class Vehicle(object):

    def __init__(self, id, speed, location, sink_url, event_streamer={}):
        self.id = id
        self.location = location
        self.speed = speed
        self.departure = 0
        self.scale_factor = 25
        self.sink_url = sink_url
        self.sender = context.socket(zmq.PUSH)
        self.sender.connect(self.sink_url)
        self.is_running = False
        try:
            host = event_streamer.get('host') or 'localhost'
            port = event_streamer.get('port') or 9092
            servers = f'{host}:{port}'
            logging.info(f'bootstrap_servers={servers}')
            self.producer = kafka.KafkaProducer(bootstrap_servers=f'{servers}',
                                                value_serializer=lambda v: json.dumps(v).encode())
            self.topic = 'vehicle-positions'
            logging.info(f'bootstrap_servers={servers}')
        except:
            logging.exception("no event streaming")

            self.topic = False

    def start(self, itinerary):
        """set itinerary (list of ordered waypoints) and start voyage"""
        self.thread = threading.Thread(target=self.run, args=(itinerary,),
                                       daemon=True)
        self.is_running = True
        self.thread.start()

    def stop(self):
        self.is_running = False
        self.sender.close()

    def run(self, itinerary):
        """calculates distance and moves to new position
        returns if end of itinerary is reached
        """
        s = 0
        self.departure = time.time()
        time_step = 1000 / self.speed / self.scale_factor
        while self.is_running:
            try:
                dt = time.time() - self.departure
                s = self.speed * dt * self.scale_factor
                self.location = itinerary[int(round(s / 1000))]
                msg = dict(location=self.location,
                           id=self.id)
                if self.topic:
                    self.producer.send(self.topic, msg)
                self.sender.send_json(msg)

                time.sleep(time_step)
            except IndexError:
                self.is_running = False

        logging.info("Vehicle {0} stopped: pos {1} dist {2:5.1f}".format(
            self.id, self.location, s / 1000))
        self.departure = 0
        self.sender.send_string("{}".format(self.id))

    def join(self):
        self.thread.join()