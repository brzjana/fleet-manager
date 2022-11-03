from manager.vehicle import Vehicle
import zmq
import logging

def test_vehicle():
    context = zmq.Context.instance()
    event_url = "inproc://location-events"
    event_receiver = context.socket(zmq.PULL)
    event_receiver.bind(event_url)
    itinerary = ((7.58, 47.57),
                 (7.589388372000748, 47.56500578532769),
                (7.598774959308661, 47.5600107979814))
    v = Vehicle('1', 15.0, (7.58, 47.57), event_url)
    v.start(itinerary)
    num_msgs = 0
    while(True):
        msg = event_receiver.recv()
        logging.info(msg)
        if msg.decode() == v.id:
            break
        num_msgs += 1
    v.join()
    event_receiver.close()
    assert num_msgs == len(itinerary)