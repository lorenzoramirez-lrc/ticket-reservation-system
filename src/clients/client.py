import json
import os
import random
import struct
from itertools import cycle
import zmq.green as zmq     
from locust import User, task, constant_throughput


FORMAT = "<i32s32s32s32si16s"
RESERVE, CANCEL, QUERY, MODIFY_QUANTITY, MODIFY_OCCURRENCE, CHECK_SEATS, CHECK_RESERVATION, CHECK_OCCURRENCES = range(8)
MANAGER_ADDR = os.environ.get("MANAGER_ADDR", "tcp://10.43.99.201:5555")
REQS_PER_MIN = float(os.environ.get("REQS_PER_MIN", 30))     
QUERY_USERS = int(os.environ.get("QUERY_USERS", 2))         
MIXED_USERS = int(os.environ.get("MIXED_USERS", 1))         
MIXED_QUERY_PCT = float(os.environ.get("MIXED_QUERY_PCT", 50))  

with open(os.path.join(os.path.dirname(__file__), "load_data.json")) as f:
    DATA = json.load(f)

CLIENT_POOL = cycle(DATA["clients"])

def buildRequest(type, clientID="", eventID="", occurrenceID="", reservationID="", quantity=0, month=""):
    def field(text, size):
        return text.encode()[:size - 1]

    return struct.pack(FORMAT, type, field(clientID, 32), field(eventID, 32), field(occurrenceID, 32), 
                       field(reservationID, 32), quantity, field(month, 16))


class BaseClient(User):
    abstract = True
    wait_time = constant_throughput(REQS_PER_MIN / 60)  

    def on_start(self):
        self.ctx = zmq.Context()
        self.clientID = next(CLIENT_POOL)
        self._connect()

    def _connect(self):
        self.sock = self.ctx.socket(zmq.REQ)
        self.sock.setsockopt(zmq.LINGER, 0)
        self.sock.setsockopt(zmq.RCVTIMEO, 5000)
        self.sock.connect(MANAGER_ADDR)

    def on_stop(self):
        self.sock.close()
        self.ctx.term()

    def _send(self, data):
        try:
            self.sock.send(data)
            self.sock.recv()
        except zmq.Again:
            self.sock.close()
            self._connect()

    def _query(self):
        month = random.choice(DATA["months"])
        self._send(buildRequest(QUERY, self.clientID, month=month))

    def _reserve(self):
        event = random.choice(DATA["events"])
        occurrenceID = random.choice(event["occurrences"])
        self._send(buildRequest(RESERVE, self.clientID, event["eventID"],
                                           occurrenceID, quantity=random.randint(1, 4)))


class QueryUser(BaseClient):
    fixed_count = QUERY_USERS

    @task
    def query(self):
        self._query()


class MixedUser(BaseClient):
    fixed_count = MIXED_USERS

    @task
    def mixed(self):
        if random.uniform(0, 100) < MIXED_QUERY_PCT:
            self._query()
        else:
            self._reserve()
