import json
import os

from locust import task

from client import BaseClient, RESERVE, buildRequest


SCENARIO = os.environ.get("SCENARIO", "e2")

CONFIG_PATH = os.path.join(
    os.path.dirname(__file__),
    "reserve_scenarios.json"
)

with open(CONFIG_PATH, encoding="utf-8") as file:
    SCENARIOS = json.load(file)

if SCENARIO not in SCENARIOS:
    raise ValueError(
        f"Escenario desconocido: {SCENARIO}. "
        f"Opciones disponibles: {', '.join(SCENARIOS)}"
    )

CONFIG = SCENARIOS[SCENARIO]

class ReserveUser(BaseClient):

    def on_start(self):
        super().on_start()
        self.finished = False

    @task
    def execute_scenario(self):
        if self.finished:
            return

        mode = CONFIG["mode"]

        if mode == "sequential":
            for reservation in CONFIG["reservations"]:
                print(reservation)
                request = buildRequest(
                    RESERVE,
                    clientID=reservation["clientID"],
                    eventID=reservation["eventID"],
                    occurrenceID=reservation["occurrenceID"],
                    quantity=reservation["quantity"]
                )
                print(request)

                response = self._send(request)
                print(
                    f"Escenario={SCENARIO}, "
                    f"cliente={reservation['clientID']}, "
                    f"cantidad={reservation['quantity']}, "
                    f"respuesta={response!r}"
                )

        elif mode == "concurrent":
            reservation = CONFIG["reservation"]

            request = buildRequest(
                RESERVE,
                clientID=self.clientID,
                eventID=reservation["eventID"],
                occurrenceID=reservation["occurrenceID"],
                quantity=reservation["quantity"]
            )

            print(request)
            response = self._send(request)
            print(
                f"Escenario={SCENARIO}, "
                f"cliente={self.clientID}, "
                f"cantidad={reservation['quantity']}, "
                f"respuesta={response!r}"
            )

        self.finished = True

