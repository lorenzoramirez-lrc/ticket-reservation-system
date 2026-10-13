"""
Archivo: client.py
Objetivo: Definir los clientes utilizados para simular la carga de trabajo
"""

import json
import os
import random
import struct
from itertools import cycle

import zmq.green as zmq
from locust import User, task, constant_throughput
from locust.exception import StopUser

# Formato de Request
FORMAT = "<i32s32s32s32si16s"
RESERVE, CANCEL, QUERY, MODIFY_QUANTITY, MODIFY_OCCURRENCE, CHECK_SEATS, CHECK_RESERVATION, CHECK_OCCURRENCES = range(8)

# Variables de entorno
MANAGER_ADDR = os.environ.get("MANAGER_ADDR", "tcp://10.43.100.20:5555")
REQS_PER_MIN = float(os.environ.get("REQS_PER_MIN", 30))

# Datos de carga (clientes, eventos, ocurrencias y meses), los mismos con los que se llena la BD
with open(os.path.join(os.path.dirname(__file__), "load_data.json")) as f:   
    DATA = json.load(f)

CLIENT_POOL = cycle(DATA["clients"])

# Si SCENARIO está definida, ReserveUser ejecuta ese escenario de reserve_scenarios.json
SCENARIO = os.environ.get("SCENARIO")
SCENARIO_CONFIG = None

if SCENARIO:
    with open(os.path.join(os.path.dirname(__file__), "reserve_scenarios.json"), encoding="utf-8") as f:
        scenarios = json.load(f)

    if SCENARIO not in scenarios:
        raise ValueError(f"Escenario desconocido: {SCENARIO}. ")
    SCENARIO_CONFIG = scenarios[SCENARIO]


# buildRequest
# Parametros de entrada: tipo de solicitud, IDs de cliente, evento, ocurrencia y reservación, cantidad de asientos y mes
# Estos parametros (excepto tipo) son opcionales porque no todas las solicitudes usan los mismos parametros, asi que al inicializarlos
# en 0 o strings "vacios" en la firma de la función, se asignan como valores por defecto si no se colocan al llamar la función
# Parametros de salida: bytes (mensaje binario de 152 bytes, con el mismo formato del struct Request de C++)
# Función: Construye el mensaje de una solicitud empaquetando sus parámetros con el formato binario del struct Request.
# Cada texto se trunca a (tamaño - 1) bytes para que siempre quede un '\0' al final, y los campos que no se usan quedan vacíos.
def buildRequest(type, clientID="", eventID="", occurrenceID="", reservationID="", quantity=0, month=""):
    def field(text, size):
        return text.encode()[:size - 1]

    return struct.pack(FORMAT, type, field(clientID, 32), field(eventID, 32), field(occurrenceID, 32),
                       field(reservationID, 32), quantity, field(month, 16))


# Clase abstracta BaseClient
# Contiene los metodos por defecto del cliente
# El atributo wait_time fija el ritmo de cada cliente (REQS_PER_MIN solicitudes por minuto) y abstract = True
# evita que Locust la use directamente como un cliente simulado
class BaseClient(User):
    abstract = True
    wait_time = constant_throughput(REQS_PER_MIN / 60)

    # on_start
    # Parametros de entrada: ninguno (self es el cliente simulado)
    # Parametros de salida: ninguno
    # Función: Locust lo ejecuta una vez, cuando se crea cada cliente simulado. Crea el contexto de ZeroMQ,
    # le asigna a este cliente el siguiente ID de la lista de clientes (CLIENT_POOL) y abre la conexión con el Gestor de Transacciones.
    def on_start(self):
        self.ctx = zmq.Context()
        self.clientID = next(CLIENT_POOL)
        self._connect()

    # _connect
    # Parametros de entrada: ninguno
    # Parametros de salida: ninguno
    # Función: Crea un socket REQ de ZeroMQ y lo conecta a la dirección del Gestor de Transacciones (MANAGER_ADDR).
    # Con LINGER en 0, el socket se cierra sin esperar a que se envíen los mensajes pendientes.
    def _connect(self):
        self.sock = self.ctx.socket(zmq.REQ)
        self.sock.setsockopt(zmq.LINGER, 0)
        self.sock.connect(MANAGER_ADDR)

    # on_stop
    # Parametros de entrada: ninguno
    # Parametros de salida: ninguno
    # Función: Locust lo ejecuta cuando el cliente termina. Cierra el socket y el contexto de ZeroMQ para liberar los recursos.
    def on_stop(self):
        print()
        self.sock.close()
        self.ctx.term()

    # _send
    # Parametros de entrada: mensaje de la solicitud en bytes, construido previamente con buildRequest
    # Parametros de salida: bytes (respuesta del Gestor de Transacciones)
    # Función: Envía la solicitud al Gestor de Transacciones y espera la respuesta (patrón REQ/REP, la espera es bloqueante).
    # Imprime mensajes de seguimiento al enviar y al recibir.
    def _send(self, data):

        print(f"[CLIENTE] Enviando {len(data)} bytes a {MANAGER_ADDR}", flush=True)

        self.sock.send(data)
        print("[CLIENTE] Solicitud enviada", flush=True)

        response = self.sock.recv()

        print("[CLIENTE] Respuesta recibida", flush=True)
        return response

    # _query
    # Parametros de entrada: ninguno
    # Parametros de salida: ninguno (la respuesta se recibe, pero no se usa)
    # Función: Elige al azar uno de los meses del archivo de datos y envía, a nombre de este cliente, una solicitud de consulta de los eventos de ese mes.
    def _query(self):
        month = random.choice(DATA["months"])
        response = self._send(buildRequest(QUERY, self.clientID, month=month))
        print(f"cliente={clientID}, {response}")

    # _reserve
    # Parametros de entrada: ninguno
    # Parametros de salida: ninguno (la respuesta se recibe, pero no se usa)
    # Función: Elige al azar un evento y una de sus ocurrencias del archivo de datos, y envía, a nombre de este cliente,
    # una solicitud de reserva de entre 1 y 4 asientos.
    def _reserve(self):
        event = random.choice(DATA["events"])
        occurrenceID = random.choice(event["occurrences"])
        self._send(buildRequest(RESERVE, self.clientID, event["eventID"],
                                occurrenceID, quantity=random.randint(1, 4)))


# Clase QueryUser
# Cliente simulado que solo realiza consultas de eventos
# La cantidad de clientes de este tipo se define con la variable de entorno QUERY_USERS (2 por defecto)
class QueryUser(BaseClient):
    fixed_count = int(os.environ.get("QUERY_USERS", 2))

    # query
    # Parametros de entrada: ninguno
    # Parametros de salida: ninguno
    # Función: Tarea de Locust. Cada vez que Locust la ejecuta, el cliente hace una consulta de eventos (llama a _query).
    @task
    def query(self):
        self._query()


# Clase MixedUser
# Cliente simulado que alterna consultas de eventos y reservas
# La cantidad de clientes de este tipo se define con la variable de entorno MIXED_USERS (1 por defecto) y el
# porcentaje de consultas con MIXED_QUERY_PCT (50 por defecto), el resto de las solicitudes son reservas
class MixedUser(BaseClient):
    fixed_count = int(os.environ.get("MIXED_USERS", 1))
    query_pct = float(os.environ.get("MIXED_QUERY_PCT", 50))

    # mixed
    # Parametros de entrada: ninguno
    # Parametros de salida: ninguno
    # Función: Tarea de Locust. Genera un número aleatorio entre 0 y 100: si es menor que el porcentaje de consultas,
    # el cliente hace una consulta (_query), y si no, una reserva (_reserve).
    @task
    def mixed(self):
        if random.uniform(0, 100) < self.query_pct:
            self._query()
        else:
            self._reserve()


# Clase ReserveUser
# Cliente que realiza reservas. Su comportamiento depende de la variable de entorno SCENARIO:
# - Con SCENARIO definida (por ejemplo e1 o e2): ejecuta una vez ese escenario de reserve_scenarios.json
#   y termina. La cantidad de clientes la define -u de Locust.
# - Sin SCENARIO: reserva de forma repetida un evento y una cantidad al azar (_reserve), como los demás clientes de carga.
#   La cantidad de clientes se define con RESERVE_USERS (1 por defecto).
class ReserveUser(BaseClient):
    if SCENARIO_CONFIG:

        # execute_scenario
        # Parametros de entrada: ninguno
        # Parametros de salida: ninguno
        # Función: Tarea de Locust. En modo "sequential" envía en orden todas las reservas del escenario (cada una con el
        # clientID indicado en el JSON); en modo "concurrent" envía una sola reserva con el ID de este cliente.
        # Al terminar lanza StopUser, para que este cliente no repita el escenario.
        @task
        def execute_scenario(self):
            if SCENARIO_CONFIG["mode"] == "sequential":
                for reservation in SCENARIO_CONFIG["reservations"]:
                    self._reserve_and_report(reservation["clientID"], reservation)
            elif SCENARIO_CONFIG["mode"] == "concurrent":
                self._reserve_and_report(self.clientID, SCENARIO_CONFIG["reservation"])

            raise StopUser()

        # _reserve_and_report
        # Parametros de entrada: ID del cliente que reserva, diccionario de la reserva (evento, ocurrencia y cantidad)
        # Parametros de salida: ninguno
        # Función: Envía una solicitud de reserva con esos datos, espera la respuesta del sistema e imprime el resultado.
        def _reserve_and_report(self, clientID, reservation):
            request = buildRequest(RESERVE, clientID=clientID, eventID=reservation["eventID"], occurrenceID=reservation["occurrenceID"],
                                   quantity=reservation["quantity"])
            response = self._send(request).decode(errors="replace")
            print(f"Escenario={SCENARIO}, cliente={clientID}, "
                  f"cantidad={reservation['quantity']} -> {response}")

    else:
        fixed_count = int(os.environ.get("RESERVE_USERS", 1))

        # reserve
        # Parametros de entrada: ninguno
        # Parametros de salida: ninguno
        # Función: Tarea de Locust. Cada vez que Locust la ejecuta, el cliente hace una reserva al azar (llama a _reserve).
        @task
        def reserve(self):
            self._reserve()
