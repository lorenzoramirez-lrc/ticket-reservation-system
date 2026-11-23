"""
Archivo: client.py
Objetivo: Definir los clientes utilizados para simular la carga de trabajo
"""

import csv
from datetime import datetime, timezone
import json
import os
import random
import struct
import time
from itertools import cycle

import gevent
from gevent.event import Event
import zmq.green as zmq
from locust import User, task, constant_throughput, events, constant
from locust.exception import StopUser

# Formato de Request (compatible con Request struct de C++)
FORMAT = "<i32s32s32s32si16s64s64s"
RESERVE, CANCEL, QUERY, MODIFY_QUANTITY, MODIFY_OCCURRENCE, CHECK_SEATS, CHECK_RESERVATION, CHECK_OCCURRENCES = range(8)

# Variables de entorno
MANAGER_ADDR = os.environ.get("MANAGER_ADDR", "tcp://10.43.100.20:5555")
BACKUP_MANAGER_ADDR = os.environ.get("BACKUP_MANAGER_ADDR", "tcp://10.43.100.20:5557")
NOTIFICATION_ADDR = os.environ.get("NOTIFICATION_ADDR", "tcp://10.43.99.201:9999")
REQUEST_TIMEOUT_MS = int(os.environ.get("REQUEST_TIMEOUT_MS", 2500))
REQUEST_RETRIES = int(os.environ.get("REQUEST_RETRIES", 3))
REQS_PER_MIN = float(os.environ.get("REQS_PER_MIN", 30))

# Datos de carga (clientes, eventos, ocurrencias y meses), los mismos con los que se llena la BD
with open(os.path.join(os.path.dirname(__file__), "load_data.json")) as f:   
    DATA = json.load(f)

CLIENT_POOL = cycle(DATA["clients"])

# Configuración de escenarios de rendimiento (rendimiento.md)
PERF_SCENARIOS = {
    "1": {"clients": 3, "queries": 2, "reserves": 1},
    "2": {"clients": 5, "queries": 3, "reserves": 2},
    "3": {"clients": 8, "queries": 4, "reserves": 4},
    "scenario_1": {"clients": 3, "queries": 2, "reserves": 1},
    "scenario_2": {"clients": 5, "queries": 3, "reserves": 2},
    "scenario_3": {"clients": 8, "queries": 4, "reserves": 4},
}

# Si SCENARIO está definida, ReserveUser ejecuta ese escenario de reserve_scenarios.json
SCENARIO = os.environ.get("SCENARIO")
SCENARIO_CONFIG = None

if SCENARIO:
    scenarios_file = os.path.join(os.path.dirname(__file__), "reserve_scenarios.json")
    if os.path.exists(scenarios_file):
        with open(scenarios_file, encoding="utf-8") as f:
            scenarios = json.load(f)
        if SCENARIO in scenarios:
            SCENARIO_CONFIG = scenarios[SCENARIO]

RUN_ID = os.environ.get("RUN_ID")
if not RUN_ID:
    RUN_ID = f"run_{SCENARIO or '1'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

RESULTS_DIR = os.environ.get(
    "RESULTS_DIR",
    os.path.join(os.path.dirname(__file__), "..", "..", "tests", "performance", "results")
)

_COORDINATED_START = Event()
_SPAWNED_USERS = 0
_EXPECTED_USERS = PERF_SCENARIOS.get(str(SCENARIO), PERF_SCENARIOS["1"])["clients"]


# buildRequest
# Parametros de entrada: tipo de solicitud, IDs de cliente, evento, ocurrencia y reservación, cantidad de asientos y mes
# Estos parametros (excepto tipo) son opcionales porque no todas las solicitudes usan los mismos parametros, asi que al inicializarlos
# en 0 o strings "vacios" en la firma de la función, se asignan como valores por defecto si no se colocan al llamar la función
# Parametros de salida: bytes (mensaje binario de 152 bytes, con el mismo formato del struct Request de C++)
# Función: Construye el mensaje de una solicitud empaquetando sus parámetros con el formato binario del struct Request.
# Cada texto se trunca a (tamaño - 1) bytes para que siempre quede un '\0' al final, y los campos que no se usan quedan vacíos.
def buildRequest(type, clientID="", eventID="", occurrenceID="", reservationID="", quantity=0, month="", runID="", requestID=""):
    def field(text, size):
        return text.encode()[:size - 1]

    return struct.pack(FORMAT, type, field(clientID, 32), field(eventID, 32), field(occurrenceID, 32),
                       field(reservationID, 32), quantity, field(month, 16),
                       field(runID, 64), field(requestID, 64))


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
        self.reservations = []
        self.manager_addrs = [MANAGER_ADDR, BACKUP_MANAGER_ADDR]
        self.current_manager = 0
        self.pending_cancellations = {}
        self._running = True
        self._connect()
        self._connect_notifications()
        self._notification_greenlet = gevent.spawn(self.receive_notification)

    # _connect
    # Parametros de entrada: ninguno
    # Parametros de salida: ninguno
    # Función: Crea un socket REQ de ZeroMQ y lo conecta al Gestor de Transacciones activo (primario o secundario).
    # Con LINGER en 0, el socket se cierra sin esperar a que se envíen los mensajes pendientes.
    def _connect(self):
        self.sock = self.ctx.socket(zmq.REQ)
        self.sock.setsockopt(zmq.LINGER, 0)
        self.sock.connect(self.manager_addrs[self.current_manager])

    # _switch_manager
    # Parametros de entrada: ninguno
    # Parametros de salida: ninguno
    # Función: Cierra el socket REQ actual (patrón Lazy Pirate) y reconecta alternando entre el gestor primario y el secundario.
    def _switch_manager(self):
        self.sock.close()
        self.current_manager = (self.current_manager + 1) % len(self.manager_addrs)
        print(f"[CLIENTE {self.clientID}] Failover -> reconectando a {self.manager_addrs[self.current_manager]}", flush=True)
        self._connect()

    # _connect_notifications
    # Parametros de entrada: ninguno
    # Parametros de salida: ninguno
    # Función: Crea un socket DEALER de ZeroMQ y lo conecta a la dirección del servicio de notificaciones (NOTIFICATION_ADDR).
    # Configura la identidad del cliente mediante CLIENT_ID para que el ROUTER pueda identificarlo y enviarle las notificaciones correspondientes.
    # Con LINGER en 0, el socket se cierra sin esperar a que se envíen los mensajes pendientes.
    def _connect_notifications(self):
        self.notification_sock = self.ctx.socket(zmq.DEALER)
        self.notification_sock.setsockopt(zmq.LINGER, 0)
        self.notification_sock.setsockopt(zmq.IDENTITY, str(self.clientID).encode("utf-8"))
        self.notification_sock.connect(NOTIFICATION_ADDR)

    # receive_notification
    # Parametros de entrada: ninguno
    # Parametros de salida: ninguno
    # Función: Escucha de forma continua y no bloqueante (en un greenlet) las notificaciones enviadas por el
    # Servicio de Cancelación a través de notification_sock. Cada notificación recibida se decodifica y se
    # entrega a _handle_notification para procesarla. El bucle termina cuando el cliente se detiene o el socket se cierra.
    def receive_notification(self):
        while self._running:
            try:
                notification = self.notification_sock.recv().decode(errors="replace")
            except zmq.ZMQError:
                break
            self._handle_notification(notification)


    # _handle_notification
    # Parametros de entrada: texto de la notificación, con el formato "<id de la reserva>|<resultado>"
    # Parametros de salida: ninguno
    # Función: Busca entre las cancelaciones pendientes (self.pending_cancellations) la reserva a la que se refiere la notificación.
    # Si el resultado empieza con ERROR, la cancelación no se efectuó y la reserva vuelve a self.reservations (sigue activa);
    # si no, la reserva ya está cancelada y se descarta. Si la notificación no tiene el formato esperado, solo se imprime.
    def _handle_notification(self, notification):
        reservationID, separator, result = notification.partition("|")
        if not separator:
            print(f"[CLIENTE {self.clientID}] Notificacion recibida: {notification}", flush=True)
            return

        res = self.pending_cancellations.pop(reservationID, None)
        if res is not None and result.startswith("ERROR"):
            self.reservations.append(res)

        print(f"[CLIENTE {self.clientID}] Notificacion recibida: reserva {reservationID} -> {result}", flush=True)


    # on_stop
    # Parametros de entrada: ninguno
    # Parametros de salida: ninguno
    # Función: Locust lo ejecuta cuando el cliente termina. Detiene el listener de notificaciones y cierra los sockets y el contexto de ZeroMQ para liberar los recursos.
    def on_stop(self):
        print()
        self._running = False
        if getattr(self, "_notification_greenlet", None):
            self._notification_greenlet.kill(block=False)
        self.notification_sock.close()
        self.sock.close()
        self.ctx.term()
        


    # _send
    # Parametros de entrada: mensaje de la solicitud en bytes, construido previamente con buildRequest
    # Parametros de salida: bytes (respuesta del Gestor de Transacciones)
    # Función: Envía la solicitud al Gestor de Transacciones con timeout (patrón Lazy Pirate para Binary Star).
    # Si el gestor actual no responde dentro de REQUEST_TIMEOUT_MS, cierra el socket, cambia al gestor alterno y reintenta.
    def _send(self, data):
        for attempt in range(REQUEST_RETRIES):
            addr = self.manager_addrs[self.current_manager]
            print(f"[CLIENTE {self.clientID}] Enviando {len(data)} bytes a {addr} (intento {attempt + 1}/{REQUEST_RETRIES})", flush=True)

            self.sock.send(data)

            poller = zmq.Poller()
            poller.register(self.sock, zmq.POLLIN)
            socks = dict(poller.poll(REQUEST_TIMEOUT_MS))

            if socks.get(self.sock) == zmq.POLLIN:
                response = self.sock.recv()
                print(f"[CLIENTE {self.clientID}] Respuesta recibida de {addr}", flush=True)
                return response

            print(f"[CLIENTE {self.clientID}] Timeout esperando respuesta de {addr}", flush=True)
            self._switch_manager()

        return b"ERROR: ningun gestor disponible"

    # _query
    # Parametros de entrada: ninguno
    # Parametros de salida: ninguno (la respuesta se recibe, pero no se usa)
    # Función: Elige al azar uno de los meses del archivo de datos y envía, a nombre de este cliente, una solicitud de consulta de los eventos de ese mes.
    def _query(self):
        month = random.choice(DATA["months"])
        response = self._send(buildRequest(QUERY, self.clientID, month=month))
        print(f"cliente={self.clientID}, consulta de {month} -> {response.decode(errors='replace')}") 

    # _reserve
    # Parametros de entrada: ninguno
    # Parametros de salida: ninguno (la respuesta se recibe, pero no se usa)
    # Función: Elige al azar un evento y una de sus ocurrencias del archivo de datos, y envía, a nombre de este cliente,
    # una solicitud de reserva de entre 1 y 4 asientos. Si la reserva se confirma (la respuesta es "OK:<id de la reserva>"),
    # guarda el ID, el evento, la ocurrencia y la cantidad, que se necesitan después para modificarla.
    def _reserve(self):
        event = random.choice(DATA["events"])
        occurrenceID = random.choice(event["occurrences"])
        quantity = random.randint(1, 4)
        response = self._send(buildRequest(RESERVE, self.clientID, event["eventID"],
                                           occurrenceID, quantity=quantity)).decode(errors="replace")
        print(f"cliente={self.clientID}, reserva de {quantity} en {occurrenceID} -> {response}")

        if response.startswith("OK:"):                                                      
            self.reservations.append({"id": response.split(":", 1)[1].strip(),
                                      "eventID": event["eventID"],
                                      "occurrenceID": occurrenceID,
                                      "quantity": quantity})

    # _otherOccurrence
    # Parametros de entrada: reserva (diccionario con id, evento, ocurrencia y cantidad)
    # Parametros de salida: ID de la otra ocurrencia del evento, o None si el evento solo tiene una
    # Función: Cada evento tiene una o dos ocurrencias. Si tiene dos, devuelve la que no es la actual de la reserva.
    def _otherOccurrence(self, res):
        event = next(e for e in DATA["events"] if e["eventID"] == res["eventID"])
        occurrences = event["occurrences"]
        if len(occurrences) < 2:
            return None
        return occurrences[1] if occurrences[0] == res["occurrenceID"] else occurrences[0]

    # _modifyOccurrence
    # Parametros de entrada: reserva a modificar (diccionario con id, evento, ocurrencia y cantidad)
    # Parametros de salida: ninguno
    # Función: Cambia la reserva a la otra ocurrencia de su evento (_otherOccurrence). Envía una solicitud MODIFY_OCCURRENCE
    # y, si el sistema la acepta (la respuesta no empieza con ERROR), actualiza la ocurrencia guardada de la reserva.
    # Si el evento solo tiene una ocurrencia, no envía nada, porque no hay a dónde cambiarla.
    def _modifyOccurrence(self, res):
        newOccurrence = self._otherOccurrence(res)
        if newOccurrence is None:
            print(f"cliente={self.clientID}, reserva {res['id']}: el evento {res['eventID']} solo tiene una ocurrencia")
            return                                  

        response = self._send(buildRequest(MODIFY_OCCURRENCE, self.clientID, res["eventID"], newOccurrence,
                                       reservationID=res["id"])).decode(errors="replace")
        if not response.startswith("ERROR"):
            res["occurrenceID"] = newOccurrence

        print(f"cliente={self.clientID}, reserva {res['id']}: cambio de ocurrencia a {newOccurrence} -> {response}")

    # _modifyQuantity
    # Parametros de entrada: reserva a modificar (diccionario con id, evento, ocurrencia y cantidad), nueva cantidad de asientos (int)
    # Parametros de salida: ninguno
    # Función: Envía una solicitud MODIFY_QUANTITY con la nueva cantidad y, si el sistema la acepta (la respuesta no
    # empieza con ERROR), actualiza la cantidad guardada de la reserva.
    def _modifyQuantity(self, res, newQuantity):
        response = self._send(buildRequest(MODIFY_QUANTITY, self.clientID, res["eventID"], res["occurrenceID"],
                                       reservationID=res["id"], quantity=newQuantity)).decode(errors="replace")
        if not response.startswith("ERROR"):
            res["quantity"] = newQuantity

        print(f"cliente={self.clientID}, reserva {res['id']}: cambio de cantidad a {newQuantity} -> {response}")

    # _cancel
    # Parametros de entrada: reserva a cancelar (diccionario con id, evento, ocurrencia y cantidad)
    # Parametros de salida: ninguno
    # Función: Envía una solicitud CANCEL con el ID de la reserva. La cancelación es asíncrona: el gestor solo confirma que recibió
    # la solicitud, y el resultado real llega después por notificación (_handle_notification). Mientras tanto, la reserva pasa de
    # self.reservations a self.pending_cancellations, para no volver a elegirla. Se mueve antes de enviar porque la notificación
    # podría llegar antes de que este método termine. Si el gestor rechaza la solicitud (ERROR), la reserva vuelve a self.reservations.
    def _cancel(self, res):
        self.reservations.remove(res)
        self.pending_cancellations[res["id"]] = res

        response = self._send(buildRequest(CANCEL, self.clientID, reservationID=res["id"])).decode(errors="replace")

        if response.startswith("ERROR"):
            self.pending_cancellations.pop(res["id"], None)
            self.reservations.append(res)

        print(f"cliente={self.clientID}, solicitud de cancelacion de {res['id']} -> {response}")

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

# Clase ModifyUser
# Cliente que primero realiza una reserva y después modifica una de las reservas
# que él mismo creó (cambiando la cantidad de asientos o la ocurrencia).
# La cantidad de clientes de este tipo se define con la variable de entorno
# MODIFY_USERS (2 por defecto), lo que permite correr al menos dos clientes
# de forma concurrente.
class ModifyUser(BaseClient):
    fixed_count = int(os.environ.get("MODIFY_USERS", 2))

    # reserve_and_modify
    # Parametros de entrada: ninguno
    # Parametros de salida: ninguno
    # Función: Tarea de Locust. Primero realiza una reserva (_reserve). Si la
    # reserva se confirma y se guarda en self.reservations, elige una de ellas
    # al azar y aplica una de las dos variantes de modificación:
    #   - MODIFY_QUANTITY: cambia la cantidad de asientos (entre 1 y 4).
    #   - MODIFY_OCCURRENCE: cambia la reserva a la otra ocurrencia del evento
    #     (si el evento tiene dos ocurrencias).
    @task
    def reserve_and_modify(self):
        self._reserve()

        if not self.reservations:
            return

        res = random.choice(self.reservations)

        if random.random() < 0.5:
            new_quantity = random.randint(1, 4)
            while new_quantity == res["quantity"]:
                new_quantity = random.randint(1, 4)
            self._modifyQuantity(res, new_quantity)
        else:
            self._modifyOccurrence(res)

# Clase CancelUser
# Cliente que primero realiza una reserva y después la cancela.
# La cantidad de clientes de este tipo se define con la variable de entorno
# CANCEL_USERS (2 por defecto), lo que permite correr al menos dos clientes
# de forma concurrente.
class CancelUser(BaseClient):
    fixed_count = int(os.environ.get("CANCEL_USERS", 2))

    # reserve_and_cancel
    # Parametros de entrada: ninguno
    # Parametros de salida: ninguno
    # Función: Tarea de Locust. Primero realiza una reserva (_reserve). Si la
    # reserva se confirma y se guarda en self.reservations, elige una de ellas
    # al azar y la cancela (_cancel).
    @task
    def reserve_and_cancel(self):
        self._reserve()

        if not self.reservations:
            return

        res = random.choice(self.reservations)
        self._cancel(res)


# ==============================================================================
# PerformanceUser: Cliente para evaluación de rendimiento según rendimiento.md
# ==============================================================================

@events.init.add_listener
def on_locust_init(environment, **kwargs):
    """Inicializa metadatos de la corrida de rendimiento para el analizador."""
    if SCENARIO in PERF_SCENARIOS or os.environ.get("RUN_ID"):
        run_dir = os.path.join(RESULTS_DIR, RUN_ID)
        os.makedirs(run_dir, exist_ok=True)
        s_cfg = PERF_SCENARIOS.get(str(SCENARIO), PERF_SCENARIOS["1"])
        metadata = {
            "run_id": RUN_ID,
            "scenario": f"scenario_{SCENARIO}" if str(SCENARIO) in ["1", "2", "3"] else str(SCENARIO),
            "clients": s_cfg["clients"],
            "queries_per_cycle": s_cfg["queries"],
            "reserves_per_cycle": s_cfg["reserves"],
            "manager_addr": MANAGER_ADDR,
            "timestamp_start": datetime.now(timezone.utc).isoformat(),
            "runner": "locust"
        }
        with open(os.path.join(run_dir, "metadata.json"), "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)


class PerformanceUser(User):
    """
    Cliente de rendimiento de Locust para evaluar los escenarios 1, 2 y 3.
    - Ejecuta de forma continua sin pausas artificiales (wait_time nulo).
    - Medición de latencia con time.perf_counter() de alta precisión.
    - Genera CSVs individuales (client_<clientID>.csv) procesables por analyze_metrics.py.
    """
    wait_time = constant(0)

    def on_start(self):
        global _SPAWNED_USERS, _COORDINATED_START, _EXPECTED_USERS
        self.ctx = zmq.Context()
        self.sock = self.ctx.socket(zmq.REQ)
        self.sock.setsockopt(zmq.LINGER, 0)
        self.sock.setsockopt(zmq.RCVTIMEO, 5000)
        self.sock.setsockopt(zmq.SNDTIMEO, 5000)
        self.sock.connect(MANAGER_ADDR)

        self.clientID = next(CLIENT_POOL)
        self.seq_num = 0
        self.records = []
        self.scenario_cfg = PERF_SCENARIOS.get(str(SCENARIO), PERF_SCENARIOS["1"])

        # Coordinación de inicio simultáneo entre clientes
        _SPAWNED_USERS += 1
        if _SPAWNED_USERS >= _EXPECTED_USERS:
            _COORDINATED_START.set()
        else:
            _COORDINATED_START.wait(timeout=10)

    def _execute_req(self, op_code, op_name, event_id="", occ_id="", qty=0, month=""):
        self.seq_num += 1
        req_id = f"{RUN_ID}_{self.clientID}_{self.seq_num:06d}"

        data = buildRequest(
            op_code,
            clientID=self.clientID,
            eventID=event_id,
            occurrenceID=occ_id,
            quantity=qty,
            month=month,
            runID=RUN_ID,
            requestID=req_id
        )

        start_wall = datetime.now(timezone.utc).isoformat()
        t0 = time.perf_counter()
        status = "SYSTEM_ERROR"
        error_msg = ""
        resp_bytes = b""

        try:
            self.sock.send(data)
            resp_bytes = self.sock.recv()
            t1 = time.perf_counter()
            elapsed_ms = (t1 - t0) * 1000.0

            text = resp_bytes.decode("utf-8", errors="replace").strip()
            if text.startswith("OK"):
                status = "SUCCESS"
            elif any(ind in text.lower() for ind in ["insuficientes", "sin cupo", "invalida", "no existe"]) or text.startswith("ERROR:"):
                status = "BUSINESS_ERROR"
                error_msg = text
            else:
                status = "SYSTEM_ERROR"
                error_msg = text

        except zmq.Again:
            t1 = time.perf_counter()
            elapsed_ms = (t1 - t0) * 1000.0
            status = "TIMEOUT"
            error_msg = "ZeroMQ REQ receive timeout"
            self.sock.close()
            self.sock = self.ctx.socket(zmq.REQ)
            self.sock.setsockopt(zmq.LINGER, 0)
            self.sock.setsockopt(zmq.RCVTIMEO, 5000)
            self.sock.setsockopt(zmq.SNDTIMEO, 5000)
            self.sock.connect(MANAGER_ADDR)
        except Exception as e:
            t1 = time.perf_counter()
            elapsed_ms = (t1 - t0) * 1000.0
            status = "SYSTEM_ERROR"
            error_msg = str(e)

        # Disparar evento a Locust para seguimiento en consola/UI
        self.environment.events.request.fire(
            request_type="ZeroMQ",
            name=op_name,
            response_time=elapsed_ms,
            response_length=len(resp_bytes),
            exception=None if status == "SUCCESS" else Exception(error_msg or status),
            context={"status": status}
        )

        # Guardar en buffer para CSV final
        self.records.append({
            "run_id": RUN_ID,
            "client_id": self.clientID,
            "request_id": req_id,
            "operation_type": op_name,
            "start_time": start_wall,
            "response_time_ms": elapsed_ms,
            "status": status,
            "error_message": error_msg
        })

    @task
    def cycle(self):
        """Ejecuta un ciclo completo de consultas y reservas secuenciales."""
        # 1. Consultas del ciclo
        for _ in range(self.scenario_cfg["queries"]):
            month = random.choice(DATA["months"])
            self._execute_req(QUERY, "CONSULTA", month=month)

        # 2. Reservas del ciclo
        for _ in range(self.scenario_cfg["reserves"]):
            event = random.choice(DATA["events"])
            occ_id = random.choice(event["occurrences"])
            qty = random.randint(1, 4)
            self._execute_req(RESERVE, "RESERVA", event_id=event["eventID"], occ_id=occ_id, qty=qty)

    def on_stop(self):
        # Escritura segura a disco fuera de la medición de latencia
        run_dir = os.path.join(RESULTS_DIR, RUN_ID)
        os.makedirs(run_dir, exist_ok=True)
        csv_file = os.path.join(run_dir, f"client_{self.clientID}.csv")
        fieldnames = [
            "run_id", "client_id", "request_id", "operation_type",
            "start_time", "response_time_ms", "status", "error_message"
        ]
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.records)

        self.sock.close()
        self.ctx.term()

