/*
Archivo: servicio-persistencia.cpp
Objetivo: Atender solicitudes concurrentes de RMC y Cancelación con conexiones a PostgreSQL exclusivas de cada trabajador.
Funciones: sendResponse, checkSeats, checkReservation, checkOccurrence, queryEvents, reserveSeats, modifyQuantity, modifyOccurrence, cancelReservation, executeIdempotent, processRequest, runWorker, main
*/

#include <cerrno>
#include <cctype>
#include <exception>
#include <functional>
#include <mutex>
#include <thread>
#include <type_traits>
#include <vector>
#include <stdexcept>
#include <string>
#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <zmq.hpp>
#include <zmq_addon.hpp>
#include <pqxx/pqxx>
#include "../request-structs/structs.cpp"

constexpr int PERSISTENCE_WORKERS = 4;
constexpr char WORKER_ENDPOINT[] = "inproc://persistence-workers";

/* writeLog
Parametros de entrada: flujo de salida (ostream), mensaje (String)
Parametros de salida: Ninguno
Función: Escribe el mensaje completo bajo un mutex para que los trabajadores no mezclen sus registros.
*/
static void writeLog(std::ostream &output, const std::string &message) {
    static std::mutex mutex;
    std::lock_guard<std::mutex> lock(mutex);
    output << message << std::endl;
}

static std::string err(const std::string& m) { return "ERROR: " + m; }


/* sendResponse
Parametros de entrada: socket del zmq (zmq), respuesta (String)
Parametros de salida: Ninguno
Función: Envia la respuesta (reply) al socket correspondiente.
*/
void sendResponse(zmq::socket_t &socket, const std::string &response) {
    zmq::message_t reply(response.data(), response.size());
    socket.send(reply, zmq::send_flags::none);
}


/* checkSeats
Parametros de entrada: conexión con la db, id del evento (String), id de la ocurrencia (String)
Parametros de salida: String (El numero de asientos en la ocurrencia)
Función: Retorna el numero de asientos disponible en una ocurrencia específica, además de asegurarse de la existencia de esta. En caso de no encontrarla retorna un error.
*/
std::string checkSeats(pqxx::connection& db, const std::string& eventID,
                       const std::string& occurrenceID) {
    pqxx::nontransaction tx(db);
    auto r = tx.exec_params(
        "SELECT capacity - occupied_seats AS free_seats FROM event_occurrences "
        "WHERE occurrence_id = $1 AND event_id = $2", occurrenceID, eventID);
    if (r.empty()) return err("la ocurrencia no existe para ese evento");
    return std::to_string(r[0]["free_seats"].as<int>());
}

/* checkReservation
Parametros de entrada: conexión con la db, id de la reserva (String), id del cliente (String)
Parametros de salida: String (El numero de asientos de la reserva)
Función: Busca una reserva en la base de datos y en caso de que exista retorna el numero de asientos reservados. En caso contrario retorna un error.
*/
std::string checkReservation(pqxx::connection& db, const std::string& reservationID,
                             const std::string& clientID) {
    pqxx::nontransaction tx(db);
    auto r = tx.exec_params(
        "SELECT seat_count FROM reservations "
        "WHERE reservation_id = $1 AND client_id = $2 AND status = 'ACTIVE'",
        reservationID, clientID);
    if (r.empty()) return err("la reserva no existe, esta cancelada o no es del cliente");
    return std::to_string(r[0]["seat_count"].as<int>());
}

/* checkOccurrence
Parametros de entrada: conexión con la db, id del evento (String), id de la ocurrencia (String)
Parametros de salida: String (OK si existe, Error si no)
Función: Revisa si una ocurrencia existe o no.
*/
std::string checkOccurrence(pqxx::connection& db, const std::string& eventID,
                            const std::string& occurrenceID) {
    pqxx::nontransaction tx(db);
    auto r = tx.exec_params(
        "SELECT 1 FROM event_occurrences WHERE occurrence_id = $1 AND event_id = $2",
        occurrenceID, eventID);
    if (r.empty()) return err("la ocurrencia no existe para ese evento");
    return "OK";
}


/* queryEvents
Parametros de entrada: conexión con la db, mes a evaluar (valor numérico entero int)
Parametros de salida: String (todas las ocurrencias en el mes en formato CSV)
Función: Busca todas las ocurrencias en un mes y las retorna en formato CSV en orden cronológico.
*/
std::string queryEvents(pqxx::connection& db, int month) {
    pqxx::nontransaction tx(db);
    auto r = tx.exec_params(
        "SELECT o.event_id, o.occurrence_id, e.name, o.date_time, "
        "       o.capacity - o.occupied_seats AS free_seats "
        "FROM event_occurrences o JOIN events e ON e.event_id = o.event_id "
        "WHERE EXTRACT(MONTH FROM o.date_time) = $1 "
        "ORDER BY o.date_time, o.event_id", month);
    std::ostringstream out;
    out << "OK\n";
    for (const auto& row : r)
        out << row["event_id"].as<std::string>() << ';'
            << row["occurrence_id"].as<std::string>() << ';'
            << row["name"].as<std::string>() << ';'
            << row["date_time"].as<std::string>() << ';'
            << row["free_seats"].as<int>() << '\n';
    return out.str();
}

/* reserveSeats
Parametros de entrada: transacción de PostgreSQL (transaction_base), id del cliente (String), id del evento (String), id de la ocurrencia (String), cantidad de asientos a reservar (int)
Parametros de salida: String (OK con el id de reserva si se pudo, error en caso contrario)
Función: Revisa la disponibilidad de cupos de una ocurrencia en determinado evento y dependiendo de esta hace una reserva o no.
*/
std::string reserveSeats(pqxx::transaction_base& tx, const std::string& clientID,
                         const std::string& eventID, const std::string& occurrenceID,
                         int quantity) {
    if (quantity <= 0) return err("cantidad invalida");

    auto upd = tx.exec_params
    (   "UPDATE event_occurrences SET occupied_seats = occupied_seats + $1 "
        "WHERE occurrence_id = $2 AND event_id = $3 AND occupied_seats + $1 <= capacity", quantity, occurrenceID, eventID);
    if (upd.affected_rows() != 1)
        return err("sin cupo o la ocurrencia no existe");   

    auto ins = tx.exec_params(
        "INSERT INTO reservations(reservation_id, occurrence_id, client_id, seat_count) "
        "VALUES (substr(replace(gen_random_uuid()::text, '-', ''), 1, 16), $1, $2, $3) RETURNING reservation_id",
        occurrenceID, clientID, quantity);
    auto id = ins[0]["reservation_id"].as<std::string>();

    return "OK:" + id;
}


/* modifyQuantity
Parametros de entrada: transacción de PostgreSQL (transaction_base), id de la reserva (String), id del cliente (String), id del evento (String), id de la ocurrencia (String), nueva cantidad de asientos (int)
Parametros de salida: String (OK si se pudo, Error si no)
Función: Revisa si es posible cambiar la cantidad de asientos reservados en un pedido y realiza el cambio en caso de que sea posible. 
*/
std::string modifyQuantity(pqxx::transaction_base& tx, const std::string& reservationID,
                           const std::string& clientID, const std::string& eventID,
                           const std::string& occurrenceID, int newQuantity) {
    if (newQuantity<=0) return err("cantidad invalida");

    auto cur =tx.exec_params(
        "SELECT r.seat_count FROM reservations r "
        "JOIN event_occurrences o ON o.occurrence_id = r.occurrence_id "
        "WHERE r.reservation_id = $1 AND r.client_id = $2 AND r.status = 'ACTIVE' "
        "  AND r.occurrence_id = $3 AND o.event_id = $4 FOR UPDATE OF r", reservationID, clientID, occurrenceID, eventID);
    if (cur.empty()) return err("la reserva no existe, esta cancelada o no coincide");

    int diff =newQuantity-cur[0]["seat_count"].as<int>();
    if (diff==0) return err("No se modifico el numero de asientos");

    auto upd =tx.exec_params(
        "UPDATE event_occurrences SET occupied_seats = occupied_seats + $1 "
        "WHERE occurrence_id = $2 AND occupied_seats + $1 <= capacity",
        diff, occurrenceID);
    if (upd.affected_rows() != 1) return err("sin cupo para la nueva cantidad");

    auto updR =tx.exec_params("UPDATE reservations SET seat_count = $2 WHERE reservation_id = $1", reservationID, newQuantity);
    if (updR.affected_rows()!=1) return err("no se pudo cambiar el numero de asientos");

    return "OK";
}

/* modifyOccurrence
Parametros de entrada: transacción de PostgreSQL (transaction_base), id de la reserva (String), id del cliente (String), id del evento (String), id de la ocurrencia (String)
Parametros de salida: String (OK si se pudo, Error si no)
Función: Mueve la reserva entre ocurrencias del evento. Bloquea ambas por ID ascendente para evitar interbloqueos cuando dos operaciones mueven reservas en sentidos opuestos.
*/
std::string modifyOccurrence(pqxx::transaction_base& tx, const std::string& reservationID,
                             const std::string& clientID, const std::string& eventID,
                             const std::string& occurrenceID) {

    auto cur = tx.exec_params(
        "SELECT r.occurrence_id, r.seat_count FROM reservations r "
        "JOIN event_occurrences o ON o.occurrence_id = r.occurrence_id "
        "WHERE r.reservation_id = $1 AND r.client_id = $2 AND r.status = 'ACTIVE' "
        "  AND o.event_id = $3 FOR UPDATE OF r", reservationID, clientID, eventID);

    if (cur.empty()) return err("la reserva no existe, esta cancelada o no es de ese evento");

    auto oldOcc = cur[0]["occurrence_id"].as<std::string>();
    int  seats  = cur[0]["seat_count"].as<int>();
    if (oldOcc == occurrenceID) return err("la reserva ya esta en esa ocurrencia");

    // Bloquear en el mismo orden aunque origen y destino estén invertidos.
    auto locked = tx.exec_params(
        "SELECT occurrence_id FROM event_occurrences "
        "WHERE event_id = $1 AND occurrence_id IN ($2, $3) "
        "ORDER BY occurrence_id FOR NO KEY UPDATE", eventID, oldOcc, occurrenceID);
    if (locked.size() != 2) return err("sin cupo o la nueva ocurrencia no es de ese evento");

    auto take = tx.exec_params(
        "UPDATE event_occurrences SET occupied_seats = occupied_seats + $1 "
        "WHERE occurrence_id = $2 AND event_id = $3 AND occupied_seats + $1 <= capacity", seats, occurrenceID, eventID);
        
    if (take.affected_rows() != 1)
        return err("sin cupo o la nueva ocurrencia no es de ese evento");

    auto release = tx.exec_params(
        "UPDATE event_occurrences SET occupied_seats = occupied_seats - $1 "
        "WHERE occurrence_id = $2 AND occupied_seats >= $1", seats, oldOcc);
    if (release.affected_rows() != 1) return err("no se pudieron liberar los asientos de la ocurrencia");

    auto move = tx.exec_params(
        "UPDATE reservations SET occurrence_id = $2 WHERE reservation_id = $1",
        reservationID, occurrenceID);
    if (move.affected_rows() != 1) return err("no se pudo cambiar la ocurrencia de la reserva");

    return "OK";
}

/* cancelReservation
Parametros de entrada: transacción de PostgreSQL (transaction_base), id de la reserva (String), id del cliente (String)
Parametros de salida: String (OK si se pudo, Error si no)
Función: Verifica que la reserva exista, esté activa y pertenezca al cliente, libera los asientos ocupados en la ocurrencia correspondiente y cambia el estado de la reserva a CANCELLED.
*/
std::string cancelReservation(pqxx::transaction_base& tx, const std::string& reservationID,
                              const std::string& clientID) {

    auto cur = tx.exec_params(
        "SELECT occurrence_id, seat_count FROM reservations "
        "WHERE reservation_id = $1 AND client_id = $2 AND status = 'ACTIVE' "
        "FOR UPDATE", reservationID, clientID);
    if (cur.empty()) return err("la reserva no existe, esta cancelada o no es del cliente");

    auto occurrenceID = cur[0]["occurrence_id"].as<std::string>();
    int seats = cur[0]["seat_count"].as<int>();

    auto updOcc = tx.exec_params(
        "UPDATE event_occurrences SET occupied_seats = occupied_seats - $1 "
        "WHERE occurrence_id = $2 AND occupied_seats >= $1",
        seats, occurrenceID);
    if (updOcc.affected_rows() != 1)
        return err("no se pudieron liberar los asientos de la ocurrencia");

    auto updRes = tx.exec_params(
        "UPDATE reservations SET status = 'CANCELLED' "
        "WHERE reservation_id = $1 AND status = 'ACTIVE'",
        reservationID);
    if (updRes.affected_rows() != 1)
        return err("no se pudo cancelar la reserva");

    return "OK: CANCELACION REALIZADA EXITOSAMENTE!!!";
}


/* executeWrite
Parametros de entrada: transacción de PostgreSQL (transaction_base), solicitud (Request)
Parametros de salida: String (resultado de la operación)
Función: Ejecuta la escritura dentro de la subtransacción abierta por executeIdempotent.
*/
std::string executeWrite(
    pqxx::transaction_base& tx,
    const Request& q
) {
    switch (q.request) {
        case RESERVE:
            return reserveSeats(
                tx, q.clientID, q.eventID,
                q.occurrenceID, q.quantity
            );

        case MODIFY_QUANTITY:
            return modifyQuantity(
                tx, q.reservationID, q.clientID,
                q.eventID, q.occurrenceID, q.quantity
            );

        case MODIFY_OCCURRENCE:
            return modifyOccurrence(
                tx, q.reservationID, q.clientID,
                q.eventID, q.occurrenceID
            );

        case CANCEL:
            return cancelReservation(
                tx, q.reservationID, q.clientID
            );

        default:
            return err("Operacion no soportada");
    }
}


/* parseMonth
Parametros de entrada: mes (String), como número ("10") o como nombre en minúsculas ("octubre")
Parametros de salida: número del mes (int, de 1 a 12). Lanza una excepción si el texto no es un mes válido.
*/
static int parseMonth(const std::string& m) {
    static const std::vector<std::string> names = {"enero", "febrero", "marzo", "abril", "mayo", "junio",
                                                   "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"};
    for (size_t i = 0; i < names.size(); i++)
        if (m == names[i]) return static_cast<int>(i) + 1;

    int month = std::stoi(m);
    if (month < 1 || month > 12) throw std::invalid_argument("mes fuera de rango");
    return month;
}

/* attend
Parametros de entrada: conexión con la db, solicitud (Request)
Parametros de salida: Tipo de dato que retorne de la función llamada. Error en caso inválido.
Función: Recibe la solicitud y dependiendo del tipo, ejecuta la función correspondiente para atenderla.
*/
std::string attend(pqxx::connection& db, const Request& q) {
    switch (q.request) {
        case CHECK_SEATS:
            return checkSeats(db, (q.eventID), (q.occurrenceID));
        case CHECK_RESERVATION:
            return checkReservation(db, (q.reservationID), (q.clientID));
        case CHECK_OCCURRENCES:
            return checkOccurrence(db, (q.eventID), (q.occurrenceID));
        case QUERY: {
            int month;
            try { month = parseMonth(q.month); }
            catch (...) { return err("mes invalido"); }
            return queryEvents(db, month);
        }
        default:
            break;
    }
    return err("solicitud desconocida");
}

template <size_t N>
std::string boundedString(const char (&value)[N]) {
    return std::string(value, strnlen(value, N));
}

bool validUUID(const std::string& id) {
    if (id.size() != 36) return false;

    for (size_t i = 0; i < id.size(); ++i) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (id[i] != '-') return false;
        } else if (!std::isxdigit(
            static_cast<unsigned char>(id[i])
        )) {
            return false;
        }
    }

    return true;
}

void appendField(
    std::string& output,
    const std::string& value
) {
    output += std::to_string(value.size());
    output += ':';
    output += value;
}

std::string requestFingerprint(const Request& q) {
    std::string result;

    appendField(result, std::to_string(static_cast<int>(q.request)));
    appendField(result, boundedString(q.clientID));
    appendField(result, boundedString(q.reservationID));
    appendField(result, boundedString(q.eventID));
    appendField(result, boundedString(q.occurrenceID));
    appendField(result, std::to_string(q.quantity));

    return result;
}

bool useReplica(const Request& request) {
    return request.request == QUERY;
}

bool isWriteOperation(const Request& q) {
    switch (q.request) {
        case RESERVE:
        case MODIFY_QUANTITY:
        case MODIFY_OCCURRENCE:
        case CANCEL:
            return true;

        default:
            return false;
    }
}


/* executeIdempotent
Parametros de entrada: conexión a la base principal (connection), solicitud (Request)
Parametros de salida: String (resultado original o error si el UUID se reutiliza con otros datos)
Función: Reclama el UUID mediante la restricción única de processed_requests. Guarda la respuesta y las escrituras
         exitosas en la misma transacción. Si la operación devuelve un error de negocio, revierte sus cambios
         mediante una subtransacción y guarda solo la respuesta para los reintentos.
*/
std::string executeIdempotent(
    pqxx::connection& db,
    const Request& q
) {
    std::string requestId = boundedString(q.requestId);
    std::string clientId = boundedString(q.clientID);

    if (!validUUID(requestId)) {
        return err("requestId invalido");
    }

    std::string fingerprint = requestFingerprint(q);
    int operationType = static_cast<int>(q.request);

    pqxx::work tx(db);

    // Intentar reclamar el identificador.
    auto inserted = tx.exec_params(
        "INSERT INTO processed_requests "
        "(request_id, client_id, operation_type, "
        " request_fingerprint, response) "
        "VALUES ($1::uuid, $2, $3, $4, '') "
        "ON CONFLICT (request_id) DO NOTHING "
        "RETURNING request_id",
        requestId,
        clientId,
        operationType,
        fingerprint
    );

    // La solicitud ya había sido registrada.
    if (inserted.empty()) {
        auto previous = tx.exec_params(
            "SELECT client_id, operation_type, "
            "request_fingerprint, response "
            "FROM processed_requests "
            "WHERE request_id = $1::uuid",
            requestId
        );

        if (previous.empty()) {
            throw std::runtime_error(
                "No se encontro la solicitud registrada"
            );
        }

        const auto& row = previous[0];

        if (row["client_id"].as<std::string>() != clientId ||
            row["operation_type"].as<int>() != operationType ||
            row["request_fingerprint"].as<std::string>() != fingerprint) {

            return err(
                "requestId reutilizado con parametros diferentes"
            );
        }

        std::string response = row["response"].as<std::string>();

        writeLog(std::cout, "[IDEMPOTENCIA] Solicitud repetida: " + requestId);

        return response;
    }

    std::string response;
    {
        pqxx::subtransaction operation(tx, "request_write");
        response = executeWrite(operation, q);
        if (response.rfind("OK", 0) == 0) {
            operation.commit();
        } else {
            operation.abort();
        }
    }

    // Guardar el resultado en la misma transacción.
    tx.exec_params(
        "UPDATE processed_requests "
        "SET response = $2 "
        "WHERE request_id = $1::uuid",
        requestId,
        response
    );

    // Confirmar operación y respuesta conjuntamente.
    tx.commit();

    writeLog(std::cout, "[IDEMPOTENCIA] Solicitud nueva: " + requestId);

    return response;
}

/* processRequest
Parametros de entrada: solicitud (Request), conexión del trabajador a la base seleccionada (unique_ptr),
                      datos de conexión (String)
Parametros de salida: String (respuesta de la operación o error de conexión)
Función: Abre la conexión cuando se necesita. Si se pierde, permite reintentar QUERY y descarta la conexión rota.
         Una escritura con resultado incierto puede consultarse reenviando el UUID original desde el cliente.
*/
static std::string processRequest(const Request &request, std::unique_ptr<pqxx::connection> &database,
                                  const std::string &conninfo) {
    const bool readReplica = useReplica(request);

    try {
        if (!database) database = std::make_unique<pqxx::connection>(conninfo);

        if (isWriteOperation(request)) return executeIdempotent(*database, request);
        return attend(*database, request);
    } catch (const pqxx::in_doubt_error &) {
        database.reset();
        return err("Conexion perdida; resultado de operacion desconocido");
    } catch (const pqxx::broken_connection &) {
        database.reset();
        if (!readReplica) return err("Conexion perdida; resultado de operacion desconocido");

        database = std::make_unique<pqxx::connection>(conninfo);
        return attend(*database, request);
    }
}

/* runWorker
Parametros de entrada: contexto de ZeroMQ (context_t), datos de conexión a la base principal y la réplica (String),
                      número de trabajador (int)
Parametros de salida: Ninguno
Función: Recibe una solicitud por su socket REP, ejecuta la operación con conexiones exclusivas del hilo y responde.
         Registra la recepción y la respuesta con el UUID para seguir la solicitud desde el gestor y RMC.
*/
static void runWorker(zmq::context_t &context, const std::string &primaryConninfo,
                      const std::string &replicaConninfo, int workerId) {
    std::unique_ptr<pqxx::connection> primaryDB;
    std::unique_ptr<pqxx::connection> replicaDB;
    const std::string prefix = "[PERSISTENCIA] Hilo " + std::to_string(workerId);

    try {
        zmq::socket_t worker(context, zmq::socket_type::rep);
        worker.set(zmq::sockopt::linger, 0);
        worker.set(zmq::sockopt::rcvhwm, 1);
        worker.connect(WORKER_ENDPOINT);

        while (true) {
            zmq::multipart_t message;
            if (!message.recv(worker)) continue;

            if (message.size() != 1 || message[0].size() != sizeof(Request)) {
                sendResponse(worker, err("solicitud invalida"));
                writeLog(std::cout, prefix + " | Solicitud invalida");
                continue;
            }

            Request request{};
            std::memcpy(&request, message[0].data(), sizeof(request));

            std::underlying_type_t<RequestType> operation;
            std::memcpy(&operation, &request.request, sizeof(operation));
            if (operation > CHECK_OCCURRENCES) {
                sendResponse(worker, err("solicitud desconocida"));
                continue;
            }

            request.clientID[sizeof(request.clientID) - 1] = '\0';
            request.eventID[sizeof(request.eventID) - 1] = '\0';
            request.occurrenceID[sizeof(request.occurrenceID) - 1] = '\0';
            request.reservationID[sizeof(request.reservationID) - 1] = '\0';
            request.month[sizeof(request.month) - 1] = '\0';

            const bool replica = useReplica(request);
            const std::string trace = prefix + " | UUID: " + boundedString(request.requestId)
                                    + " | Operacion: " + requestTypeName(request.request)
                                    + " | Base: " + (replica ? "replica" : "principal");
            writeLog(std::cout, trace + " | Solicitud recibida");

            std::string response;
            try {
                response = processRequest(request, replica ? replicaDB : primaryDB,
                                          replica ? replicaConninfo : primaryConninfo);
            } catch (const pqxx::broken_connection &error) {
                (replica ? replicaDB : primaryDB).reset();
                writeLog(std::cerr, trace + " | " + error.what());
                response = err("Error de servidor");
            } catch (const std::exception &error) {
                writeLog(std::cerr, trace + " | " + error.what());
                response = err("Error de servidor");
            }

            sendResponse(worker, response);
            writeLog(std::cout, trace + " | [RESP] " + response);
        }
    } catch (const zmq::error_t &error) {
        if (error.num() != ETERM) {
            writeLog(std::cerr, prefix + " | Error en trabajador: " + error.what());
            context.shutdown();
        }
    } catch (const std::exception &error) {
        writeLog(std::cerr, prefix + " | Error en trabajador: " + error.what());
        context.shutdown();
    }
}

/* main
Parametros de entrada: cantidad de argumentos (int), argumentos de ejecución (char**)
Parametros de salida: 0 al completar la ejecución, 1 si ocurre un error
Función: Conserva el puerto de persistencia y distribuye las solicitudes con un proxy ROUTER/DEALER.
         Al salir del distribuidor, cierra el contexto y espera a los trabajadores.
*/
int main(int argc, char *argv[]) {
    if (argc != 2) {
        std::cout << "Error: Numero de argumentos incorrecto\n";
        std::cout << "Formato: " << argv[0] << " <ip-persistencia>\n";
        return 1;
    }

    const std::string endpoint = std::string("tcp://") + argv[1] + ":8888";
    const std::string primaryConninfo = "host=localhost port=5432 dbname=event-reservation user=root password=root";
    const std::string replicaConninfo = "host=localhost port=5433 dbname=event-reservation user=root password=root";

    zmq::context_t context(1);
    std::vector<std::thread> workers;
    int status = 0;

    try {
        zmq::socket_t frontend(context, zmq::socket_type::router);
        frontend.set(zmq::sockopt::linger, 0);
        frontend.bind(endpoint);

        zmq::socket_t distributor(context, zmq::socket_type::dealer);
        distributor.set(zmq::sockopt::linger, 0);
        distributor.set(zmq::sockopt::sndhwm, 1);
        distributor.bind(WORKER_ENDPOINT);

        workers.reserve(PERSISTENCE_WORKERS);
        for (int i = 0; i < PERSISTENCE_WORKERS; ++i) {
            workers.emplace_back(runWorker, std::ref(context), std::cref(primaryConninfo),
                                 std::cref(replicaConninfo), i + 1);
        }

        writeLog(std::cout, "[PERSISTENCIA] Escuchando en " + endpoint + " con "
                            + std::to_string(PERSISTENCE_WORKERS) + " trabajadores");
        zmq::proxy(frontend, distributor);
    } catch (const std::exception &error) {
        writeLog(std::cerr, std::string("[PERSISTENCIA] Error en distribuidor: ") + error.what());
        status = 1;
    }

    context.shutdown();
    for (auto &worker : workers) {
        worker.join();
    }
    return status;
}

