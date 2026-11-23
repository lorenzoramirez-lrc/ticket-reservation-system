/*
Archivo: servicio-persistencia.cpp
Objetivo: Recibir las solicitudes desde el servicio RMC y realizar las distintas operaciones en la persistencia de datos usando postgreSQL y su librería oficial.
Funciones: sendResponse, checkSeats, checkReservation, checkOccurrence, queryEvents, reserveSeats, modifyQuantity, modifyOcurrence, attend
*/

#include <vector>
#include <stdexcept>
#include <string>
#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <cstdlib>
#include <zmq.hpp>
#include <pqxx/pqxx>
#include "../request-structs/structs.cpp"   

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
Parametros de entrada: conexión con la db, id del cliente (String), id del evento (String), id de la ocurrencia (String), cantidad de asientos a reservar (int)
Parametros de salida: String (OK con el id de reserva si se pudo, error en caso contrario)
Función: Revisa la disponibilidad de cupos de una ocurrencia en determinado evento y dependiendo de esta hace una reserva o no.
*/
std::string reserveSeats(pqxx::work& tx,, const std::string& clientID,
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
Parametros de entrada: conexión con la db, id de la reserva (String), id del cliente (String), id del evento (String), id de la ocurrencia (String), nueva cantidad de asientos (int)
Parametros de salida: String (OK si se pudo, Error si no)
Función: Revisa si es posible cambiar la cantidad de asientos reservados en un pedido y realiza el cambio en caso de que sea posible. 
*/
std::string modifyQuantity(pqxx::work& tx,, const std::string& reservationID,
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
Parametros de entrada: conexión con la db, id de la reserva (String), id de la reserva (String), id del cliente (String), id del evento (String), id de la ocurrencia (String)
Parametros de salida: String (OK si se pudo, Error si no)
Función: Busca otras ocurrencias del mismo evento para poder cambiar los eventos de la ocurrencia actual a otra distinta.
*/
std::string modifyOccurrence(pqxx::work& tx, const std::string& reservationID,
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

    auto take = tx.exec_params(
        "UPDATE event_occurrences SET occupied_seats = occupied_seats + $1 "
        "WHERE occurrence_id = $2 AND event_id = $3 AND occupied_seats + $1 <= capacity", seats, occurrenceID, eventID);
        
    if (take.affected_rows() != 1)
        return err("sin cupo o la nueva ocurrencia no es de ese evento");

    tx.exec_params("UPDATE event_occurrences SET occupied_seats = occupied_seats - $1 "
                   "WHERE occurrence_id = $2", seats, oldOcc); //OldOcc new seats
    tx.exec_params("UPDATE reservations SET occurrence_id = $2 WHERE reservation_id = $1", reservationID, occurrenceID); //new reservation Ocurrence

    return "OK";
}

/* cancelReservation
Parametros de entrada: conexión con la db, id de la reserva (String), id del cliente (String)
Parametros de salida: String (OK si se pudo, Error si no)
Función: Verifica que la reserva exista, esté activa y pertenezca al cliente, libera los asientos ocupados en la ocurrencia correspondiente y cambia el estado de la reserva a CANCELLED.
*/
std::string cancelReservation(pqxx::work& tx, const std::string& reservationID,
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

    return "OK";
}


std::string executeWrite(
    pqxx::work& tx,
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

        std::cout << "[IDEMPOTENCIA] Solicitud repetida: "
                  << requestId << '\n';

        return response;
    }

    // Primera ejecución de esta solicitud.
    std::string response = executeWrite(tx, q);

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

    std::cout << "[IDEMPOTENCIA] Solicitud nueva: "
              << requestId << '\n';

    return response;
}

int main(int argc, char *argv[]) {


    if(argc != 2){
        std::cout<<"Error: Numero de argumentos incorrecto\n";
        std::cout<<"Formato: "<<argv[0]<<" <ip-persistencia>\n";
        return 1;
    }

    std::string ipPersistence= "tcp://";
    ipPersistence+=argv[1];
    ipPersistence+=":8888";


    std::string primaryConninfo = "host=localhost port=5432 dbname=event-reservation user=root password=root";
    std::string replicaConninfo = "host=localhost port=5433 dbname=event-reservation user=root password=root";

    auto primaryDB = std::make_unique<pqxx::connection>(primaryConninfo);
    auto replicaDB = std::make_unique<pqxx::connection>(replicaConninfo);

    zmq::context_t ctx(1);
    zmq::socket_t rmcSocket(ctx, zmq::socket_type::rep);
    rmcSocket.bind(ipPersistence);

    while (true) {

        zmq::message_t msg;
        auto result = rmcSocket.recv(msg, zmq::recv_flags::none);
        if (!result) continue;

        std::string resp;
        if (msg.size() != sizeof(Request)) {
            resp = err("solicitud invalida");
            sendResponse(rmcSocket, resp);
            continue;
        } 

        Request q{};
        memcpy(&q, msg.data(), sizeof(Request));

        try {
            bool readReplica = useReplica(q);

            auto& selectedDB = readReplica ? replicaDB : primaryDB;

            const std::string& conninfo =
                readReplica ? replicaConninfo : primaryConninfo;

            try {
                if (isWriteOperation(q)) {
                    resp = executeIdempotent(*primaryDB, q);
                } else {
                    resp = attend(*selectedDB, q);
                }
            } catch (const pqxx::broken_connection&) {
                selectedDB = std::make_unique<pqxx::connection>(conninfo);

                // Reintento automático solo para consultas de lectura.
                if (readReplica) {
                    resp = attend(*selectedDB, q);
                } else {
                    resp = err("Conexion perdida; resultado de operacion desconocido");
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "[PERSISTENCIA] " << e.what() << '\n';
            resp = err("Error de servidor");
        }


        std::cout<<"Solicitud recibida:"<<q.request<<"\n";

        std::cout << "[RESP] " << resp << "\n";
        sendResponse(rmcSocket, resp);
    }
}
