/*
Archivo: servicio-persistencia.cpp
Objetivo: Recibir las solicitudes desde el servicio RMC y realizar las distintas operaciones en la persistencia de datos usando postgreSQL y su librería oficial.
Funciones: sendResponse, checkSeats, checkReservation, checkOccurrence, queryEvents, reserveSeats, modifyQuantity, modifyOcurrence, attend
*/

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
std::string reserveSeats(pqxx::connection& db, const std::string& clientID,
                         const std::string& eventID, const std::string& occurrenceID,
                         int quantity) {
    if (quantity <= 0) return err("cantidad invalida");
    pqxx::work tx(db);

    auto upd = tx.exec_params
    (   "UPDATE event_occurrences SET occupied_seats = occupied_seats + $1 "
        "WHERE occurrence_id = $2 AND event_id = $3 AND occupied_seats + $1 <= capacity", quantity, occurrenceID, eventID);
    if (upd.affected_rows() != 1)
        return err("sin cupo o la ocurrencia no existe");   

    auto ins = tx.exec_params(
        "INSERT INTO reservations(reservation_id, occurrence_id, client_id, seat_count) "
        "VALUES (gen_random_uuid()::text, $1, $2, $3) RETURNING reservation_id", occurrenceID, clientID, quantity);
    auto id = ins[0]["reservation_id"].as<std::string>();

    tx.commit();
    return "OK reservationID=" + id;
}


/* modifyQuantity
Parametros de entrada: conexión con la db, id de la reserva (String), id del cliente (String), id del evento (String), id de la ocurrencia (String), nueva cantidad de asientos (int)
Parametros de salida: String (OK si se pudo, Error si no)
Función: Revisa si es posible cambiar la cantidad de asientos reservados en un pedido y realiza el cambio en caso de que sea posible. 
*/
std::string modifyQuantity(pqxx::connection& db, const std::string& reservationID,
                           const std::string& clientID, const std::string& eventID,
                           const std::string& occurrenceID, int newQuantity) {
    if (newQuantity<=0) return err("cantidad invalida");
    pqxx::work tx(db);

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

    tx.commit();
    return "OK";
}

/* modifyOccurrence
Parametros de entrada: conexión con la db, id de la reserva (String), id de la reserva (String), id del cliente (String), id del evento (String), id de la ocurrencia (String)
Parametros de salida: String (OK si se pudo, Error si no)
Función: Busca otras ocurrencias del mismo evento para poder cambiar los eventos de la ocurrencia actual a otra distinta.
*/
std::string modifyOccurrence(pqxx::connection& db, const std::string& reservationID,
                             const std::string& clientID, const std::string& eventID,
                             const std::string& occurrenceID) {
    pqxx::work tx(db);

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

    tx.commit();
    return "OK";
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
        case RESERVE:
            return reserveSeats(db, (q.clientID), (q.eventID), (q.occurrenceID), q.quantity);
        case MODIFY_QUANTITY:   
            return modifyQuantity(db, (q.reservationID), (q.clientID),
                                  (q.eventID), (q.occurrenceID), q.quantity);
        case MODIFY_OCCURRENCE:
            return modifyOccurrence(db, (q.reservationID), (q.clientID),
                                    (q.eventID), (q.occurrenceID));
        case QUERY: {
            int month;
            try { month = std::stoi((q.month)); }
            catch (...) { return err("mes invalido"); }
            return queryEvents(db, month);
        }
        default:
            break;
    }
    return err("solicitud desconocida");
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

    const char* env = std::getenv("SRB_DB");
    std::string conninfo = env ? env : "host=localhost dbname=event-reservation user=root password=root";
    auto db = std::make_unique<pqxx::connection>(conninfo);

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
                try {
                    resp = attend(*db, q);
                } catch (const pqxx::broken_connection&) {
                    db = std::make_unique<pqxx::connection>(conninfo);   // reconectar y reintentar
                    resp = attend(*db, q);
                }
            } catch (const std::exception& e) {
                resp = err("Error de servidor.");
        }

        std::cout<<"Solicitud recibida:"<<q.request<<"\n";

        std::cout << "[RESP] " << resp << "\n";
        sendResponse(rmcSocket, resp);
    }
}
