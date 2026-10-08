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


template <size_t N>
static std::string str(const char (&a)[N]) { return std::string(a, strnlen(a, N)); }

void sendResponse(zmq::socket_t &socket, const std::string &response) {
    zmq::message_t reply(response.data(), response.size());
    socket.send(reply, zmq::send_flags::none);
}



std::string checkSeats(pqxx::connection& db, const std::string& eventID,
                       const std::string& occurrenceID) {
    pqxx::nontransaction tx(db);
    auto r = tx.exec_params(
        "SELECT capacity - occupied_seats AS free_seats FROM event_occurrences "
        "WHERE occurrence_id = $1 AND event_id = $2", occurrenceID, eventID);
    if (r.empty()) return err("la ocurrencia no existe para ese evento");
    return std::to_string(r[0]["free_seats"].as<int>());
}


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


std::string checkOccurrence(pqxx::connection& db, const std::string& eventID,
                            const std::string& occurrenceID) {
    pqxx::nontransaction tx(db);
    auto r = tx.exec_params(
        "SELECT 1 FROM event_occurrences WHERE occurrence_id = $1 AND event_id = $2",
        occurrenceID, eventID);
    if (r.empty()) return err("la ocurrencia no existe para ese evento");
    return "OK";
}



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



std::string reserveSeats(pqxx::connection& db, const std::string& clientID,
                         const std::string& eventID, const std::string& occurrenceID,
                         int quantity) {
    if (quantity <= 0) return err("cantidad invalida");
    pqxx::work tx(db);

    auto upd = tx.exec_params(
        "UPDATE event_occurrences SET occupied_seats = occupied_seats + $1 "
        "WHERE occurrence_id = $2 AND event_id = $3 AND occupied_seats + $1 <= capacity",
        quantity, occurrenceID, eventID);
    if (upd.affected_rows() != 1)
        return err("sin cupo o la ocurrencia no existe");      // sin commit: rollback

    auto ins = tx.exec_params(
        "INSERT INTO reservations(reservation_id, occurrence_id, client_id, seat_count) "
        "VALUES (gen_random_uuid()::text, $1, $2, $3) RETURNING reservation_id",
        occurrenceID, clientID, quantity);
    auto id = ins[0][0].as<std::string>();

    tx.commit();
    return "OK reservationID=" + id;
}



std::string modifyQuantity(pqxx::connection& db, const std::string& reservationID,
                           const std::string& clientID, const std::string& eventID,
                           const std::string& occurrenceID, int newQuantity) {
    if (newQuantity <= 0) return err("cantidad invalida");
    pqxx::work tx(db);

    auto cur = tx.exec_params(
        "SELECT r.seat_count FROM reservations r "
        "JOIN event_occurrences o ON o.occurrence_id = r.occurrence_id "
        "WHERE r.reservation_id = $1 AND r.client_id = $2 AND r.status = 'ACTIVE' "
        "  AND r.occurrence_id = $3 AND o.event_id = $4 FOR UPDATE OF r",
        reservationID, clientID, occurrenceID, eventID);
    if (cur.empty()) return err("la reserva no existe, esta cancelada o no coincide");

    int delta = newQuantity - cur[0]["seat_count"].as<int>();
    if (delta == 0) return "OK";

    auto upd = tx.exec_params(
        "UPDATE event_occurrences SET occupied_seats = occupied_seats + $1 "
        "WHERE occurrence_id = $2 AND occupied_seats + $1 <= capacity",
        delta, occurrenceID);
    if (upd.affected_rows() != 1) return err("sin cupo para la nueva cantidad");

    tx.exec_params("UPDATE reservations SET seat_count = $2 WHERE reservation_id = $1",
                   reservationID, newQuantity);
    tx.commit();
    return "OK";
}


std::string modifyOccurrence(pqxx::connection& db, const std::string& reservationID,
                             const std::string& clientID, const std::string& eventID,
                             const std::string& occurrenceID) {
    pqxx::work tx(db);

    auto cur = tx.exec_params(
        "SELECT r.occurrence_id, r.seat_count FROM reservations r "
        "JOIN event_occurrences o ON o.occurrence_id = r.occurrence_id "
        "WHERE r.reservation_id = $1 AND r.client_id = $2 AND r.status = 'ACTIVE' "
        "  AND o.event_id = $3 FOR UPDATE OF r",
        reservationID, clientID, eventID);
    if (cur.empty()) return err("la reserva no existe, esta cancelada o no es de ese evento");

    auto oldOcc = cur[0]["occurrence_id"].as<std::string>();
    int  seats  = cur[0]["seat_count"].as<int>();
    if (oldOcc == occurrenceID) return err("la reserva ya esta en esa ocurrencia");

    auto take = tx.exec_params(
        "UPDATE event_occurrences SET occupied_seats = occupied_seats + $1 "
        "WHERE occurrence_id = $2 AND event_id = $3 AND occupied_seats + $1 <= capacity",
        seats, occurrenceID, eventID);
    if (take.affected_rows() != 1)
        return err("sin cupo o la nueva ocurrencia no es de ese evento");

    tx.exec_params("UPDATE event_occurrences SET occupied_seats = occupied_seats - $1 "
                   "WHERE occurrence_id = $2", seats, oldOcc);
    tx.exec_params("UPDATE reservations SET occurrence_id = $2 WHERE reservation_id = $1",
                   reservationID, occurrenceID);
    tx.commit();
    return "OK";
}


std::string cancelReservation(pqxx::connection& db, const std::string& reservationID,
                              const std::string& clientID) {
    pqxx::work tx(db);

    auto r = tx.exec_params(
        "UPDATE reservations SET status = 'CANCELLED' "
        "WHERE reservation_id = $1 AND client_id = $2 AND status = 'ACTIVE' "
        "RETURNING occurrence_id, seat_count", reservationID, clientID);
    if (r.empty()) return err("la reserva no existe, ya esta cancelada o no es del cliente");

    tx.exec_params("UPDATE event_occurrences SET occupied_seats = occupied_seats - $1 "
                   "WHERE occurrence_id = $2",
                   r[0]["seat_count"].as<int>(), r[0]["occurrence_id"].as<std::string>());
    tx.commit();
    return "OK";
}


std::string atender(pqxx::connection& db, const Request& q) {
    switch (q.request) {
        case CHECK_SEATS:
            return checkSeats(db, str(q.eventID), str(q.occurrenceID));
        case CHECK_RESERVATION:
            return checkReservation(db, str(q.reservationID), str(q.clientID));
        case CHECK_OCCURRENCES:
            return checkOccurrence(db, str(q.eventID), str(q.occurrenceID));
        case RESERVE:
            return reserveSeats(db, str(q.clientID), str(q.eventID), str(q.occurrenceID), q.quantity);
        case MODIFY_QUANTITY:   // el RMC manda la cantidad nueva en quantity
            return modifyQuantity(db, str(q.reservationID), str(q.clientID),
                                  str(q.eventID), str(q.occurrenceID), q.quantity);
        case MODIFY_OCCURRENCE:
            return modifyOccurrence(db, str(q.reservationID), str(q.clientID),
                                    str(q.eventID), str(q.occurrenceID));
        case QUERY: {
            int month;
            try { month = std::stoi(str(q.month)); }
            catch (...) { return err("mes invalido"); }
            return queryEvents(db, month);
        }
        case CANCEL:
            return cancelReservation(db, str(q.reservationID), str(q.clientID));
        default:
            break;
    }
    return err("solicitud desconocida");
}

int main() {
    const char* env = std::getenv("SRB_DB");
    std::string conninfo = env ? env : "host=localhost dbname=srb user=postgres password=srb";
    auto db = std::make_unique<pqxx::connection>(conninfo);

    zmq::context_t ctx(1);
    zmq::socket_t sock(ctx, zmq::socket_type::rep);
    sock.bind("tcp://*:8888");

    while (true) {
        zmq::message_t msg;
        auto result = sock.recv(msg, zmq::recv_flags::none);
        if (!result) continue;

        std::string resp;
        if (msg.size() != sizeof(Request)) {
            resp = err("solicitud invalida");
        } else {
            Request q{};
            memcpy(&q, msg.data(), sizeof(Request));
        
            try {
                try {
                    resp = atender(*db, q);
                } catch (const pqxx::broken_connection&) {
                    db = std::make_unique<pqxx::connection>(conninfo);   // reconectar y reintentar
                    resp = atender(*db, q);
                }
            } catch (const std::exception& e) {
                resp = err(e.what());
            }
        }
        std::cout << "[RESP] " << resp << "\n";
        sendResponse(sock, resp);
    }
}
