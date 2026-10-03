#include <string>
#include <zmq.hpp>
#include <cstring>
#include "../request-structs/structs.cpp"

void sendResponse(zmq::socket_t &socket, const std::string &response) {
    zmq::message_t reply(response.data(), response.size());
    socket.send(reply, zmq::send_flags::none);
}

std::string sendPersistence(zmq::socket_t &persistenceSocket, const Request &req) {
    zmq::message_t message(sizeof(Request));
    memcpy(message.data(), &req, sizeof(Request));
    persistenceSocket.send(message, zmq::send_flags::none);

    zmq::message_t response;
    auto result = persistenceSocket.recv(response, zmq::recv_flags::none);
    if (!result) return "ERROR: sin respuesta de persistencia";

    return std::string(static_cast<char*>(response.data()), response.size());
}

std::string checkSeats(zmq::socket_t &persistenceSocket, std::string eventID, std::string occurenceID, int quantity){
    Request check_seats{};
    check_seats.request = CHECK_SEATS;
    copy(check_seats.eventID, eventID);
    copy(check_seats.occurrenceID, occurenceID);

    std::string response = sendPersistence(persistenceSocket, check_seats);

    int available;
    try {
        available = std::stoi(response);
    }catch (...){
        return response; //Llega un error 
    }
    
    if (available < quantity) return "ERROR: Asientos insuficientes";

    return "OK";
}

std::string checkReservation(zmq::socket_t &persistenceSocket, std::string reservationID, std::string clientID){
    Request check_reservation{};
    check_reservation.request = CHECK_RESERVATION;
    copy(check_reservation.reservationID, reservationID);
    copy(check_reservation.clientID, clientID);

    return sendPersistence(persistenceSocket, check_reservation);
}

std::string checkOccurrences(zmq::socket_t &persistenceSocket, std::string eventID, std::string occurrenceID){
    Request check_occurrence{};
    check_occurrence.request = CHECK_OCCURRENCES;
    copy(check_occurrence.eventID, eventID);
    copy(check_occurrence.occurrenceID, occurrenceID);

    return sendPersistence(persistenceSocket, check_occurrence);
}


std::string reserveSeats (zmq::socket_t &persistenceSocket, std::string clientID, std::string eventID, std::string occurenceID, int quantity){
    std::string check = checkSeats(persistenceSocket, eventID, occurenceID, quantity);

    if (check != "OK") return check;

    Request reserve{};
    reserve.request = RESERVE;
    copy(reserve.clientID, clientID);
    copy(reserve.eventID, eventID);
    copy(reserve.occurrenceID, occurenceID);
    copy(reserve.quantity, std::to_string(quantity));

    return sendPersistence(persistenceSocket, reserve);
}

std::string modifyQuantity (zmq::socket_t &persistenceSocket, std::string clientID, std::string reservationID, int newQuantity, std::string eventID, std::string occurenceID){
    std::string check_res = checkReservation(persistenceSocket, reservationID, clientID);

    int current;
    try {
        current = std::stoi(check_res);
    }catch (...) {
        return check_res;   
    }

    int extra = newQuantity - current;
    if (extra > 0){
        std::string check_seats = checkSeats(persistenceSocket, eventID, occurenceID, newQuantity);
        if (check_seats != "OK") return check_seats;
    }

    Request modify{};
    modify.request = MODIFY_QUANTITY;
    copy(modify.clientID, clientID);
    copy(modify.eventID, eventID);
    copy(modify.occurrenceID, occurenceID);
    copy(modify.quantity, std::to_string(newQuantity));
    copy(modify.reservationID, reservationID);

    return sendPersistence(persistenceSocket, modify);
}

std::string modifyOccurrence (zmq::socket_t &persistenceSocket, std::string clientID, std::string reservationID, std::string eventID, std::string occurrenceID){
    std::string check_res = checkReservation(persistenceSocket, reservationID, clientID);

    int current;
    try {
        current = std::stoi(check_res);
    }catch (...) {
        return check_res;   
    }

    std::string check_occ = checkOccurrences(persistenceSocket, eventID, occurrenceID);
    if(check_occ != "OK") return check_occ;

    std::string check_seats = checkSeats(persistenceSocket, eventID, occurrenceID, current);
    if (check_seats != "OK") return check_seats;

    Request modify{};
    modify.request = MODIFY_OCCURRENCE;
    copy(modify.clientID, clientID);
    copy(modify.reservationID, reservationID);
    copy(modify.eventID, eventID);
    copy(modify.occurrenceID, occurrenceID);

    return sendPersistence(persistenceSocket, modify);
}

std::string checkEvents (zmq::socket_t &persistenceSocket, std::string month){
    Request events{};
    events.request = QUERY;
    copy(events.month, month);

    return sendPersistence(persistenceSocket, events);
}

int main(){
    zmq::context_t contextZMQ(1);
    zmq::socket_t managerSocket(contextZMQ, zmq::socket_type::rep);
    managerSocket.bind("tcp://10.43.100.20:7777");

    zmq::socket_t persistenceSocket(contextZMQ, zmq::socket_type::req);
    persistenceSocket.connect("tcp://10.43.100.34:8888");

    while(true){
        zmq::message_t managerRequest;

        auto result = managerSocket.recv(managerRequest, zmq::recv_flags::none);

        if(!result) continue;

        if (managerRequest.size() != sizeof(Request)){
            sendResponse(managerSocket, "ERROR: Solicitud invalida");
            continue;
        }

        Request structuredRequest; 
        memcpy(&structuredRequest, managerRequest.data(), sizeof(Request));
        
        std::string transactionResponse{};

        switch(structuredRequest.request){
            case RESERVE:
                transactionResponse = reserveSeats(persistenceSocket, structuredRequest.clientID, 
                        structuredRequest.eventID, structuredRequest.occurrenceID, std::stoi(structuredRequest.quantity));
                break;

            case MODIFY_OCCURRENCE:
                transactionResponse = modifyOccurrence(persistenceSocket, structuredRequest.clientID, structuredRequest.reservationID,
                        structuredRequest.eventID, structuredRequest.occurrenceID);
                break;

            case MODIFY_QUANTITY:
                transactionResponse = modifyQuantity(persistenceSocket, structuredRequest.clientID, structuredRequest.reservationID, 
                        std::stoi(structuredRequest.quantity), structuredRequest.eventID,structuredRequest.occurrenceID);
                break;

            case QUERY:
                transactionResponse = checkEvents(persistenceSocket, structuredRequest.month);
                break;

            default:
                transactionResponse = "ERROR: Solicitud desconocida";
                break;
        }

        sendResponse(managerSocket, transactionResponse);


    }

}


