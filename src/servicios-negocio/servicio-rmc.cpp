/*
Archivo: servicio-rmc.cpp
Objetivo: Recibir las solicitudes desde el Gestor de Transacciones y realizar la comunicación con el servicio de persistencia para retornar la respuesta al gestor.
Funciones: sendResponse, sendPersistence, checkSeats, checkReservation, checkOccurrence, checkEvents, reserveSeats, modifyQuantity, modifyOcurrence
*/

#include <string>
#include <zmq.hpp>
#include <cstring>
#include "../request-structs/structs.cpp"
#include <iostream>
#include <cstring>

template <size_t N>
static std::string str(const char (&a)[N]) { return std::string(a, strnlen(a, N)); }


/* sendResponse
Parametros de entrada: socket del zmq (zmq), respuesta (String)
Parametros de salida: Ninguno
Función: Envia la respuesta (reply) al socket correspondiente.
*/
void sendResponse(zmq::socket_t &socket, const std::string &response) {
    zmq::message_t reply(response.data(), response.size());
    socket.send(reply, zmq::send_flags::none);
}

/* sendPersistence
Parametros de entrada: socket del zmq/persistencia (zmq), solicitud (Request)
Parametros de salida: String
Función: Copia los datos de la solicitud, los envia al servicio de persistencia y los guarda en la variable result para luego retornarla
*/
std::string sendPersistence(zmq::socket_t &persistenceSocket, const Request &req) {
    zmq::message_t message(sizeof(Request));
    memcpy(message.data(), &req, sizeof(Request));
    persistenceSocket.send(message, zmq::send_flags::none);

    zmq::message_t response;
    auto result = persistenceSocket.recv(response, zmq::recv_flags::none);
    if (!result) return "ERROR: sin respuesta de persistencia";

    return std::string(static_cast<char*>(response.data()), response.size());
}

/* checkSeats
Parametros de entrada: socket del zmq/persistencia (zmq), id del evento (String), id de la ocurrencia (String), cantidad (int)
Parametros de salida: String
Función: Genera la solicitud de tipo CHECK_SEATS para luego enviarla al servicio de persistencia y comprobar si el numero de asientos disponible es mayor o igual a la cantidad solicitada, en tal caso retorna OK, en caso contrario retornará un error.
*/
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

/* checkReservation
Parametros de entrada: socket del zmq/persistencia (zmq), id de reserva (String), id del cliente (String)
Parametros de salida: String
Función: Genera la solicitud tipo CHECK_RESERVATION para comprobar la existencia de la reserva en la base de datos. Retorna el resultado que arroje el servicio de persistencia.
*/
std::string checkReservation(zmq::socket_t &persistenceSocket, std::string reservationID, std::string clientID){
    Request check_reservation{};
    check_reservation.request = CHECK_RESERVATION;
    copy(check_reservation.reservationID, reservationID);
    copy(check_reservation.clientID, clientID);

    return sendPersistence(persistenceSocket, check_reservation);
}

/* checkOccurrences
Parametros de entrada: socket del zmq/persistencia (zmq), id de la ocurrencia (String), id del evento (String)
Parametros de salida: String
Función: Genera la solicitud tipo CHECK_OCCURRENCES para ver si existe la ocurrencia solicitada. Retorna el resultado que arroje el servicio de persistencia.
*/
std::string checkOccurrences(zmq::socket_t &persistenceSocket, std::string eventID, std::string occurrenceID){
    Request check_occurrence{};
    check_occurrence.request = CHECK_OCCURRENCES;
    copy(check_occurrence.eventID, eventID);
    copy(check_occurrence.occurrenceID, occurrenceID);

    return sendPersistence(persistenceSocket, check_occurrence);
}

/* reserveSeats
Parametros de entrada: socket del zmq/persistencia (zmq), id de la ocurrencia (String), id del evento (String), cantidad solicitada (int), id del cliente (String)
Parametros de salida: String
Función: Llama a la función checkSeats para saber si hay suficientes asientos, después genera la solicitud tipo RESERVE para enviarla al socket de persistencia y retornar su resultado.
*/
std::string reserveSeats (zmq::socket_t &persistenceSocket, std::string clientID, std::string eventID, std::string occurenceID, int quantity){
    if (quantity <= 0) return "ERROR: Cantidad invalida";   
    std::string check = checkSeats(persistenceSocket, eventID, occurenceID, quantity);

    if (check != "OK") return check;

    Request reserve{};
    reserve.request = RESERVE;
    copy(reserve.clientID, clientID);
    copy(reserve.eventID, eventID);
    copy(reserve.occurrenceID, occurenceID);
    reserve.quantity = quantity;

    return sendPersistence(persistenceSocket, reserve);
}

/* modifyQuantity
Parametros de entrada: socket del zmq/persistencia (zmq), id del cliente (String), id de la reserva (String), nueva cantidad solicitada (int), id del evento (String), id de la ocurrencia (String)
Parametros de salida: String
Función: Llama a la función checkReservation para comprobar la existencia de la reserva, después verifica si la nueva cantidad solicitada es mayor o menor a la actual, invoca a la función checkSeats para ver si se puede hace el cambio y en caso oportuno genera la solicitud tipo MODIFY_QUANTITY para enviarla al socket de persistencia y retornar su resultado.
*/
std::string modifyQuantity (zmq::socket_t &persistenceSocket, std::string clientID, std::string reservationID, int newQuantity, std::string eventID, std::string occurenceID){
    if (newQuantity <= 0) return "ERROR: Cantidad invalida";
    std::string check_res = checkReservation(persistenceSocket, reservationID, clientID);

    int current;
    try {
        current = std::stoi(check_res);
    }catch (...) {
        return check_res;   
    }

    int extra = newQuantity - current;
    if (extra > 0){
        std::string check_seats = checkSeats(persistenceSocket, eventID, occurenceID, extra);
        if (check_seats != "OK") return check_seats;
    }

    Request modify{};
    modify.request = MODIFY_QUANTITY;
    copy(modify.clientID, clientID);
    copy(modify.eventID, eventID);
    copy(modify.occurrenceID, occurenceID);
    modify.quantity = newQuantity;
    copy(modify.reservationID, reservationID);

    return sendPersistence(persistenceSocket, modify);
}

/* modifyOccurrence
Parametros de entrada: socket del zmq/persistencia (zmq), id del cliente (String), id de la reserva (String), id del evento (String), id de la ocurrencia (String)
Parametros de salida: String
Función: Llama a la función checkReservation comprobar la existencia de la reserva, después invoca a checkOccurrences para verificar que exista la ocurrencia e 
         invoca a checkSeats para verificar que hayan suficientes asientos disponibles en la nueva ocurrencia. Finalmente genera la solicitud de tipo MODIFY_OCCURRENCE para enviarla al servicio de persistencia y retornar su resultado.
*/
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

/* checkEvents
Parametros de entrada: socket del zmq/persistencia (zmq), mes (String)
Parametros de salida: String
Función: Genera una solicitud tipo QUERY para enviarla al servicio de persistencia y retornar su resultado. 
*/
std::string checkEvents (zmq::socket_t &persistenceSocket, std::string month){
    Request events{};
    events.request = QUERY;
    copy(events.month, month);

    return sendPersistence(persistenceSocket, events);
}

int main(int argc, char *argv[]){

    if(argc != 3){
        std::cout<<"Error: Numero de argumentos incorrecto\n";
        std::cout<<"Formato: "<<argv[0]<<" <ip-rmc> <ip-persistencia>\n";
        return 1;
    }

    std::string ipRMC= "tcp://";
    ipRMC +=argv[1];
    ipRMC +=":7777";

    std::string ipPersistence= "tcp://";
    ipPersistence+=argv[2];
    ipPersistence+=":8888";


    zmq::context_t contextZMQ(1);
    zmq::socket_t managerSocket(contextZMQ, zmq::socket_type::rep);
    managerSocket.bind(ipRMC);

    zmq::socket_t persistenceSocket(contextZMQ, zmq::socket_type::req);
    persistenceSocket.connect(ipPersistence);

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

        std::cout << "[RMC] Solicitud recibida: ";
        printRequest(structuredRequest);

        
        std::string transactionResponse{};

        switch(structuredRequest.request){
            case RESERVE:
                transactionResponse = reserveSeats(persistenceSocket, structuredRequest.clientID, 
                        structuredRequest.eventID, structuredRequest.occurrenceID, structuredRequest.quantity);
                break;

            case MODIFY_OCCURRENCE:
                transactionResponse = modifyOccurrence(persistenceSocket, structuredRequest.clientID, structuredRequest.reservationID,
                        structuredRequest.eventID, structuredRequest.occurrenceID);
                break;

            case MODIFY_QUANTITY:
                transactionResponse = modifyQuantity(persistenceSocket, structuredRequest.clientID, structuredRequest.reservationID, 
                        structuredRequest.quantity, structuredRequest.eventID,structuredRequest.occurrenceID);
                break;

            case QUERY:
                transactionResponse = checkEvents(persistenceSocket, structuredRequest.month);
                break;

            default:
                transactionResponse = "ERROR: Solicitud desconocida";
                break;
        }

        std::cout << "[RMC] Respuesta: " << transactionResponse << std::endl;

        sendResponse(managerSocket, transactionResponse);
    }

}


