/*
Archivo: servicio-cancelacion.cpp
Objetivo: Recibir de forma asíncrona las solicitudes de cancelación desde el Gestor de Transacciones (PUSH/PULL :6666), comunicarse con el Servicio de Persistencia (REQ/REP :8888) para validar y cancelar la reserva, y enviar la notificación resultante al cliente (ROUTER/DEALER :9999).
Funciones: sendNotification, sendPersistence, checkReservation, cancelReservation, main
*/

#include <string>
#include <cstring>
#include <iostream>
#include <zmq.hpp>
#include "../request-structs/structs.cpp"

constexpr int PERSISTENCE_SEND_TIMEOUT_MS = 100;
constexpr int PERSISTENCE_RECV_TIMEOUT_MS = 1500;

template <size_t N>
static std::string str(const char (&a)[N]) { return std::string(a, strnlen(a, N)); }

/* sendNotification
Parametros de entrada: socket ROUTER de notificaciones (zmq), id del cliente (String), mensaje de notificación (String)
Parametros de salida: Ninguno
Función: Envía una notificación asíncrona al cliente identificado por clientID mediante el socket ROUTER (frame de identidad + frame de contenido).
*/
void sendNotification(zmq::socket_t &notificationSocket, const std::string &clientID, const std::string &notification) {
    zmq::message_t identity(clientID.data(), clientID.size());
    zmq::message_t payload(notification.data(), notification.size());

    notificationSocket.send(identity, zmq::send_flags::sndmore);
    notificationSocket.send(payload, zmq::send_flags::none);
}

/* sendPersistence
Parametros de entrada: socket del zmq/persistencia (zmq), solicitud (Request)
Parametros de salida: String
Función: Copia los datos de la solicitud, los envía al servicio de persistencia respetando los timeouts del socket y retorna la respuesta recibida.
*/
std::string sendPersistence(zmq::socket_t &persistenceSocket, const Request &req) {
    if (!persistenceSocket.send(zmq::buffer(&req, sizeof(req)), zmq::send_flags::none)) {
        return "ERROR: sin respuesta de persistencia";
    }

    zmq::message_t response;
    auto result = persistenceSocket.recv(response, zmq::recv_flags::none);
    if (!result) return "ERROR: sin respuesta de persistencia";

    return std::string(static_cast<char*>(response.data()), response.size());
}

/* checkReservation
Parametros de entrada: socket del zmq/persistencia (zmq), id de la reserva (String), id del cliente (String)
Parametros de salida: String
Función: Genera la solicitud de tipo CHECK_RESERVATION para comprobar la existencia de la reserva en la base de datos. Retorna el resultado que arroje el servicio de persistencia.
*/
std::string checkReservation(zmq::socket_t &persistenceSocket, std::string reservationID, std::string clientID) {
    Request check_reservation{};
    check_reservation.request = CHECK_RESERVATION;
    copy(check_reservation.reservationID, reservationID);
    copy(check_reservation.clientID, clientID);

    return sendPersistence(persistenceSocket, check_reservation);
}

/* cancelReservation
Parametros de entrada: socket del zmq/persistencia (zmq), solicitud original (Request)
Parametros de salida: String
Función: Preserva el requestId (UUID de idempotencia) y demás metadatos de la solicitud original, asegura la terminación nula
         de los campos y la envía al servicio de persistencia para ejecutar la cancelación atómica.
*/
std::string cancelReservation(zmq::socket_t &persistenceSocket, Request request) {
    request.request = CANCEL;
    request.requestId[sizeof(request.requestId) - 1] = '\0';
    request.clientID[sizeof(request.clientID) - 1] = '\0';
    request.reservationID[sizeof(request.reservationID) - 1] = '\0';
    request.runID[sizeof(request.runID) - 1] = '\0';
    request.requestID[sizeof(request.requestID) - 1] = '\0';

    return sendPersistence(persistenceSocket, request);
}

int main(int argc, char *argv[]) {

    if (argc != 3) {
        std::cout << "Error: Numero de argumentos incorrecto\n";
        std::cout << "Formato: " << argv[0] << " <ip-cancelacion> <ip-persistencia>\n";
        return 1;
    }

    std::string ipCancel = "tcp://";
    ipCancel += argv[1];
    ipCancel += ":6666";

    std::string ipNotification = "tcp://";
    ipNotification += argv[1];
    ipNotification += ":9999";

    std::string ipPersistence = "tcp://";
    ipPersistence += argv[2];
    ipPersistence += ":8888";

    zmq::context_t contextZMQ(1);

    // Canal asíncrono con el Gestor de Transacciones (PUSH/PULL :6666)
    zmq::socket_t managerSocket(contextZMQ, zmq::socket_type::pull);
    managerSocket.set(zmq::sockopt::linger, 0);
    managerSocket.bind(ipCancel);

    // Canal asíncrono de notificaciones hacia los Clientes (ROUTER/DEALER :9999)
    zmq::socket_t notificationSocket(contextZMQ, zmq::socket_type::router);
    notificationSocket.set(zmq::sockopt::linger, 0);
    notificationSocket.bind(ipNotification);

    // Canal síncrono con el Servicio de Persistencia (REQ/REP :8888) con timeouts y correlación
    zmq::socket_t persistenceSocket(contextZMQ, zmq::socket_type::req);
    persistenceSocket.set(zmq::sockopt::linger, 0);
    persistenceSocket.set(zmq::sockopt::immediate, 1);
    persistenceSocket.set(zmq::sockopt::sndtimeo, PERSISTENCE_SEND_TIMEOUT_MS);
    persistenceSocket.set(zmq::sockopt::rcvtimeo, PERSISTENCE_RECV_TIMEOUT_MS);
    persistenceSocket.set(zmq::sockopt::req_relaxed, 1);
    persistenceSocket.set(zmq::sockopt::req_correlate, 1);
    persistenceSocket.connect(ipPersistence);

    while (true) {
        zmq::message_t managerRequest;
        auto result = managerSocket.recv(managerRequest, zmq::recv_flags::none);
        if (!result) continue;

        if (managerRequest.size() != sizeof(Request)) {
            std::cout << "[CANCELACION] Error: Solicitud invalida (" << managerRequest.size() << " bytes)\n";
            continue;
        }

        Request structuredRequest{};
        memcpy(&structuredRequest, managerRequest.data(), sizeof(Request));

        std::cout << "[CANCELACION] Solicitud recibida: ";
        printRequest(structuredRequest);

        std::string clientID = str(structuredRequest.clientID);
        std::string reservationID = str(structuredRequest.reservationID);
        std::string cancelResponse{};

        if (structuredRequest.request == RequestType::CANCEL) {
            cancelResponse = cancelReservation(persistenceSocket, structuredRequest);
        } else {
            cancelResponse = "ERROR: Solicitud desconocida para servicio de cancelacion";
        }

        std::cout << "[CANCELACION] Respuesta para " << clientID << ": " << cancelResponse << std::endl;
        sendNotification(notificationSocket, clientID, reservationID + "|" + cancelResponse);
    }
}
