/*
Archivo: servicio-rmc.cpp
Objetivo: Atender solicitudes concurrentes del Gestor de Transacciones mediante un distribuidor ROUTER/DEALER y un grupo de trabajadores.
Funciones: sendResponse, sendPersistence, traceRequest, processRequest, runWorker, main
*/

#include <cerrno>
#include <cstring>
#include <exception>
#include <functional>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>
#include <zmq.hpp>
#include <zmq_addon.hpp>
#include "../request-structs/structs.cpp"

constexpr int RMC_WORKERS = 4;
constexpr int PERSISTENCE_SEND_TIMEOUT_MS = 100;
constexpr int PERSISTENCE_RECV_TIMEOUT_MS = 1500;
constexpr char WORKER_ENDPOINT[] = "inproc://rmc-workers";

/* sendResponse
Parametros de entrada: socket de respuestas (socket_t), respuesta (String)
Parametros de salida: Ninguno
Función: Envía la respuesta al distribuidor interno. El socket REP conserva el sobre para que el ROUTER la entregue al gestor correcto.
*/
static void sendResponse(zmq::socket_t &socket, const std::string &response) {
    socket.send(zmq::buffer(response), zmq::send_flags::none);
}

/* sendPersistence
Parametros de entrada: socket de persistencia del trabajador (socket_t), solicitud (Request)
Parametros de salida: String (resultado de persistencia o error de comunicación)
Función: Envía la solicitud una sola vez y espera dentro del plazo del socket. Si la espera vence, devuelve un error
         y el socket puede iniciar otra solicitud con un identificador de correlación nuevo.
*/
static std::string sendPersistence(zmq::socket_t &persistenceSocket, const Request &request) {
    if (!persistenceSocket.send(zmq::buffer(&request, sizeof(request)), zmq::send_flags::none)) {
        return "ERROR: sin respuesta de persistencia";
    }

    zmq::message_t response;
    if (!persistenceSocket.recv(response, zmq::recv_flags::none)) {
        return "ERROR: sin respuesta de persistencia";
    }
    return response.to_string();
}

/* traceRequest
Parametros de entrada: número de trabajador (int), mensaje de seguimiento (char*), solicitud (Request)
                      y respuesta opcional (char*)
Parametros de salida: Ninguno
Función: Imprime la operación y sus identificadores en un solo bloque para que los trabajadores
         no mezclen sus mensajes de seguimiento en la consola.
*/
static void traceRequest(int workerId, const char *event, const Request &request, const char *response = nullptr) {
    std::underlying_type_t<RequestType> code;
    std::memcpy(&code, &request.request, sizeof(code));
    const char *operation = code <= CHECK_OCCURRENCES ?
                            requestTypeName(static_cast<RequestType>(code)) : "DESCONOCIDO";
    std::ostringstream line;
    line << "[RMC] " << event << " | Hilo: " << workerId << " | Request: " << operation
         << " | Client: " << std::string(request.clientID, strnlen(request.clientID, sizeof(request.clientID)))
         << " | UUID: " << std::string(request.requestId, strnlen(request.requestId, sizeof(request.requestId)))
         << " | Run: " << std::string(request.runID, strnlen(request.runID, sizeof(request.runID)))
         << " | ReqId: " << std::string(request.requestID, strnlen(request.requestID, sizeof(request.requestID)));
    if (response != nullptr) line << " | Respuesta: " << response;
    static std::mutex traceMutex;
    std::lock_guard<std::mutex> lock(traceMutex);
    std::cout << line.str() << std::endl;
}

/* processRequest
Parametros de entrada: socket de persistencia del trabajador (socket_t), solicitud (Request)
Parametros de salida: String (resultado de la operación o error de validación)
Función: Valida el tipo y la cantidad cuando aplica, y remite la solicitud completa a persistencia.
         La disponibilidad, la titularidad y el estado de la reserva se verifican dentro de la transacción que aplica el cambio.
*/
static std::string processRequest(zmq::socket_t &persistenceSocket, Request request) {
    std::underlying_type_t<RequestType> operation;
    std::memcpy(&operation, &request.request, sizeof(operation));

    switch (operation) {
        case RESERVE:
        case MODIFY_QUANTITY:
            if (request.quantity <= 0) return "ERROR: Cantidad invalida";
            break;
        case MODIFY_OCCURRENCE:
        case QUERY:
            break;
        default:
            return "ERROR: Solicitud desconocida";
    }

    request.clientID[sizeof(request.clientID) - 1] = '\0';
    request.eventID[sizeof(request.eventID) - 1] = '\0';
    request.occurrenceID[sizeof(request.occurrenceID) - 1] = '\0';
    request.reservationID[sizeof(request.reservationID) - 1] = '\0';
    request.month[sizeof(request.month) - 1] = '\0';
    return sendPersistence(persistenceSocket, request);
}

/* runWorker
Parametros de entrada: contexto compartido de ZeroMQ (context_t), dirección del servicio de persistencia (String), número de trabajador (int)
Parametros de salida: Ninguno
Función: Atiende solicitudes del distribuidor con sockets REP y REQ exclusivos de este hilo. Configura plazos de
         comunicación y correlación en REQ para descartar respuestas tardías de operaciones anteriores.
*/
static void runWorker(zmq::context_t &context, const std::string &ipPersistence, int workerId) {
    try {
        zmq::socket_t persistenceSocket(context, zmq::socket_type::req);
        persistenceSocket.set(zmq::sockopt::linger, 0);
        persistenceSocket.set(zmq::sockopt::immediate, 1);
        persistenceSocket.set(zmq::sockopt::sndtimeo, PERSISTENCE_SEND_TIMEOUT_MS);
        persistenceSocket.set(zmq::sockopt::rcvtimeo, PERSISTENCE_RECV_TIMEOUT_MS);
        persistenceSocket.set(zmq::sockopt::req_relaxed, 1);
        persistenceSocket.set(zmq::sockopt::req_correlate, 1);
        persistenceSocket.connect(ipPersistence);

        zmq::socket_t workerSocket(context, zmq::socket_type::rep);
        workerSocket.set(zmq::sockopt::linger, 0);
        workerSocket.set(zmq::sockopt::rcvhwm, 1);
        workerSocket.connect(WORKER_ENDPOINT);

        while (true) {
            zmq::multipart_t message;
            if (!message.recv(workerSocket)) continue;

            if (message.size() != 1 || message[0].size() != sizeof(Request)) {
                sendResponse(workerSocket, "ERROR: Solicitud invalida");
                continue;
            }

            Request request{};
            std::memcpy(&request, message[0].data(), sizeof(request));
            traceRequest(workerId, "Solicitud recibida", request);
            const std::string response = processRequest(persistenceSocket, request);
            sendResponse(workerSocket, response);
            traceRequest(workerId, "Respuesta enviada al gestor", request, response.c_str());
        }
    } catch (const zmq::error_t &error) {
        if (error.num() != ETERM) {
            std::cerr << "[RMC] Error en trabajador: " << error.what() << '\n';
            context.shutdown();
        }
    } catch (const std::exception &error) {
        std::cerr << "[RMC] Error en trabajador: " << error.what() << '\n';
        context.shutdown();
    }
}

/* main
Parametros de entrada: número de argumentos (int), argumentos de ejecución (char**)
Parametros de salida: 1 si ocurre un error, 0 en caso contrario
Función: Vincula un ROUTER para los gestores y un DEALER interno para distribuir solicitudes entre los trabajadores.
         Al cerrar el contexto, desbloquea los sockets de los trabajadores y espera la terminación de los hilos.
*/
int main(int argc, char *argv[]) {
    if (argc != 3) {
        std::cout << "Error: Numero de argumentos incorrecto\n";
        std::cout << "Formato: " << argv[0] << " <ip-rmc> <ip-persistencia>\n";
        return 1;
    }

    const std::string ipRMC = std::string("tcp://") + argv[1] + ":7777";
    const std::string ipPersistence = std::string("tcp://") + argv[2] + ":8888";

    zmq::context_t context(1);
    std::vector<std::thread> workers;
    int status = 0;

    try {
        zmq::socket_t managerSocket(context, zmq::socket_type::router);
        managerSocket.set(zmq::sockopt::linger, 0);
        managerSocket.bind(ipRMC);

        zmq::socket_t distributor(context, zmq::socket_type::dealer);
        distributor.set(zmq::sockopt::linger, 0);
        distributor.set(zmq::sockopt::sndhwm, 1);
        distributor.bind(WORKER_ENDPOINT);

        workers.reserve(RMC_WORKERS);
        for (int i = 0; i < RMC_WORKERS; ++i) {
            workers.emplace_back(runWorker, std::ref(context), std::cref(ipPersistence), i + 1);
        }

        std::cout << "[RMC] Escuchando en " << ipRMC << " con " << RMC_WORKERS << " trabajadores" << std::endl;
        zmq::proxy(managerSocket, distributor);
    } catch (const std::exception &error) {
        std::cerr << "[RMC] Error en distribuidor: " << error.what() << '\n';
        status = 1;
    }

    context.shutdown();
    for (auto &worker : workers) {
        worker.join();
    }

    return status;
}
