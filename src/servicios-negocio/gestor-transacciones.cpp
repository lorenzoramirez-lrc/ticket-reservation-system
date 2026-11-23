/*
Archivo: gestor-transacciones.cpp
Objetivo: Implementar el Gestor de Transacciones con alta disponibilidad mediante el patrón Binary Star (Primario/Secundario con heartbeats PUB/SUB).
          Gestiona solicitudes síncronas concurrentes (ROUTER :5555/:5557 -> DEALER :7777 RMC) y asíncrona de cancelaciones (ROUTER :5556/:5558 -> PUSH :6666 Servicio de Cancelación).
Funciones: stateName, classifyStatus, requestCode, traceRequest, logTransaction, sendAsynchronousRequestCancel, sendRmcRequest, replyToClient, receiveSynchronousRequest, main
*/
#include <zmq.hpp>
#include <zmq_addon.hpp>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include <iostream>
#include <fstream>
#include <chrono>
#include <cstring>
#include <string>
#include <sstream>
#include <utility>
#include <queue>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <atomic>
#include <ctime>
#include <filesystem>
#include <csignal>
#include "../request-structs/structs.cpp"

constexpr int HEARTBEAT_MS   = 1000;
constexpr int PEER_EXPIRY_MS = 2000;
constexpr int RMC_TIMEOUT_MS = 2000;
constexpr std::size_t MAX_PENDING_RMC = 4;

using Clock = std::chrono::steady_clock;

// Guarda el sobre original del cliente hasta recibir la respuesta de RMC o vencer su plazo.
struct PendingRmcRequest {
    zmq::multipart_t clientEnvelope;
    Request request;
    Clock::time_point expiresAt;
};

enum BStarState {
    STATE_PRIMARY = 1,
    STATE_BACKUP  = 2,
    STATE_ACTIVE  = 3,
    STATE_PASSIVE = 4
};

enum BStarEvent {
    PEER_PRIMARY   = 1,
    PEER_BACKUP    = 2,
    PEER_ACTIVE    = 3,
    PEER_PASSIVE   = 4,
    CLIENT_REQUEST = 5
};

const char* stateName(BStarState s) {
    switch (s) {
        case STATE_PRIMARY: return "PRIMARY";
        case STATE_BACKUP:  return "BACKUP";
        case STATE_ACTIVE:  return "ACTIVE";
        case STATE_PASSIVE: return "PASSIVE";
        default:            return "UNKNOWN";
    }
}

/* BinaryStar
Estructura que encapsula la máquina de estados finitos del patrón Binary Star
para coordinar el failover entre el Gestor Primario y el Gestor Secundario.
*/
struct BinaryStar {
    BStarState state;
    bool isPrimary;
    std::chrono::steady_clock::time_point peerExpiry;

    explicit BinaryStar(bool primary)
        : state(primary ? STATE_PRIMARY : STATE_BACKUP),
          isPrimary(primary),
          peerExpiry(std::chrono::steady_clock::now() + std::chrono::milliseconds(PEER_EXPIRY_MS)) {}

    bool canAcceptClients(std::chrono::steady_clock::time_point now) const {
        if (state == STATE_ACTIVE || state == STATE_PRIMARY) return true;
        if (state == STATE_PASSIVE && now >= peerExpiry) return true;
        return false;
    }

    bool execute(BStarEvent event, std::chrono::steady_clock::time_point now) {
        if (state == STATE_PRIMARY) {
            if (event == PEER_BACKUP) {
                std::cout << "[BSTAR] Conectado al backup (espera), pasando a ACTIVE\n";
                state = STATE_ACTIVE;
            } else if (event == PEER_ACTIVE) {
                std::cout << "[BSTAR] Conectado al backup (activo), pasando a PASSIVE\n";
                state = STATE_PASSIVE;
            } else if (event == CLIENT_REQUEST) {
                std::cout << "[BSTAR] Solicitud de cliente recibida en PRIMARY, pasando a ACTIVE\n";
                state = STATE_ACTIVE;
            }
            return true;
        }

        if (state == STATE_BACKUP) {
            if (event == PEER_ACTIVE) {
                std::cout << "[BSTAR] Conectado al primario (activo), pasando a PASSIVE\n";
                state = STATE_PASSIVE;
            } else if (event == CLIENT_REQUEST) {
                return false;
            }
            return true;
        }

        if (state == STATE_ACTIVE) {
            if (event == PEER_ACTIVE) {
                std::cout << "[BSTAR] Conflicto: dos nodos ACTIVE detectados (split-brain)\n";
                if (!isPrimary) {
                    std::cout << "[BSTAR] Secundario cede control -> pasando a PASSIVE\n";
                    state = STATE_PASSIVE;
                }
                return false;
            }
            return true;
        }

        if (state == STATE_PASSIVE) {
            if (event == PEER_PRIMARY) {
                std::cout << "[BSTAR] Primario reiniciando, asumiendo rol ACTIVE\n";
                state = STATE_ACTIVE;
            } else if (event == PEER_BACKUP) {
                std::cout << "[BSTAR] Secundario reiniciando, asumiendo rol ACTIVE\n";
                state = STATE_ACTIVE;
            } else if (event == PEER_PASSIVE) {
                if (isPrimary) {
                    std::cout << "[BSTAR] Ambos en PASSIVE, primario asume ACTIVE\n";
                    state = STATE_ACTIVE;
                }
            } else if (event == CLIENT_REQUEST) {
                if (now >= peerExpiry) {
                    std::cout << "[BSTAR] Failover: par expirado, pasando a ACTIVE\n";
                    state = STATE_ACTIVE;
                    return true;
                }
                return false;
            }
            return true;
        }

        return false;
    }
};

// Estructura de registro de transacción para métricas
struct TransactionLogEntry {
    std::string instanceId;
    std::string runId;
    std::string requestId;
    std::string operationType;
    std::string completionTimestamp;
    std::string status;
};

// Logger asíncrono no bloqueante para el Gestor de Transacciones
class AsyncTransactionLogger {
private:
    std::string instanceId;
    std::string filePath;
    std::queue<TransactionLogEntry> queue;
    std::mutex mtx;
    std::condition_variable cv;
    bool running = true;
    std::thread worker;

    void processQueue() {
        // Asegurar que el directorio padre exista
        std::filesystem::path p(filePath);
        if (p.has_parent_path()) {
            std::error_code ec;
            std::filesystem::create_directories(p.parent_path(), ec);
        }

        std::ofstream file(filePath, std::ios::app);
        if (!file.is_open()) {
            std::cerr << "[GESTOR-METRICS] Error abriendo archivo de métricas: " << filePath << std::endl;
            return;
        }

        // Si el archivo está vacío, escribir encabezado CSV
        file.seekp(0, std::ios::end);
        if (file.tellp() == 0) {
            file << "instance_id,run_id,request_id,operation_type,completion_timestamp,status\n";
            file.flush();
        }

        while (true) {
            std::vector<TransactionLogEntry> batch;
            bool done;
            {
                std::unique_lock<std::mutex> lock(mtx);
                cv.wait_for(lock, std::chrono::milliseconds(50), [this]() {
                    return !queue.empty() || !running;
                });

                while (!queue.empty()) {
                    batch.push_back(std::move(queue.front()));
                    queue.pop();
                }
                done = !running;
            }

            if (!batch.empty()) {
                for (const auto& entry : batch) {
                    file << entry.instanceId << ","
                         << entry.runId << ","
                         << entry.requestId << ","
                         << entry.operationType << ","
                         << entry.completionTimestamp << ","
                         << entry.status << "\n";
                }
                file.flush();
            }

            if (done) {
                break;
            }
        }
    }

public:
    AsyncTransactionLogger(const std::string& instId, const std::string& path)
        : instanceId(instId), filePath(path) {
        worker = std::thread(&AsyncTransactionLogger::processQueue, this);
    }

    ~AsyncTransactionLogger() {
        {
            std::lock_guard<std::mutex> lock(mtx);
            running = false;
        }
        cv.notify_all();
        if (worker.joinable()) {
            worker.join();
        }
    }

    void log(const std::string& runId, const std::string& reqId,
             const std::string& opType, const std::string& status) {
        // Timestamp UTC de alta precisión con microsegundos (ISO 8601)
        auto now = std::chrono::system_clock::now();
        auto epoch_us = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch());
        auto micros = epoch_us.count() % 1000000;
        std::time_t tt = std::chrono::system_clock::to_time_t(now);
        std::tm tm;
        gmtime_r(&tt, &tm);

        char tsBuf[64];
        std::snprintf(tsBuf, sizeof(tsBuf), "%04d-%02d-%02dT%02d:%02d:%02d.%06ldZ",
                      tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                      tm.tm_hour, tm.tm_min, tm.tm_sec, (long)micros);

        TransactionLogEntry entry{
            instanceId,
            runId.empty() ? "unknown" : runId,
            reqId.empty() ? "unknown" : reqId,
            opType,
            std::string(tsBuf),
            status
        };

        {
            std::lock_guard<std::mutex> lock(mtx);
            queue.push(std::move(entry));
        }
        cv.notify_one();
    }
};

// Clasificación de resultado para métricas
static std::string classifyStatus(const std::string& response) {
    if (response.rfind("OK", 0) == 0) {
        return "SUCCESS";
    }
    if (response.rfind("ERROR: sin respuesta", 0) == 0 ||
        response.rfind("ERROR: gestor ocupado", 0) == 0 ||
        response.rfind("ERROR: servicio de cancelacion ocupado", 0) == 0 ||
        response.rfind("ERROR: gestor en modo pasivo", 0) == 0 ||
        response.rfind("ERROR: Solicitud desconocida", 0) == 0 ||
        response.rfind("ERROR: solicitud invalida", 0) == 0) {
        return "SYSTEM_ERROR";
    }
    if (response.rfind("ERROR", 0) == 0) {
        return "BUSINESS_ERROR";
    }
    return "UNKNOWN";
}

// Variable atómica para control de terminación limpia
static_assert(std::atomic<bool>::is_always_lock_free);
static std::atomic<bool> gKeepRunning{true};
void handleSignal(int) {
    gKeepRunning = false;
}

/* requestCode
Parametros de entrada: solicitud (Request)
Parametros de salida: Código numérico de la operación
Función: Lee el código binario sin interpretar valores desconocidos como miembros de RequestType.
*/
static std::underlying_type_t<RequestType> requestCode(const Request &request) {
    std::underlying_type_t<RequestType> code;
    std::memcpy(&code, &request.request, sizeof(code));
    return code;
}

/* traceRequest
Parametros de entrada: identificador de instancia (String), mensaje de seguimiento (char*),
                      solicitud (Request), identificador interno de RMC (uint64_t) y respuesta opcional (char*)
Parametros de salida: Ninguno
Función: Imprime la operación y los identificadores originales para seguir la solicitud entre el gestor y RMC.
*/
static void traceRequest(const std::string &instanceId, const char *event, const Request &request,
                         std::uint64_t rmcId = 0, const char *response = nullptr) {
    const auto code = requestCode(request);
    const char *operation = code <= CHECK_OCCURRENCES ?
                            requestTypeName(static_cast<RequestType>(code)) : "DESCONOCIDO";
    std::ostringstream line;
    line << "[GESTOR] " << event
         << " | Instancia: " << instanceId << " | Request: " << operation
         << " | Client: " << std::string(request.clientID, strnlen(request.clientID, sizeof(request.clientID)))
         << " | UUID: " << std::string(request.requestId, strnlen(request.requestId, sizeof(request.requestId)))
         << " | Run: " << std::string(request.runID, strnlen(request.runID, sizeof(request.runID)))
         << " | ReqId: " << std::string(request.requestID, strnlen(request.requestID, sizeof(request.requestID)));
    if (rmcId != 0) line << " | RMC ID: " << rmcId;
    if (response != nullptr) line << " | Respuesta: " << response;
    std::cout << line.str() << std::endl;
}

/* logTransaction
Parametros de entrada: registrador de métricas (AsyncTransactionLogger), solicitud (Request), respuesta (String)
Parametros de salida: Ninguno
Función: Registra el resultado de la solicitud con los identificadores originales del cliente y el código de operación usado en el CSV.
*/
static void logTransaction(AsyncTransactionLogger &logger, const Request &request, const std::string &response) {
    const auto code = requestCode(request);
    std::string opType = code <= CHECK_OCCURRENCES ?
                         requestTypeName(static_cast<RequestType>(code)) : "DESCONOCIDO";
    switch (code) {
        case QUERY:   opType = "CONSULTA"; break;
        case RESERVE: opType = "RESERVA"; break;
        case CANCEL:  opType = "CANCELACION"; break;
        default: break;
    }

    std::string runId(request.runID, strnlen(request.runID, sizeof(request.runID)));
    std::string requestId(request.requestID, strnlen(request.requestID, sizeof(request.requestID)));
    logger.log(runId.empty() ? "default_run" : runId, requestId, opType, classifyStatus(response));
}

/* sendAsynchronousRequestCancel
Parametros de entrada: solicitud (Request), socket de cancelación (socket_t)
Parametros de salida: String (confirmación de encolado o error si la cola está llena)
Función: Envía la cancelación al socket PUSH sin detener el procesamiento de heartbeats y solicitudes.
*/
static std::string sendAsynchronousRequestCancel(Request request, zmq::socket_t &cancelSocket) {
    request.clientID[sizeof(request.clientID) - 1] = '\0';
    request.reservationID[sizeof(request.reservationID) - 1] = '\0';
    if (!cancelSocket.send(zmq::buffer(&request, sizeof(request)), zmq::send_flags::dontwait)) {
        return "ERROR: servicio de cancelacion ocupado";
    }
    return "OK: Solicitud de cancelacion recibida";
}

/* sendRmcRequest
Parametros de entrada: socket de RMC (socket_t), identificador interno (uint64_t), solicitud (Request)
Parametros de salida: bool (true si ZeroMQ aceptó el mensaje)
Función: Envía el identificador, un delimitador vacío y la solicitud. El delimitador permite que el socket REP
         de RMC reciba solo el cuerpo y devuelva el identificador en el sobre de respuesta.
*/
static bool sendRmcRequest(zmq::socket_t &rmcSocket, std::uint64_t requestId, const Request &request) {
    const zmq::const_buffer frames[] = {
        zmq::buffer(&requestId, sizeof(requestId)),
        zmq::const_buffer{},
        zmq::buffer(&request, sizeof(request))
    };
    return zmq::send_multipart(rmcSocket, frames, zmq::send_flags::dontwait).has_value();
}

/* replyToClient
Parametros de entrada: socket de clientes síncronos (socket_t), sobre original de ZeroMQ (multipart_t), respuesta (String)
Parametros de salida: Ninguno
Función: Adjunta la respuesta al sobre original, incluido el identificador de correlación de REQ si está presente.
*/
static void replyToClient(zmq::socket_t &clientSocket, zmq::multipart_t &envelope, const std::string &response) {
    envelope.addstr(response);
    envelope.send(clientSocket, ZMQ_DONTWAIT);
}

/* receiveSynchronousRequest
Parametros de entrada: socket de clientes síncronos (socket_t), sobre de ZeroMQ (multipart_t), solicitud (Request)
Parametros de salida: bool (true si el cuerpo contiene una sola estructura Request)
Función: Recibe el mensaje completo y conserva el sobre hasta el delimitador vacío, para responder incluso si el cuerpo es inválido.
*/
static bool receiveSynchronousRequest(zmq::socket_t &clientSocket, zmq::multipart_t &envelope, Request &request) {
    if (!envelope.recv(clientSocket, ZMQ_DONTWAIT)) {
        return false;
    }

    std::size_t body = 1;
    while (body < envelope.size() && envelope[body].size() != 0) {
        ++body;
    }
    if (body == envelope.size()) {
        envelope.clear();
        return false;
    }
    ++body;

    bool valid = envelope.size() == body + 1 && envelope[body].size() == sizeof(request);
    if (valid) {
        std::memcpy(&request, envelope[body].data(), sizeof(request));
    }
    while (envelope.size() > body) {
        envelope.remove();
    }
    return valid;
}

/* main
Parametros de entrada: numero de argumentos a ingresar (int), cadena de caracteres con los argumentos (char*)
Parametros de salida: 1 si se encontró algún error, 0 en caso contrario.
Función: Inicializa el Gestor de Transacciones en modo primary o backup bajo el patrón Binary Star, configurando los sockets de heartbeat (PUB/SUB),
         clientes síncronos y asíncronos (ROUTER), RMC (DEALER) y Servicio de Cancelación (PUSH).
         Alterna entre estos canales y los heartbeats sin esperar de forma bloqueante una respuesta de RMC.
*/
int main(int argc, char *argv[]){

    if(argc < 4 || argc > 6){
        std::cout << "Error: Numero de argumentos incorrecto\n";
        std::cout << "Formato: " << argv[0] << " <primary|backup> <ip-gestor> <ip-rmc> [<ip-cancelacion>] [<ip-par>]\n";
        return 1;
    }

    std::string role = argv[1];
    if(role != "primary" && role != "backup"){
        std::cout << "Error: El rol debe ser 'primary' o 'backup'\n";
        return 1;
    }
    // Configuración de instancia y archivo de log para métricas de TPM
    const char* envInst = std::getenv("GT_INSTANCE_ID");
    std::string instanceId = envInst ? envInst : "gt-1";

    const char* envLog = std::getenv("GT_METRICS_FILE");
    std::string logFilePath = envLog ? envLog : ("gt_transactions_" + instanceId + ".csv");

    std::cout << "[GESTOR] Iniciando Gestor de Transacciones. ID: " << instanceId
              << ", Archivo métricas: " << logFilePath << std::endl;

    AsyncTransactionLogger metricsLogger(instanceId, logFilePath);

    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);


    bool isPrimary = (role == "primary");
    std::string hostGestor = argv[2];
    std::string hostRMC    = argv[3];
    std::string hostCancel = (argc >= 5) ? argv[4] : hostRMC;
    std::string hostPeer   = (argc >= 6) ? argv[5] : hostGestor;

    std::string ipManager      = "tcp://" + hostGestor + (isPrimary ? ":5555" : ":5557");
    std::string ipAsyncManager = "tcp://" + hostGestor + (isPrimary ? ":5556" : ":5558");
    std::string ipStatePub     = "tcp://" + hostGestor + (isPrimary ? ":5003" : ":5004");
    std::string ipStateSub     = "tcp://" + hostPeer   + (isPrimary ? ":5004" : ":5003");
    std::string ipRMC          = "tcp://" + hostRMC    + ":7777";
    std::string ipCancel       = "tcp://" + hostCancel + ":6666";

    zmq::context_t contextZMQ(1);

    zmq::socket_t statePub(contextZMQ, zmq::socket_type::pub);
    statePub.set(zmq::sockopt::linger, 0);
    statePub.bind(ipStatePub);

    zmq::socket_t stateSub(contextZMQ, zmq::socket_type::sub);
    stateSub.set(zmq::sockopt::linger, 0);
    stateSub.set(zmq::sockopt::subscribe, "");
    stateSub.connect(ipStateSub);

    zmq::socket_t clientSocket(contextZMQ, zmq::socket_type::router);
    clientSocket.set(zmq::sockopt::linger, 0);
    clientSocket.bind(ipManager);

    zmq::socket_t asyncClientSocket(contextZMQ, zmq::socket_type::router);
    asyncClientSocket.set(zmq::sockopt::linger, 0);
    asyncClientSocket.bind(ipAsyncManager);

    zmq::socket_t rmcSocket(contextZMQ, zmq::socket_type::dealer);
    rmcSocket.set(zmq::sockopt::linger, 0);
    rmcSocket.set(zmq::sockopt::immediate, 1);
    rmcSocket.connect(ipRMC);

    zmq::socket_t cancelSocket(contextZMQ, zmq::socket_type::push);
    cancelSocket.set(zmq::sockopt::linger, 0);
    cancelSocket.connect(ipCancel);

    std::unordered_map<std::uint64_t, PendingRmcRequest> pendingRmc;
    std::uint64_t nextRmcId = 0;

    BinaryStar bstar(isPrimary);
    auto heartbeatAt = std::chrono::steady_clock::now() + std::chrono::milliseconds(HEARTBEAT_MS);

    std::cout << "[GESTOR-" << (isPrimary ? "PRIMARIO" : "SECUNDARIO") << "] Iniciado en estado "
              << stateName(bstar.state) << " | ROUTER sincrono=" << ipManager
              << " | ROUTER asincrono=" << ipAsyncManager
              << " | PUB=" << ipStatePub << " | SUB=" << ipStateSub << "\n";

    while (gKeepRunning) {
        auto now = Clock::now();
        bool acceptClients = bstar.canAcceptClients(now);
        zmq::pollitem_t items[] = {
            {stateSub.handle(), 0, ZMQ_POLLIN, 0},
            {rmcSocket.handle(), 0, ZMQ_POLLIN, 0},
            {clientSocket.handle(), 0, static_cast<short>(acceptClients ? ZMQ_POLLIN : 0), 0},
            {asyncClientSocket.handle(), 0, static_cast<short>(acceptClients ? ZMQ_POLLIN : 0), 0}
        };

        try {
            zmq::poll(items, 4, std::chrono::milliseconds(50));
        } catch (const zmq::error_t &error) {
            if (error.num() == EINTR) continue;
            throw;
        }
        now = Clock::now();

        // 1. Procesar el heartbeat antes de admitir solicitudes de clientes.
        if (items[0].revents & ZMQ_POLLIN) {
            zmq::message_t stateMsg;
            if (stateSub.recv(stateMsg, zmq::recv_flags::dontwait)) {
                try {
                    int peerCode = std::stoi(stateMsg.to_string());
                    if (peerCode >= PEER_PRIMARY && peerCode <= PEER_PASSIVE) {
                        bstar.peerExpiry = now + std::chrono::milliseconds(PEER_EXPIRY_MS);
                        bstar.execute(static_cast<BStarEvent>(peerCode), now);
                    }
                } catch (const std::logic_error &) {
                    // Ignorar mensajes de heartbeat malformados.
                }
            }
        }

        // 2. Entregar respuestas pendientes al cliente original, incluso si el gestor pasó a PASSIVE.
        if (items[1].revents & ZMQ_POLLIN) {
            zmq::multipart_t response;
            if (response.recv(rmcSocket, ZMQ_DONTWAIT) && response.size() == 3 &&
                response[0].size() == sizeof(std::uint64_t) && response[1].size() == 0) {
                std::uint64_t requestId;
                std::memcpy(&requestId, response[0].data(), sizeof(requestId));
                auto pending = pendingRmc.find(requestId);
                if (pending != pendingRmc.end()) {
                    auto &transaction = pending->second;
                    std::string result = response[2].to_string();
                    replyToClient(clientSocket, transaction.clientEnvelope, result);
                    traceRequest(instanceId, "Respuesta recibida de RMC", transaction.request, requestId, result.c_str());
                    logTransaction(metricsLogger, transaction.request, result);
                    pendingRmc.erase(pending);
                } else {
                    std::cout << "[GESTOR] Respuesta tardia de RMC descartada | Instancia: " << instanceId
                              << " | RMC ID: " << requestId << std::endl;
                }
            }
        }

        // 3. Vencer solicitudes sin respuesta. Una respuesta tardía ya no tendrá un identificador pendiente.
        for (auto pending = pendingRmc.begin(); pending != pendingRmc.end();) {
            auto &transaction = pending->second;
            if (now < transaction.expiresAt) {
                ++pending;
                continue;
            }
            const std::string response = "ERROR: sin respuesta de RMC";
            replyToClient(clientSocket, transaction.clientEnvelope, response);
            traceRequest(instanceId, "Tiempo de espera de RMC agotado", transaction.request,
                         pending->first, response.c_str());
            logTransaction(metricsLogger, transaction.request, response);
            pending = pendingRmc.erase(pending);
        }

        // 4. Admitir varias solicitudes síncronas mientras RMC procesa las anteriores.
        if (items[2].revents & ZMQ_POLLIN) {
            zmq::multipart_t envelope;
            Request request{};
            bool valid = receiveSynchronousRequest(clientSocket, envelope, request);
            if (!envelope.empty()) {
                if (valid) traceRequest(instanceId, "Solicitud recibida de cliente", request);
                if (!bstar.execute(CLIENT_REQUEST, now)) {
                    if (valid) traceRequest(instanceId, "Solicitud rechazada: gestor en modo pasivo", request);
                    replyToClient(clientSocket, envelope, "ERROR: gestor en modo pasivo");
                } else if (!valid) {
                    std::cout << "[GESTOR] Error: Solicitud invalida | Instancia: " << instanceId << std::endl;
                    replyToClient(clientSocket, envelope, "ERROR: solicitud invalida");
                } else {
                    std::string response;
                    if (requestCode(request) == CANCEL) {
                        response = sendAsynchronousRequestCancel(request, cancelSocket);
                    } else if (pendingRmc.size() >= MAX_PENDING_RMC) {
                        response = "ERROR: gestor ocupado";
                    } else if (sendRmcRequest(rmcSocket, ++nextRmcId, request)) {
                        pendingRmc.emplace(nextRmcId, PendingRmcRequest{
                            std::move(envelope), request, now + std::chrono::milliseconds(RMC_TIMEOUT_MS)
                        });
                        traceRequest(instanceId, "Solicitud enviada a RMC", request, nextRmcId);
                    } else {
                        response = "ERROR: sin respuesta de RMC";
                    }
                    if (!response.empty()) {
                        replyToClient(clientSocket, envelope, response);
                        const char *event = requestCode(request) == CANCEL && response.rfind("OK", 0) == 0
                            ? "Solicitud de cancelacion enviada asincronamente al Servicio de Cancelacion"
                            : "Solicitud rechazada";
                        traceRequest(instanceId, event, request, 0, response.c_str());
                        logTransaction(metricsLogger, request, response);
                    }
                }
            }
        }

        // 5. Recibir cancelaciones del canal DEALER/ROUTER y confirmar su encolado.
        if (items[3].revents & ZMQ_POLLIN) {
            zmq::multipart_t message;
            if (message.recv(asyncClientSocket, ZMQ_DONTWAIT)) {
                if (message.size() < 2 || message.back().size() != sizeof(Request)) {
                    std::cout << "[GESTOR] Error: Solicitud asincrona invalida | Instancia: " << instanceId << std::endl;
                } else {
                    Request request{};
                    std::memcpy(&request, message.back().data(), sizeof(request));
                    traceRequest(instanceId, "Solicitud asincrona recibida de cliente", request);
                    if (!bstar.execute(CLIENT_REQUEST, now)) {
                        traceRequest(instanceId, "Solicitud asincrona ignorada: gestor en modo pasivo", request);
                    } else if (requestCode(request) == CANCEL) {
                        const std::string response = sendAsynchronousRequestCancel(request, cancelSocket);
                        zmq::multipart_t reply(message.pop());
                        reply.addstr(response);
                        reply.send(asyncClientSocket, ZMQ_DONTWAIT);
                        const char *event = response.rfind("OK", 0) == 0
                            ? "Solicitud de cancelacion enviada asincronamente al Servicio de Cancelacion"
                            : "Solicitud de cancelacion rechazada";
                        traceRequest(instanceId, event, request, 0, response.c_str());
                        logTransaction(metricsLogger, request, response);
                    } else {
                        traceRequest(instanceId, "Error: Tipo de solicitud no valida en canal asincrono", request);
                    }
                }
            }
        }

        // 6. Publicar periódicamente el estado Binary Star.
        now = Clock::now();
        if (now >= heartbeatAt) {
            std::string heartbeat = std::to_string(static_cast<int>(bstar.state));
            statePub.send(zmq::buffer(heartbeat), zmq::send_flags::dontwait);
            heartbeatAt = now + std::chrono::milliseconds(HEARTBEAT_MS);
        }
    }

    std::cout << "[GESTOR] Finalizando y asegurando volcado de métricas..." << std::endl;
    return 0;
}

