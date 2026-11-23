/*
Archivo: gestor-transacciones.cpp
Objetivo: Realizar la comunicación con el cliente y con los servicios de negocio (RMC de forma síncrona mediante REQ/REP y Servicio de Cancelación de forma asíncrona mediante ROUTER/DEALER y PUSH/PULL).
          Incluye instrumentación asíncrona no bloqueante para registrar métricas de transacciones completadas (TPM, status, timestamp, ids).
Funciones: sendSynchronousRequestRMC, sendAsynchronousRequestCancel, main
*/
#include <zmq.hpp>
#include <iostream>
#include <fstream>
#include <chrono>
#include <cstring>
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
    std::atomic<bool> running{true};
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
            {
                std::unique_lock<std::mutex> lock(mtx);
                cv.wait_for(lock, std::chrono::milliseconds(50), [this]() {
                    return !queue.empty() || !running;
                });

                while (!queue.empty()) {
                    batch.push_back(std::move(queue.front()));
                    queue.pop();
                }
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

            if (!running && queue.empty()) {
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
        running = false;
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
static std::atomic<bool> gKeepRunning{true};
void handleSignal(int) {
    gKeepRunning = false;
}

/* sendSynchronousRequestRMC
Parametros de entrada: solicitud (Request), socket zmq (socket_t)
Parametros de salida: String
Función: Envía la solicitud síncrona al socket RMC para retornar su respuesta.
*/
std::string sendSynchronousRequestRMC(Request &request , zmq::socket_t &rmcSocket){

    zmq::message_t requestRMC(sizeof(request));
    memcpy(requestRMC.data(), &request, sizeof(request));

    rmcSocket.send(requestRMC, zmq::send_flags::none);

    zmq::message_t replyRMC;
    auto result = rmcSocket.recv(replyRMC, zmq::recv_flags::none);
    if (!result) return "ERROR: sin respuesta de RMC";

    return std::string(static_cast<char*>(replyRMC.data()), replyRMC.size());
}

/* sendAsynchronousRequestCancel
Parametros de entrada: solicitud (Request), socket zmq de cancelación (socket_t)
Parametros de salida: Ninguno
Función: Envía la solicitud de cancelación de forma asíncrona al servicio de cancelación mediante un socket PUSH sin bloquear a la espera de respuesta.
*/
void sendAsynchronousRequestCancel(const Request &request, zmq::socket_t &cancelSocket){
    zmq::message_t requestCancel(sizeof(request));
    memcpy(requestCancel.data(), &request, sizeof(request));

    cancelSocket.send(requestCancel, zmq::send_flags::none);
}

/* main
Parametros de entrada: numero de argumentos a ingresar (int), cadena de caracteres con los argumentos(char*)
Parametros de salida: 1 si se encontró algún error, 0 en caso contrario.
Función: Es la función principal del programa, inicializa el contexto de ZMQ y los sockets tanto para el flujo síncrono (REP con cliente en :5555 y REQ con RMC en :7777) como para el flujo asíncrono de cancelaciones (ROUTER con cliente en :5556 y PUSH con el Servicio de Cancelación en :6666).
         Usa zmq::poll para atender ambos canales sin bloquearse y redirige cada solicitud al servicio correspondiente verificando que cumpla la estructura Request.
*/
int main(int argc, char *argv[]){

    if(argc != 3 && argc != 4){
        std::cout<<"Error: Numero de argumentos incorrecto\n";
        std::cout<<"Formato: "<<argv[0]<<" <ip-gestor> <ip-rmc> [<ip-cancelacion>]\n";
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

    std::string ipManager = "tcp://";
    ipManager += argv[1];
    ipManager += ":5555";

    std::string ipAsyncManager = "tcp://";
    ipAsyncManager += argv[1];
    ipAsyncManager += ":5556";

    std::string ipRMC = "tcp://";
    ipRMC += argv[2];
    ipRMC += ":7777";

    std::string ipCancel = "tcp://";
    ipCancel += (argc == 4 ? argv[3] : argv[2]);
    ipCancel += ":6666";

    zmq::context_t contextZMQ(1);

    zmq::socket_t clientSocket(contextZMQ, zmq::socket_type::rep);
    clientSocket.bind(ipManager);

    zmq::socket_t asyncClientSocket(contextZMQ, zmq::socket_type::router);
    asyncClientSocket.bind(ipAsyncManager);

    zmq::socket_t rmcSocket(contextZMQ, zmq::socket_type::req);
    rmcSocket.connect(ipRMC);

    zmq::socket_t cancelSocket(contextZMQ, zmq::socket_type::push);
    cancelSocket.connect(ipCancel);

    zmq::pollitem_t items[] = {
        { clientSocket.handle(),      0, ZMQ_POLLIN, 0 },
        { asyncClientSocket.handle(), 0, ZMQ_POLLIN, 0 }
    };

    while(gKeepRunning){
        try {
            zmq::poll(items, 2, std::chrono::milliseconds(100));
        } catch (const zmq::error_t& e) {
            if (e.num() == EINTR) {
                break;
            }
            throw;
        }

        // Canal síncrono (REQ/REP :5555)
        if(items[0].revents & ZMQ_POLLIN){
            zmq::message_t clientRequest;

            auto result = clientSocket.recv(clientRequest, zmq::recv_flags::none);

            if(!result) continue;

            if(clientRequest.size() != sizeof(Request)){
                clientSocket.send(zmq::str_buffer("ERROR: solicitud invalida"), zmq::send_flags::none);
                continue;
            }

            Request structuredRequest; 
            memcpy(&structuredRequest, clientRequest.data(), sizeof(Request));

            std::cout<<"[GESTOR] Solicitud Recibida de Cliente "<<sizeof(structuredRequest)<<" bytes\n";
            printRequest(structuredRequest);

            std::string transactionResponse{};

            if(structuredRequest.request != RequestType::CANCEL){
                transactionResponse = sendSynchronousRequestRMC(structuredRequest, rmcSocket);
                std::cout<<"[GESTOR] Respuesta recibida de RMC: "<<transactionResponse<<"\n";
            }else{
                sendAsynchronousRequestCancel(structuredRequest, cancelSocket);
                transactionResponse = "OK: Solicitud de cancelacion recibida";
                std::cout<<"[GESTOR] Solicitud de cancelacion enviada asincronamente al Servicio de Cancelacion\n";
            }

            // Instrumentación GT: Registrar métricas al terminar procesamiento
            std::string opType = (structuredRequest.request == RequestType::QUERY) ? "CONSULTA" :
                                 (structuredRequest.request == RequestType::RESERVE) ? "RESERVA" :
                                 (structuredRequest.request == RequestType::CANCEL) ? "CANCELACION" :
                                 requestTypeName(structuredRequest.request);
            std::string status = classifyStatus(transactionResponse);
            std::string runId = structuredRequest.runID[0] != '\0' ? structuredRequest.runID : "default_run";
            std::string reqId = structuredRequest.requestID[0] != '\0' ? structuredRequest.requestID : "";
            metricsLogger.log(runId, reqId, opType, status);

            clientSocket.send(zmq::message_t(transactionResponse), zmq::send_flags::none);
        }

        // Canal asíncrono (DEALER/ROUTER :5556 -> PUSH/PULL :6666)
        if(items[1].revents & ZMQ_POLLIN){
            zmq::message_t identity;
            auto idResult = asyncClientSocket.recv(identity, zmq::recv_flags::none);
            if(!idResult) continue;

            zmq::message_t asyncRequest;
            bool hasPayload = false;
            bool more = identity.more();
            while(more){
                zmq::message_t frame;
                auto res = asyncClientSocket.recv(frame, zmq::recv_flags::none);
                if(!res) break;
                more = frame.more();
                if(frame.size() > 0 || !more){
                    asyncRequest = std::move(frame);
                    hasPayload = true;
                }
            }

            if(!hasPayload || asyncRequest.size() != sizeof(Request)){
                std::cout<<"[GESTOR] Error: Solicitud asincrona invalida\n";
                continue;
            }

            Request structuredRequest;
            memcpy(&structuredRequest, asyncRequest.data(), sizeof(Request));

            std::cout<<"[GESTOR] Solicitud Asincrona Recibida de Cliente "<<sizeof(structuredRequest)<<" bytes\n";
            printRequest(structuredRequest);

            if(structuredRequest.request == RequestType::CANCEL){
                sendAsynchronousRequestCancel(structuredRequest, cancelSocket);
                std::cout<<"[GESTOR] Solicitud de cancelacion enviada asincronamente al Servicio de Cancelacion\n";
            }else{
                std::cout<<"[GESTOR] Error: Tipo de solicitud no valida en canal asincrono\n";
            }
        }
    }

    std::cout << "[GESTOR] Finalizando y asegurando volcado de métricas..." << std::endl;
    return 0;
}
