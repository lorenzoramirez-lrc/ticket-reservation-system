/*
Archivo: gestor-transacciones.cpp
Objetivo: Implementar el Gestor de Transacciones con alta disponibilidad mediante el patrón Binary Star (Primario/Secundario con heartbeats PUB/SUB).
          Gestiona la comunicación síncrona con clientes (REP :5555/:5557 -> REQ :7777 RMC) y asíncrona de cancelaciones (ROUTER :5556/:5558 -> PUSH :6666 Servicio de Cancelación).
Funciones: sendSynchronousRequestRMC, sendAsynchronousRequestCancel, stateName, BinaryStar, main
*/
#include <zmq.hpp>
#include <iostream>
#include <chrono>
#include <cstring>
#include <string>
#include <utility>
#include "../request-structs/structs.cpp"

constexpr int HEARTBEAT_MS   = 1000;
constexpr int PEER_EXPIRY_MS = 2000;

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

/* sendSynchronousRequestRMC
Parametros de entrada: solicitud (Request), socket zmq (socket_t)
Parametros de salida: String
Función: Envía la solicitud síncrona al socket RMC para retornar su respuesta.
*/
std::string sendSynchronousRequestRMC(Request &request, zmq::socket_t &rmcSocket){
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
Parametros de entrada: numero de argumentos a ingresar (int), cadena de caracteres con los argumentos (char*)
Parametros de salida: 1 si se encontró algún error, 0 en caso contrario.
Función: Inicializa el Gestor de Transacciones en modo primary o backup bajo el patrón Binary Star, configurando los sockets de heartbeat (PUB/SUB),
         clientes síncronos (REP), clientes asíncronos (ROUTER), RMC (REQ) y Servicio de Cancelación (PUSH).
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

    zmq::socket_t clientSocket(contextZMQ, zmq::socket_type::rep);
    clientSocket.set(zmq::sockopt::linger, 0);
    clientSocket.bind(ipManager);

    zmq::socket_t asyncClientSocket(contextZMQ, zmq::socket_type::router);
    asyncClientSocket.set(zmq::sockopt::linger, 0);
    asyncClientSocket.bind(ipAsyncManager);

    zmq::socket_t rmcSocket(contextZMQ, zmq::socket_type::req);
    rmcSocket.set(zmq::sockopt::linger, 0);
    rmcSocket.connect(ipRMC);

    zmq::socket_t cancelSocket(contextZMQ, zmq::socket_type::push);
    cancelSocket.set(zmq::sockopt::linger, 0);
    cancelSocket.connect(ipCancel);

    BinaryStar bstar(isPrimary);
    auto heartbeatAt = std::chrono::steady_clock::now() + std::chrono::milliseconds(HEARTBEAT_MS);

    std::cout << "[GESTOR-" << (isPrimary ? "PRIMARIO" : "SECUNDARIO") << "] Iniciado en estado "
              << stateName(bstar.state) << " | REP=" << ipManager
              << " | ROUTER=" << ipAsyncManager
              << " | PUB=" << ipStatePub << " | SUB=" << ipStateSub << "\n";

    while(true){
        auto now = std::chrono::steady_clock::now();
        auto timeout = std::chrono::duration_cast<std::chrono::milliseconds>(heartbeatAt - now);
        if (timeout.count() < 0) timeout = std::chrono::milliseconds(0);

        bool acceptClients = bstar.canAcceptClients(now);
        zmq::pollitem_t items[] = {
            { stateSub.handle(),          0, ZMQ_POLLIN, 0 },
            { clientSocket.handle(),      0, ZMQ_POLLIN, 0 },
            { asyncClientSocket.handle(), 0, ZMQ_POLLIN, 0 }
        };
        int nfds = acceptClients ? 3 : 1;

        zmq::poll(items, nfds, timeout);
        now = std::chrono::steady_clock::now();

        // 1. Heartbeat del par (PUB/SUB Binary Star)
        if(items[0].revents & ZMQ_POLLIN){
            zmq::message_t stateMsg;
            auto res = stateSub.recv(stateMsg, zmq::recv_flags::none);
            if(res){
                std::string s(static_cast<char*>(stateMsg.data()), stateMsg.size());
                try {
                    int peerCode = std::stoi(s);
                    bstar.peerExpiry = now + std::chrono::milliseconds(PEER_EXPIRY_MS);
                    bstar.execute(static_cast<BStarEvent>(peerCode), now);
                } catch (...) {
                    // Ignorar mensajes de heartbeat malformados
                }
            }
        }

        // 2. Canal síncrono (REQ/REP :5555 o :5557)
        if(acceptClients && (items[1].revents & ZMQ_POLLIN)){
            zmq::message_t clientRequest;
            auto result = clientSocket.recv(clientRequest, zmq::recv_flags::none);

            if(result){
                if(!bstar.execute(CLIENT_REQUEST, now)){
                    clientSocket.send(zmq::str_buffer("ERROR: gestor en modo pasivo"), zmq::send_flags::none);
                } else if(clientRequest.size() != sizeof(Request)){
                    clientSocket.send(zmq::str_buffer("ERROR: solicitud invalida"), zmq::send_flags::none);
                } else {
                    Request structuredRequest;
                    memcpy(&structuredRequest, clientRequest.data(), sizeof(Request));

                    std::cout << "[GESTOR] Solicitud Recibida de Cliente " << sizeof(structuredRequest) << " bytes\n";
                    printRequest(structuredRequest);

                    std::string transactionResponse{};
                    if(structuredRequest.request != RequestType::CANCEL){
                        transactionResponse = sendSynchronousRequestRMC(structuredRequest, rmcSocket);
                        std::cout << "[GESTOR] Respuesta recibida de RMC: " << transactionResponse << "\n";
                    } else {
                        sendAsynchronousRequestCancel(structuredRequest, cancelSocket);
                        transactionResponse = "OK: Solicitud de cancelacion recibida";
                        std::cout << "[GESTOR] Solicitud de cancelacion enviada asincronamente al Servicio de Cancelacion\n";
                    }

                    clientSocket.send(zmq::message_t(transactionResponse), zmq::send_flags::none);
                }
            }
        }

        // 3. Canal asíncrono (DEALER/ROUTER :5556 o :5558 -> PUSH/PULL :6666)
        if(acceptClients && (items[2].revents & ZMQ_POLLIN)){
            zmq::message_t identity;
            auto idResult = asyncClientSocket.recv(identity, zmq::recv_flags::none);

            if(idResult){
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

                if(!bstar.execute(CLIENT_REQUEST, now)){
                    std::cout << "[GESTOR] Solicitud asincrona ignorada: gestor en modo pasivo\n";
                } else if(!hasPayload || asyncRequest.size() != sizeof(Request)){
                    std::cout << "[GESTOR] Error: Solicitud asincrona invalida\n";
                } else {
                    Request structuredRequest;
                    memcpy(&structuredRequest, asyncRequest.data(), sizeof(Request));

                    std::cout << "[GESTOR] Solicitud Asincrona Recibida de Cliente " << sizeof(structuredRequest) << " bytes\n";
                    printRequest(structuredRequest);

                    if(structuredRequest.request == RequestType::CANCEL){
                        sendAsynchronousRequestCancel(structuredRequest, cancelSocket);
                        std::cout << "[GESTOR] Solicitud de cancelacion enviada asincronamente al Servicio de Cancelacion\n";

                        // ACK inmediato al DEALER para soportar detección de failover en el cliente
                        asyncClientSocket.send(identity, zmq::send_flags::sndmore);
                        asyncClientSocket.send(zmq::str_buffer("OK: Solicitud de cancelacion recibida"), zmq::send_flags::none);
                    } else {
                        std::cout << "[GESTOR] Error: Tipo de solicitud no valida en canal asincrono\n";
                    }
                }
            }
        }

        // 4. Envío periódico de heartbeat
        now = std::chrono::steady_clock::now();
        if(now >= heartbeatAt){
            std::string hb = std::to_string(static_cast<int>(bstar.state));
            zmq::message_t hbMsg(hb.data(), hb.size());
            statePub.send(hbMsg, zmq::send_flags::none);
            heartbeatAt = now + std::chrono::milliseconds(HEARTBEAT_MS);
        }
    }
}
