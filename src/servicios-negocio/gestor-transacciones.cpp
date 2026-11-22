/*
Archivo: gestor-transacciones.cpp
Objetivo: Realizar la comunicación con el cliente y con los servicios de negocio (RMC de forma síncrona mediante REQ/REP y Servicio de Cancelación de forma asíncrona mediante ROUTER/DEALER y PUSH/PULL).
Funciones: sendSynchronousRequestRMC, sendAsynchronousRequestCancel, main
*/
#include <zmq.hpp>
#include <iostream>
#include <chrono>
#include <cstring>
#include <utility>
#include "../request-structs/structs.cpp"

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

    while(true){
        zmq::poll(items, 2, std::chrono::milliseconds(-1));

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

}
