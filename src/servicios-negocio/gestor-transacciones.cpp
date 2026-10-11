/*
Archivo: gestor-transacciones.cpp
Objetivo: Realizar la comunicación con el cliente y con el servicioRMC mediante un protocolo request reply para enviar 
Funciones: sendResponse, sendPersistence, checkSeats, checkReservation, checkOccurrence, checkEvents, reserveSeats, modifyQuantity, modifyOcurrence
*/
#include <zmq.hpp>
#include <iostream>
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

    return std::string(static_cast<char*>(replyRMC.data()));
}

/* main
Parametros de entrada: numero de argumentos a ingresar (int), cadena de caracteres con los argumentos(char*)
Parametros de salida: 1 si se encontró algún error, 0 en caso contrario.
Función: Es la función principal del programa, inicializa el contexto de ZMQ para poder gestionar las redes. Además inicializa el socket del cliente y el socket del RMC para la comunicación Request-Reply. Asimismo, declara las direcciones que se van a utilizar para las comunicaciones.
         Se van a registrar las solicitudes que envíe el cliente en la consola y las solicitudes que sean enviadas al RMC. Además de que a su vez se verificará que las solicitudes recibidas sigan la estructura establecida.
*/
int main(int argc, char *argv[]){

    if(argc != 3){
        std::cout<<"Error: Numero de argumentos incorrecto\n";
        std::cout<<"Formato: "<<argv[0]<<" <ip-gestor> <ip-rmc>\n";
        return 1;
    }

    std::string ipManager = "tcp://";
    ipManager +=argv[1];
    ipManager +=":5555";

    std::string ipRMC = "tcp://";
    ipRMC +=argv[2];
    ipRMC +=":7777";

    zmq::context_t contextZMQ(1);
    zmq::socket_t clientSocket(contextZMQ, zmq::socket_type::rep);
    clientSocket.bind(ipManager);

    zmq::socket_t rmcSocket(contextZMQ, zmq::socket_type::req);
    rmcSocket.connect(ipRMC);

    while(true){
        zmq::message_t clientRequest;

        auto result  = clientSocket.recv(clientRequest, zmq::recv_flags::none);

        if(!result) continue;

        if(clientRequest.size() != sizeof(Request)){
            clientSocket.send(zmq::str_buffer("ERROR: solicitud invalida"), zmq::send_flags::none);
            continue;
        };

        Request structuredRequest; 
        memcpy(&structuredRequest, clientRequest.data(), sizeof(Request));

        std::cout<<"[GESTOR] Solicitud Recibida de Cliente "<<sizeof(structuredRequest)<<" bytes\n";
        printRequest(structuredRequest);

        std::string transactionResponse{};

        if(structuredRequest.request != RequestType::CANCEL){
            transactionResponse = sendSynchronousRequestRMC(structuredRequest, rmcSocket);
        }else{
            //cancelService
        }
        std::cout<<"[GESTOR] Respuesta recibida de RMC: "<<transactionResponse<<"\n";

        clientSocket.send(zmq::message_t(transactionResponse), zmq::send_flags::none);
    }

}
