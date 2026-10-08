#include <zmq.hpp>
#include <iostream>
#include "../request-structs/structs.cpp"


std::string sendSynchronousRequestRMC(Request &request , zmq::socket_t &rmcSocket){

    zmq::message_t requestRMC(sizeof(request));
    memcpy(requestRMC.data(), &request, sizeof(request));

    rmcSocket.send(requestRMC, zmq::send_flags::none);

    zmq::message_t replyRMC;
    auto result = rmcSocket.recv(replyRMC, zmq::recv_flags::none);

    return std::string(static_cast<char*>(replyRMC.data()));
}

int main(int argc, char *argv[]){

    if(argc != 3){
        std::cout<<"Error: Numero de argumentos incorrecto\n";
        std::cout<<"Formato: "<<argv[0]<<" <ip-cliente> <ip-rmc>\n";
        return 1;
    }

    std::string ipClient = "tcp://";
    ipClient +=argv[1];
    ipClient +=":5555";

    std::string ipRMC = "tcp://";
    ipRMC +=argv[2];
    ipRMC +=":7777";

    zmq::context_t contextZMQ(1);
    zmq::socket_t clientSocket(contextZMQ, zmq::socket_type::rep);
    clientSocket.bind(ipClient);

    zmq::socket_t rmcSocket(contextZMQ, zmq::socket_type::req);
    rmcSocket.connect("tcp://10.43.100.20:7777");

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

        std::string transactionResponse{};

        if(structuredRequest.request != RequestType::CANCEL){
            transactionResponse = sendSynchronousRequestRMC(structuredRequest, rmcSocket);
        }else{
            //cancelService
        }

        clientSocket.send(zmq::message_t(transactionResponse), zmq::send_flags::none);
    }

}
