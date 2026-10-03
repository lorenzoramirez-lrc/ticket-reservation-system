#include <zmq.hpp>
#include "../request-structs/structs.cpp"


std::string sendSynchronousRequestRMC(Request &request , zmq::socket_t &rmcSocket){

    zmq::message_t requestRMC(sizeof(request));
    memcpy(requestRMC.data(), &request, sizeof(request));

    rmcSocket.send(requestRMC, zmq::send_flags::none);

    zmq::message_t replyRMC;
    auto result = rmcSocket.recv(replyRMC, zmq::recv_flags::none);

    return std::string(static_cast<char*>(replyRMC.data()));
}

int main(){

    zmq::context_t contextZMQ(1);
    zmq::socket_t clientSocket(contextZMQ, zmq::socket_type::rep);
    clientSocket.bind("tcp://10.43.99.201:5555");

    zmq::socket_t rmcSocket(contextZMQ, zmq::socket_type::req);
    clientSocket.connect("tcp://10.43.100.20:7777");

    while(true){
        zmq::message_t clientRequest;

        auto result  = clientSocket.recv(clientRequest, zmq::recv_flags::none);

        if(!result || clientRequest.size() != sizeof(Request)) continue;

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
