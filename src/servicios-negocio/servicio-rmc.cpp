#include <zmq.hpp>
#include "../request-structs/structs.cpp"

void sendResponse(zmq::socket_t &socket, const std::string &response) {
    zmq::message_t reply(response.data(), response.size());
    socket.send(reply, zmq::send_flags::none);
}

std::string reservarAsientos (std::string idCliente, std::string idEvento, std::string idOcurrencia, int cantidad){
    
}

std::string modificarAsientos (std::string idCliente, std::string idReserva, int nuevaCantidad){

}

std::string modificarOcurrencia (std::string idCliente, std::string idReserva){

}

std::string consultarEventos (std::string mes){

}

int main(){

    zmq::context_t contextZMQ(1);
    zmq::socket_t gestorSocket(contextZMQ, zmq::socket_type::rep);
    gestorSocket.bind("tcp://10.43.100.20:7777");

    zmq::socket_t persistenciaSocket(contextZMQ, zmq::socket_type::req);
    persistenciaSocket.connect("tcp://10.43.100.34:8888");

    while(true){
        zmq::message_t gestorRequest;

        auto result = gestorSocket.recv(gestorRequest, zmq::recv_flags::none);

        if(!result) continue;

        if (gestorRequest.size() != sizeof(Request)){
            sendResponse(gestorSocket, "ERROR: Solicitud invalida");
            continue;
        }

        Request structuredRequest; 
        memcpy(&structuredRequest, gestorRequest.data(), sizeof(Request));
        
        std::string transactionResponse{};

        switch(structuredRequest.request){
            case RESERVE:
                reservarAsientos(structuredRequest.clientID, structuredRequest.eventID, structuredRequest.occurrenceID, std::stoi(structuredRequest.quantity));
                break;

            case MODIFY_OCCURRENCE:
                modificarOcurrencia(structuredRequest.clientID, structuredRequest.reservationID);
                break;

            case MODIFY_QUANTITY:
                modificarAsientos(structuredRequest.clientID, structuredRequest.reservationID, std::stoi(structuredRequest.quantity));
                break;

            case QUERY:
                consultarEventos(structuredRequest.month);
                break;

            default:
                transactionResponse = "ERROR: Solicitud desconocida";
                break;
        }

        sendResponse(gestorSocket, transactionResponse);


    }

}


