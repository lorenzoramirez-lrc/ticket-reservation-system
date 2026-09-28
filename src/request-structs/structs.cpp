#include <string>


enum RequestType{
    RESERVE,
    CANCEL,
    QUERY,
    MODIFY_QUANTITY,
    MODIFY_OCCURRENCE
};

struct Request{
    RequestType request;
    std::string clientID;
    std::string eventID;
    std::string occurrenceID;
    std::string reservationID;
    std::string quantity;
    std::string month;
};
