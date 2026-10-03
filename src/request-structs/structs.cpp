#include <string>
#include <cstring>

enum RequestType { RESERVE, CANCEL, QUERY, MODIFY_QUANTITY, MODIFY_OCCURRENCE, CHECK_SEATS };

struct Request {
    RequestType request;
    char clientID[32];
    char eventID[32];
    char occurrenceID[32];
    char reservationID[32];
    char quantity[16];
    char month[16];
};


template <size_t N>
void copy(char (&destiny)[N], const std::string &origin) {
    std::strncpy(destiny, origin.c_str(), N - 1);
    destiny[N - 1] = '\0';
}

