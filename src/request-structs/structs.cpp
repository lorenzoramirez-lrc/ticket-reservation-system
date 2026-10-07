#include <string>
#include <cstring>

enum RequestType { RESERVE, CANCEL, QUERY, MODIFY_QUANTITY, MODIFY_OCCURRENCE, CHECK_SEATS, CHECK_RESERVATION, CHECK_OCCURRENCES};

struct Request {
    RequestType request;
    char clientID[32];
    char eventID[32];
    char occurrenceID[32];
    char reservationID[32];
    int quantity;
    char month[16];
};


template <size_t N>
void copy(char (&destiny)[N], const std::string &origin) {
    std::strncpy(destiny, origin.c_str(), N - 1);
    destiny[N - 1] = '\0';
}

