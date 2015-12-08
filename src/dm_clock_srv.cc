#include "dm_clock_srv.h"


std::ostream& dmc::operator<<(std::ostream& out, const dmc::ClientInfo& client) {
    if (client.isUnset()) {
        out << "unset";
    } else {
        out << "{w:" << client.weight << " r:" << client.reservation <<
            " l:" << client.limit << " t:" << client.prevTag << "}";
    }
    return out;
}


std::ostream& dmc::operator<<(std::ostream& out, const dmc::RequestTag& tag) {
    out << "{p:" << tag.proportion << " r:" << tag.reservation <<
        " l:" << tag.limit << "}";
    return out;
}
