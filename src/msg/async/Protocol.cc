#include "Protocol.h"

#include "AsyncConnection.h"
#include "AsyncMessenger.h"

Protocol::Protocol(int type, AsyncConnection *connection)
  : proto_type(type),
    connection(connection),
    messenger(connection->async_msgr),
    cct(connection->async_msgr->cct) {}

Protocol::~Protocol() {}
