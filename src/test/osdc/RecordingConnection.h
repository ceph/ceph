#pragma once

#include <vector>

#include "msg/Message.h"
#include "test/osd/MockConnection.h"

// A MockConnection that keeps what was sent on it, so that a test can assert
// on the messages a client produced rather than on its internal state.
class RecordingConnection : public MockConnection {
public:
  using MockConnection::MockConnection;
  std::vector<MessageRef> sent;

protected:
  int send_msg(MessageRef&& m) override {
    sent.push_back(std::move(m));
    return 0;
  }
};
