// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef _MSG_ASYNC_PROTOCOL_V2_
#define _MSG_ASYNC_PROTOCOL_V2_

#include "Protocol.h"

class ProtocolV2 : public Protocol {
private:
  enum State {
    NONE,
    START_CONNECT,
    CONNECTING,
    START_ACCEPT,
    ACCEPTING,
    CLOSED
  };

  static const char *get_state_name(int state) {
    const char *const statenames[] = {"NONE",       "START_CONNECT",
                                      "CONNECTING", "START_ACCEPT",
                                      "ACCEPTING",  "CLOSED"};
    return statenames[state];
  }

  char *temp_buffer;
  State state;

  uint64_t peer_required_features;

  using ProtFuncPtr = void (ProtocolV2::*)();
  Ct<ProtocolV2> *bannerExchangeCallback;

  ostream &_conn_prefix(std::ostream *_dout);

  Ct<ProtocolV2> *read(CONTINUATION_PARAM(next, ProtocolV2, char *, int),
                       int len, char *buffer = nullptr);
  Ct<ProtocolV2> *write(CONTINUATION_PARAM(next, ProtocolV2, int),
                        bufferlist &bl);

  inline Ct<ProtocolV2> *_fault() {
    return nullptr;
  }

  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, _banner_exchange_handle_write);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2,
                                 _banner_exchange_handle_peer_banner);

  Ct<ProtocolV2> *_banner_exchange(Ct<ProtocolV2> *callback);
  Ct<ProtocolV2> *_banner_exchange_handle_write(int r);
  Ct<ProtocolV2> *_banner_exchange_handle_peer_banner(char *buffer, int r);

public:
  ProtocolV2(AsyncConnection *connection);
  virtual ~ProtocolV2();

  virtual void connect() override;
  virtual void accept() override;
  virtual bool is_connected() override;
  virtual void stop() override;
  virtual void fault() override;
  virtual void send_message(Message *m) override;
  virtual void send_keepalive() override;

  virtual void read_event() override;
  virtual void write_event() override;
  virtual bool is_queued() override;

private:
  // Client Protocol
  CONTINUATION_DECL(ProtocolV2, start_client_banner_exchange);
  CONTINUATION_DECL(ProtocolV2, post_client_banner_exchange);

  Ct<ProtocolV2> *start_client_banner_exchange();
  Ct<ProtocolV2> *post_client_banner_exchange();

  // Server Protocol
  CONTINUATION_DECL(ProtocolV2, start_server_banner_exchange);
  CONTINUATION_DECL(ProtocolV2, post_server_banner_exchange);

  Ct<ProtocolV2> *start_server_banner_exchange();
  Ct<ProtocolV2> *post_server_banner_exchange();
};

#endif /* _MSG_ASYNC_PROTOCOL_V2_ */
