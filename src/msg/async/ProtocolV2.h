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

  enum class Tag : __le32 {
    AUTH_REQUEST,
    AUTH_BAD_METHOD,
    AUTH_BAD_AUTH,
    AUTH_MORE,
    AUTH_DONE
  };

  struct Frame {
    __le32 frame_len;
    __le32 tag;
    bufferlist payload;

    Frame(Tag tag, __le32 payload_len)
        : frame_len(sizeof(__le32) + payload_len),
          tag(static_cast<__le32>(tag)) {}

    bufferlist to_bufferlist() {
      ceph_assert(payload.length() == (frame_len - sizeof(__le32)));
      bufferlist bl;
      encode(frame_len, bl, 0);
      encode(tag, bl, 0);
      bl.claim_append(payload);
      return bl;
    }
  };

  struct AuthRequestFrame : public Frame {
    __le32 method;
    __le32 len;
    bufferlist auth_payload;

    AuthRequestFrame(__le32 method, bufferlist &auth_payload)
        : Frame(Tag::AUTH_REQUEST, sizeof(__le32) * 2 + auth_payload.length()),
          method(method),
          len(auth_payload.length()),
          auth_payload(auth_payload) {
      encode(method, payload, 0);
      encode(len, payload, 0);
      payload.claim_append(auth_payload);
    }

    AuthRequestFrame(char *payload, uint32_t length)
        : Frame(Tag::AUTH_REQUEST, length) {
      method = *(__le32 *)payload;
      len = *(__le32 *)(payload + sizeof(__le32));
      ceph_assert((length - (sizeof(__le32) * 2)) == len);
      auth_payload.append((payload + (sizeof(__le32) * 2)), len);
    }
  };

  struct AuthBadMethodFrame : public Frame {
    __le32 method;
    std::vector<__u32> allowed_methods;

    AuthBadMethodFrame(__le32 method, std::vector<__u32> methods)
        : Frame(Tag::AUTH_BAD_METHOD, sizeof(__le32) * (2 + methods.size())),
          method(method),
          allowed_methods(methods) {
      encode(method, payload, 0);
      encode((__le32)allowed_methods.size(), payload, 0);
      for (const auto &a_meth : allowed_methods) {
        encode(a_meth, payload, 0);
      }
    }

    AuthBadMethodFrame(char *payload, uint32_t length)
        : Frame(Tag::AUTH_BAD_METHOD, length) {
      method = *(__le32 *)payload;
      __le32 num_methods = *(__le32 *)(payload + sizeof(__le32));
      payload += sizeof(__le32) * 2;
      for (unsigned i = 0; i < num_methods; ++i) {
        allowed_methods.push_back(*(__le32 *)(payload + sizeof(__le32) * i));
      }
    }
  };

  struct AuthBadAuthFrame : public Frame {
    __le32 error_code;
    std::string error_msg;

    AuthBadAuthFrame(__le32 error_code, std::string error_msg)
        : Frame(Tag::AUTH_BAD_AUTH, sizeof(__le32) * 2 + error_msg.size()),
          error_code(error_code),
          error_msg(error_msg) {
      encode(error_code, payload, 0);
      encode(error_msg, payload, 0);
    }

    AuthBadAuthFrame(char *payload, uint32_t length)
        : Frame(Tag::AUTH_BAD_AUTH, length) {
      error_code = *(__le32 *)payload;
      __le32 len = *(__le32 *)(payload + sizeof(__le32));
      error_msg = std::string(payload + sizeof(__le32) * 2, len);
    }
  };

  struct AuthMoreFrame : public Frame {
    __le32 len;
    bufferlist auth_payload;

    AuthMoreFrame(bufferlist &auth_payload)
        : Frame(Tag::AUTH_MORE, sizeof(__le32) + auth_payload.length()),
          len(auth_payload.length()),
          auth_payload(auth_payload) {
      encode(len, payload, 0);
      payload.claim_append(auth_payload);
    }

    AuthMoreFrame(char *payload, uint32_t length)
        : Frame(Tag::AUTH_BAD_AUTH, length) {
      len = *(__le32 *)payload;
      ceph_assert((length - sizeof(__le32)) == len);
      auth_payload.append(payload + sizeof(__le32), len);
    }
  };

  struct AuthDoneFrame : public Frame {
    __le64 flags;

    AuthDoneFrame(uint64_t flags)
        : Frame(Tag::AUTH_DONE, sizeof(__le64)), flags(flags) {
      encode(flags, payload, 0);
    }

    AuthDoneFrame(char *payload, uint32_t length)
        : Frame(Tag::AUTH_DONE, length) {
      flags = *(__le64 *)payload;
    }
  };

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
    fault();
    return nullptr;
  }

  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, _banner_exchange_handle_write);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2,
                                 _banner_exchange_handle_peer_banner);

  Ct<ProtocolV2> *_banner_exchange(Ct<ProtocolV2> *callback);
  Ct<ProtocolV2> *_banner_exchange_handle_write(int r);
  Ct<ProtocolV2> *_banner_exchange_handle_peer_banner(char *buffer, int r);

  uint32_t next_frame_len;
  CONTINUATION_DECL(ProtocolV2, read_frame);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_read_frame_length);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_frame);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_auth_more_write);

  Ct<ProtocolV2> *read_frame();
  Ct<ProtocolV2> *handle_read_frame_length(char *buffer, int r);
  Ct<ProtocolV2> *handle_frame(char *buffer, int r);
  Ct<ProtocolV2> *handle_auth_more(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_auth_more_write(int r);

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
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_auth_request_write);

  Ct<ProtocolV2> *start_client_banner_exchange();
  Ct<ProtocolV2> *post_client_banner_exchange();
  Ct<ProtocolV2> *send_auth_request(std::vector<__u32> allowed_methods = {});
  Ct<ProtocolV2> *handle_auth_request_write(int r);
  Ct<ProtocolV2> *handle_auth_bad_method(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_auth_bad_auth(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_auth_done(char *payload, uint32_t length);
  Ct<ProtocolV2> *send_client_ident();

  // Server Protocol
  CONTINUATION_DECL(ProtocolV2, start_server_banner_exchange);
  CONTINUATION_DECL(ProtocolV2, post_server_banner_exchange);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_auth_bad_method_write);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_auth_bad_auth_write);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_auth_done_write);

  Ct<ProtocolV2> *start_server_banner_exchange();
  Ct<ProtocolV2> *post_server_banner_exchange();
  Ct<ProtocolV2> *handle_auth_request(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_auth_bad_method_write(int r);
  Ct<ProtocolV2> *handle_auth_bad_auth_write(int r);
  Ct<ProtocolV2> *handle_auth_done_write(int r);
};

#endif /* _MSG_ASYNC_PROTOCOL_V2_ */
