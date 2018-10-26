#ifndef _MSG_ASYNC_PROTOCOL_V2_
#define _MSG_ASYNC_PROTOCOL_V2_

#include "Protocol.h"


class ProtocolV2 : public Protocol {
protected:
  enum State {
    NONE = 0,
    START_CONNECT,
    CONNECTING,
    CONNECTING_WAIT_BANNER_AND_IDENTIFY,
    CONNECTING_SEND_CONNECT_MSG,
    START_ACCEPT,
    ACCEPTING,
    ACCEPTING_WAIT_CONNECT_MSG_AUTH,
    ACCEPTING_HANDLED_CONNECT_MSG,
    OPENED,
    THROTTLE_MESSAGE,
    THROTTLE_BYTES,
    THROTTLE_DISPATCH_QUEUE,
    READ_MESSAGE_FRONT,
    READ_FOOTER_AND_DISPATCH,
    CLOSED,
    WAIT,
    STANDBY
  };

  static const char *get_state_name(int state) {
    const char *const statenames[] = {"NONE",
                                      "START_CONNECT",
                                      "CONNECTING",
                                      "CONNECTING_WAIT_BANNER_AND_IDENTIFY",
                                      "CONNECTING_SEND_CONNECT_MSG",
                                      "START_ACCEPT",
                                      "ACCEPTING",
                                      "ACCEPTING_WAIT_CONNECT_MSG_AUTH",
                                      "ACCEPTING_HANDLED_CONNECT_MSG",
                                      "OPENED",
                                      "THROTTLE_MESSAGE",
                                      "THROTTLE_BYTES",
                                      "THROTTLE_DISPATCH_QUEUE",
                                      "READ_MESSAGE_FRONT",
                                      "READ_FOOTER_AND_DISPATCH",
                                      "CLOSED",
                                      "WAIT",
                                      "STANDBY"};
    return statenames[state];
  }

  char *temp_buffer;
  unsigned peer_addr_count;

  enum class WriteStatus { NOWRITE, REPLACING, CANWRITE, CLOSED };
  std::atomic<WriteStatus> can_write;
  std::list<Message *> sent;  // the first bufferlist need to inject seq
  // priority queue for outbound msgs
  std::map<int, std::list<std::pair<bufferlist, Message *>>> out_q;
  bool keepalive;

  __u32 connect_seq, peer_global_seq;
  std::atomic<uint64_t> in_seq{0};
  std::atomic<uint64_t> out_seq{0};
  std::atomic<uint64_t> ack_left{0};

  CryptoKey session_key;
  std::shared_ptr<AuthSessionHandler> session_security;
  std::unique_ptr<AuthAuthorizerChallenge> authorizer_challenge;  // accept side

  // Open state
  ceph_msg_connect connect_msg;
  ceph_msg_connect_reply connect_reply;
  bufferlist authorizer_buf;

  utime_t backoff;  // backoff time
  utime_t recv_stamp;
  utime_t throttle_stamp;
  unsigned msg_left;
  uint64_t cur_msg_size;
  ceph_msg_header current_header;
  bufferlist data_buf;
  bufferlist::iterator data_blp;
  bufferlist front, middle, data;

  bool replacing;  // when replacing process happened, we will reply connect
                   // side with RETRY tag and accept side will clear replaced
                   // connection. So when connect side reissue connect_msg,
                   // there won't exists conflicting connection so we use
                   // "replacing" to skip RESETSESSION to avoid detect wrong
                   // presentation
  bool is_reset_from_peer;
  bool once_ready;

  State state;

  void run_continuation(Ct<ProtocolV2> *continuation);
  Ct<ProtocolV2> *read(CONTINUATION_PARAM(next, ProtocolV2, char *, int),
                       int len, char *buffer = nullptr);
  Ct<ProtocolV2> *write(CONTINUATION_PARAM(next, ProtocolV2, int),
                        bufferlist &bl);
  inline Ct<ProtocolV2>
      *_fault() {  // helper fault method that stops continuation
    fault();
    return nullptr;
  }

  CONTINUATION_DECL(ProtocolV2, wait_message);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_message);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_keepalive2);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_keepalive2_ack);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_tag_ack);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_message_header);
  CONTINUATION_DECL(ProtocolV2, throttle_message);
  CONTINUATION_DECL(ProtocolV2, throttle_bytes);
  CONTINUATION_DECL(ProtocolV2, throttle_dispatch_queue);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_message_front);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_message_middle);
  CONTINUATION_DECL(ProtocolV2, read_message_data);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_message_data);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_message_footer);

  Ct<ProtocolV2> *ready();
  Ct<ProtocolV2> *wait_message();
  Ct<ProtocolV2> *handle_message(char *buffer, int r);

  Ct<ProtocolV2> *handle_keepalive2(char *buffer, int r);
  void append_keepalive_or_ack(bool ack = false, utime_t *t = nullptr);
  Ct<ProtocolV2> *handle_keepalive2_ack(char *buffer, int r);
  Ct<ProtocolV2> *handle_tag_ack(char *buffer, int r);

  Ct<ProtocolV2> *handle_message_header(char *buffer, int r);
  Ct<ProtocolV2> *throttle_message();
  Ct<ProtocolV2> *throttle_bytes();
  Ct<ProtocolV2> *throttle_dispatch_queue();
  Ct<ProtocolV2> *read_message_front();
  Ct<ProtocolV2> *handle_message_front(char *buffer, int r);
  Ct<ProtocolV2> *read_message_middle();
  Ct<ProtocolV2> *handle_message_middle(char *buffer, int r);
  Ct<ProtocolV2> *read_message_data_prepare();
  Ct<ProtocolV2> *read_message_data();
  Ct<ProtocolV2> *handle_message_data(char *buffer, int r);
  Ct<ProtocolV2> *read_message_footer();
  Ct<ProtocolV2> *handle_message_footer(char *buffer, int r);

  void session_reset();
  void randomize_out_seq();

  Message *_get_next_outgoing(bufferlist *bl);

  void prepare_send_message(uint64_t features, Message *m, bufferlist &bl);
  ssize_t write_message(Message *m, bufferlist &bl, bool more);

  void requeue_sent();
  uint64_t discard_requeued_up_to(uint64_t out_seq, uint64_t seq);
  void discard_out_queue();

  void reset_recv_state();

  ostream &_conn_prefix(std::ostream *_dout);

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

  // Client Protocol
private:
  int global_seq;
  bool got_bad_auth;
  AuthAuthorizer *authorizer;

  CONTINUATION_DECL(ProtocolV2, send_client_banner);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_client_banner_write);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_server_banner);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_server_addrvec_and_identify);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_my_addrs_write);
  CONTINUATION_DECL(ProtocolV2, send_connect_message);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_connect_message_write);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_connect_reply_1);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_connect_reply_auth);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_ack_seq);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_in_seq_write);

  Ct<ProtocolV2> *send_client_banner();
  Ct<ProtocolV2> *handle_client_banner_write(int r);
  Ct<ProtocolV2> *wait_server_banner();
  Ct<ProtocolV2> *handle_server_banner(char *buffer, int r);
  Ct<ProtocolV2> *handle_server_addrvec_and_identify(char *buffer, int r);
  Ct<ProtocolV2> *handle_my_addrs_write(int r);
  Ct<ProtocolV2> *send_connect_message();
  Ct<ProtocolV2> *handle_connect_message_write(int r);
  Ct<ProtocolV2> *wait_connect_reply();
  Ct<ProtocolV2> *handle_connect_reply_1(char *buffer, int r);
  Ct<ProtocolV2> *wait_connect_reply_auth();
  Ct<ProtocolV2> *handle_connect_reply_auth(char *buffer, int r);
  Ct<ProtocolV2> *handle_connect_reply_2();
  Ct<ProtocolV2> *wait_ack_seq();
  Ct<ProtocolV2> *handle_ack_seq(char *buffer, int r);
  Ct<ProtocolV2> *handle_in_seq_write(int r);
  Ct<ProtocolV2> *client_ready();

  // Server Protocol
protected:
  bool wait_for_seq;

  CONTINUATION_DECL(ProtocolV2, send_server_banner);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_server_banner_write);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_client_banner);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_client_addrvec);
  CONTINUATION_DECL(ProtocolV2, wait_connect_message);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_connect_message_1);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_connect_message_auth);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2,
                                  handle_connect_message_reply_write);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2,
                                  handle_ready_connect_message_reply_write);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_seq);

  Ct<ProtocolV2> *send_server_banner();
  Ct<ProtocolV2> *handle_server_banner_write(int r);
  Ct<ProtocolV2> *wait_client_banner();
  Ct<ProtocolV2> *handle_client_banner(char *buffer, int r);
  Ct<ProtocolV2> *handle_client_addrvec(char *buffer, int r);
  Ct<ProtocolV2> *wait_connect_message();
  Ct<ProtocolV2> *handle_connect_message_1(char *buffer, int r);
  Ct<ProtocolV2> *wait_connect_message_auth();
  Ct<ProtocolV2> *handle_connect_message_auth(char *buffer, int r);
  Ct<ProtocolV2> *handle_connect_message_2();
  Ct<ProtocolV2> *send_connect_message_reply(char tag,
                                             ceph_msg_connect_reply &reply,
                                             bufferlist &authorizer_reply);
  Ct<ProtocolV2> *handle_connect_message_reply_write(int r);
  Ct<ProtocolV2> *replace(AsyncConnectionRef existing,
                          ceph_msg_connect_reply &reply,
                          bufferlist &authorizer_reply);
  Ct<ProtocolV2> *open(ceph_msg_connect_reply &reply,
                       bufferlist &authorizer_reply);
  Ct<ProtocolV2> *handle_ready_connect_message_reply_write(int r);
  Ct<ProtocolV2> *wait_seq();
  Ct<ProtocolV2> *handle_seq(char *buffer, int r);
  Ct<ProtocolV2> *server_ready();
};

#endif /* _MSG_ASYNC_PROTOCOL_V2_ */
