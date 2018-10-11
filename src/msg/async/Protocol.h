#ifndef _MSG_ASYNC_PROTOCOL_
#define _MSG_ASYNC_PROTOCOL_

#include <list>
#include <map>

#include "AsyncConnection.h"
#include "include/buffer.h"
#include "include/msgr.h"

/*
 * Continuation Helper Classes
 */

#include <memory>
#include <tuple>

template <class C>
class Ct {
public:
  virtual ~Ct() {}
  virtual Ct<C> *call(C *foo) const = 0;
};

template <class C, typename... Args>
class CtFun : public Ct<C> {
private:
  using fn = Ct<C> *(C::*)(Args...);
  fn _f;
  std::tuple<Args...> _params;

  template <std::size_t... Is>
  inline Ct<C> *_call(C *foo, std::index_sequence<Is...>) const {
    return (foo->*_f)(std::get<Is>(_params)...);
  }

public:
  CtFun(fn f) : _f(f) {}

  inline void setParams(Args... args) { _params = std::make_tuple(args...); }
  inline Ct<C> *call(C *foo) const override {
    return _call(foo, std::index_sequence_for<Args...>());
  }
};

#define CONTINUATION_DECL(C, F, ...)                \
  std::unique_ptr<CtFun<C, ##__VA_ARGS__>> F##_cont_ =  \
      std::make_unique<CtFun<C, ##__VA_ARGS__>>(&C::F); \
  CtFun<C, ##__VA_ARGS__> *F##_cont = F##_cont_.get()

#define CONTINUATION_PARAM(V, C, ...) CtFun<C, ##__VA_ARGS__> *V##_cont

#define CONTINUATION(F) F##_cont
#define CONTINUE(F, ...) F##_cont->setParams(__VA_ARGS__), F##_cont

#define CONTINUATION_RUN(CT)                                      \
  {                                                               \
    Ct<std::remove_reference<decltype(*this)>::type> *_cont = CT; \
    while (_cont) {                                               \
      _cont = _cont->call(this);                                  \
    }                                                             \
  }

//////////////////////////////////////////////////////////////////////

class AsyncMessenger;

class Protocol {
protected:
  AsyncConnection *connection;
  AsyncMessenger *messenger;
  CephContext *cct;

public:
  Protocol(AsyncConnection *connection);
  virtual ~Protocol();

  // prepare protocol for connecting to peer
  virtual void connect() = 0;
  // prepare protocol for accepting peer connections
  virtual void accept() = 0;
  // true -> protocol is ready for sending messages
  virtual bool is_connected() = 0;
  // stop connection
  virtual void stop() = 0;
  // signal and handle connection failure
  virtual void fault() = 0;
  // send message
  virtual void send_message(Message *m) = 0;
  // send keepalive
  virtual void send_keepalive() = 0;

  virtual void read_event() = 0;
  virtual void write_event() = 0;
  virtual bool is_queued() = 0;
};

class ProtocolV1;
using CtPtr = Ct<ProtocolV1>*;
#define READ_HANDLER_CONTINUATION_DECL(C, F) CONTINUATION_DECL(C, F, char*, int)
#define WRITE_HANDLER_CONTINUATION_DECL(C, F) CONTINUATION_DECL(C, F, int)

class ProtocolV1 : public Protocol {
/*
 *  ProtocolV1 State Machine
 *

    send_server_banner                             send_client_banner
            |                                              |
            v                                              v
    wait_client_banner                              wait_server_banner
            |                                              |
            |                                              v
            v                                 handle_server_banner_and_identify
    wait_connect_message <---------\                       |
      |     |                      |                       v
      |  wait_connect_message_auth |           send_connect_message <----------\
      |     |                      |                       |                   |
      v     v                      |                       |                   |
handle_connect_message_2           |                       v                   |
        |           |              |            wait_connect_reply             |
        v           v              |              |        |                   |
     replace -> send_connect_message_reply        |        V                   |
        |                                         |   wait_connect_reply_auth  |
        |                                         |        |                   |
        v                                         v        v                   |
      open ---\                                 handle_connect_reply_2 --------/
        |     |                                            |
        |     v                                            v
        |   wait_seq                                  wait_ack_seq
        |     |                                            |
        v     v                                            v
    server_ready                                      client_ready
            |                                              |
            \------------------> wait_message <------------/
                                 |  ^   |  ^
        /------------------------/  |   |  |
        |                           |   |  \----------------- ------------\
        v                /----------/   v                                 |
handle_keepalive2        |        handle_message_header      read_message_footer
handle_keepalive2_ack    |              |                                 ^
handle_tag_ack           |              v                                 |
        |                |        throttle_message             read_message_data
        \----------------/              |                                 ^
                                        v                                 |
                             read_message_front --> read_message_middle --/
*/

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

  void run_continuation(CtPtr continuation);
  CtPtr read(CONTINUATION_PARAM(next, ProtocolV1, char *, int), int len,
             char *buffer = nullptr);
  CtPtr write(CONTINUATION_PARAM(next, ProtocolV1, int), bufferlist &bl);
  inline CtPtr _fault() {  // helper fault method that stops continuation
    fault();
    return nullptr;
  }

  CONTINUATION_DECL(ProtocolV1, wait_message);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_message);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_keepalive2);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_keepalive2_ack);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_tag_ack);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_message_header);
  CONTINUATION_DECL(ProtocolV1, throttle_message);
  CONTINUATION_DECL(ProtocolV1, throttle_bytes);
  CONTINUATION_DECL(ProtocolV1, throttle_dispatch_queue);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_message_front);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_message_middle);
  CONTINUATION_DECL(ProtocolV1, read_message_data);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_message_data);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_message_footer);

  CtPtr ready();
  CtPtr wait_message();
  CtPtr handle_message(char *buffer, int r);

  CtPtr handle_keepalive2(char *buffer, int r);
  void append_keepalive_or_ack(bool ack = false, utime_t *t = nullptr);
  CtPtr handle_keepalive2_ack(char *buffer, int r);
  CtPtr handle_tag_ack(char *buffer, int r);

  CtPtr handle_message_header(char *buffer, int r);
  CtPtr throttle_message();
  CtPtr throttle_bytes();
  CtPtr throttle_dispatch_queue();
  CtPtr read_message_front();
  CtPtr handle_message_front(char *buffer, int r);
  CtPtr read_message_middle();
  CtPtr handle_message_middle(char *buffer, int r);
  CtPtr read_message_data_prepare();
  CtPtr read_message_data();
  CtPtr handle_message_data(char *buffer, int r);
  CtPtr read_message_footer();
  CtPtr handle_message_footer(char *buffer, int r);

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
  ProtocolV1(AsyncConnection *connection);
  virtual ~ProtocolV1();

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

  CONTINUATION_DECL(ProtocolV1, send_client_banner);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_client_banner_write);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_server_banner_and_identify);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_my_addr_write);
  CONTINUATION_DECL(ProtocolV1, send_connect_message);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_connect_message_write);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_connect_reply_1);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_connect_reply_auth);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_ack_seq);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_in_seq_write);

  CtPtr send_client_banner();
  CtPtr handle_client_banner_write(int r);
  CtPtr wait_server_banner();
  CtPtr handle_server_banner_and_identify(char *buffer, int r);
  CtPtr handle_my_addr_write(int r);
  CtPtr send_connect_message();
  CtPtr handle_connect_message_write(int r);
  CtPtr wait_connect_reply();
  CtPtr handle_connect_reply_1(char *buffer, int r);
  CtPtr wait_connect_reply_auth();
  CtPtr handle_connect_reply_auth(char *buffer, int r);
  CtPtr handle_connect_reply_2();
  CtPtr wait_ack_seq();
  CtPtr handle_ack_seq(char *buffer, int r);
  CtPtr handle_in_seq_write(int r);
  CtPtr client_ready();

  // Server Protocol
protected:
  bool wait_for_seq;

  CONTINUATION_DECL(ProtocolV1, send_server_banner);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_server_banner_write);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_client_banner);
  CONTINUATION_DECL(ProtocolV1, wait_connect_message);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_connect_message_1);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_connect_message_auth);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV1,
                                  handle_connect_message_reply_write);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV1,
                                  handle_ready_connect_message_reply_write);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV1, handle_seq);

  CtPtr send_server_banner();
  CtPtr handle_server_banner_write(int r);
  CtPtr wait_client_banner();
  CtPtr handle_client_banner(char *buffer, int r);
  CtPtr wait_connect_message();
  CtPtr handle_connect_message_1(char *buffer, int r);
  CtPtr wait_connect_message_auth();
  CtPtr handle_connect_message_auth(char *buffer, int r);
  CtPtr handle_connect_message_2();
  CtPtr send_connect_message_reply(char tag, ceph_msg_connect_reply &reply,
                                   bufferlist &authorizer_reply);
  CtPtr handle_connect_message_reply_write(int r);
  CtPtr replace(AsyncConnectionRef existing, ceph_msg_connect_reply &reply,
                bufferlist &authorizer_reply);
  CtPtr open(ceph_msg_connect_reply &reply, bufferlist &authorizer_reply);
  CtPtr handle_ready_connect_message_reply_write(int r);
  CtPtr wait_seq();
  CtPtr handle_seq(char *buffer, int r);
  CtPtr server_ready();
};

class LoopbackProtocolV1 : public ProtocolV1 {
public:
  LoopbackProtocolV1(AsyncConnection *connection) : ProtocolV1(connection) {
    this->can_write = WriteStatus::CANWRITE;
  }
};

#endif /* _MSG_ASYNC_PROTOCOL_V1_ */