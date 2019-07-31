// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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
  using fn_t = Ct<C> *(C::*)(Args...);
  fn_t _f;
  std::tuple<Args...> _params;

  template <std::size_t... Is>
  inline Ct<C> *_call(C *foo, std::index_sequence<Is...>) const {
    return (foo->*_f)(std::get<Is>(_params)...);
  }

public:
  CtFun(fn_t f) : _f(f) {}

  inline void setParams(Args... args) { _params = std::make_tuple(args...); }
  inline Ct<C> *call(C *foo) const override {
    return _call(foo, std::index_sequence_for<Args...>());
  }
};

using rx_buffer_t =
    std::unique_ptr<buffer::ptr_node, buffer::ptr_node::disposer>;

template <class C>
class CtRxNode : public Ct<C> {
  using fn_t = Ct<C> *(C::*)(rx_buffer_t&&, int r);
  fn_t _f;

public:
  mutable rx_buffer_t node;
  int r;

  CtRxNode(fn_t f) : _f(f) {}
  void setParams(rx_buffer_t &&node, int r) {
    this->node = std::move(node);
    this->r = r;
  }
  inline Ct<C> *call(C *foo) const override {
    return (foo->*_f)(std::move(node), r);
  }
};

template <class C> using CONTINUATION_TYPE = CtFun<C>;
template <class C> using CONTINUATION_TX_TYPE = CtFun<C, int>;
template <class C> using CONTINUATION_RX_TYPE = CtFun<C, char*, int>;
template <class C> using CONTINUATION_RXBPTR_TYPE = CtRxNode<C>;

#define CONTINUATION_DECL(C, F, ...)                    \
  CtFun<C, ##__VA_ARGS__> F##_cont { (&C::F) };

#define CONTINUATION(F) F##_cont
#define CONTINUE(F, ...) (F##_cont.setParams(__VA_ARGS__), &F##_cont)

#define CONTINUATION_RUN(CT)                                      \
  {                                                               \
    Ct<std::remove_reference<decltype(*this)>::type> *_cont = &CT;\
    do {                                                          \
      _cont = _cont->call(this);                                  \
    } while (_cont);                                              \
  }

#define READ_HANDLER_CONTINUATION_DECL(C, F) \
  CONTINUATION_DECL(C, F, char *, int)

#define READ_BPTR_HANDLER_CONTINUATION_DECL(C, F) \
  CtRxNode<C> F##_cont { (&C::F) };

#define WRITE_HANDLER_CONTINUATION_DECL(C, F) CONTINUATION_DECL(C, F, int)

//////////////////////////////////////////////////////////////////////

class AsyncMessenger;

class Protocol {
public:
  const int proto_type;
protected:
  AsyncConnection *connection;
  AsyncMessenger *messenger;
  CephContext *cct;
public:
  std::shared_ptr<AuthConnectionMeta> auth_meta;

public:
  Protocol(int type, AsyncConnection *connection);
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

  int get_con_mode() const {
    return auth_meta->con_mode;
  }
};

#endif /* _MSG_ASYNC_PROTOCOL_ */
