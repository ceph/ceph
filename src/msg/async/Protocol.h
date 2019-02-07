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

#define CONTINUATION_DECL(C, F, ...)                    \
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

#define READ_HANDLER_CONTINUATION_DECL(C, F) \
  CONTINUATION_DECL(C, F, char *, int)
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

};

#endif /* _MSG_ASYNC_PROTOCOL_ */
