// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "amqp_mock.h"
#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <string>
#include <stdarg.h>

namespace amqp_mock {
int VALID_PORT(5672);
std::string VALID_HOST("localhost");
std::string VALID_VHOST("/");
std::string VALID_USER("guest");
std::string VALID_PASSWORD("guest");
}

using namespace amqp_mock;

struct amqp_connection_state_t_ {
  amqp_socket_t* socket;
  amqp_channel_open_ok_t* channel;
  amqp_exchange_declare_ok_t* exchange;
  bool login_called;
  // ctor
  amqp_connection_state_t_() : 
    socket(nullptr), 
    channel(nullptr),
    exchange(nullptr),
    login_called(false) {
    }
};

struct amqp_socket_t_ {
  bool open_called;
  // ctor
  amqp_socket_t_() : open_called(false) {
  }
};

amqp_connection_state_t AMQP_CALL amqp_new_connection(void) {
  auto s = new amqp_connection_state_t_;
  return s;
}

int amqp_destroy_connection(amqp_connection_state_t state) {
  delete state->socket;
  delete state->channel;
  delete state->exchange;
  delete state;
  return 0;
}

amqp_socket_t* amqp_tcp_socket_new(amqp_connection_state_t state) {
  state->socket = new amqp_socket_t;
  return state->socket;
}

int amqp_socket_open(amqp_socket_t *self, const char *host, int port) {
  if (!self) {
    return -1;
  }
  if (std::string(host) != VALID_HOST) {
    return -2;
  } 
  if (port != VALID_PORT) {
    return -3;
  }
  self->open_called = true;
  return 0;
}

amqp_rpc_reply_t amqp_login(
    amqp_connection_state_t state, 
    char const *vhost, 
    int channel_max,
    int frame_max, 
    int heartbeat, 
    amqp_sasl_method_enum sasl_method, ...) {
  amqp_rpc_reply_t reply;
  reply.reply_type = AMQP_RESPONSE_SERVER_EXCEPTION;
  reply.library_error = 0;
  reply.reply_type = AMQP_RESPONSE_NONE;
  reply.reply.decoded = nullptr;
  if (std::string(vhost) != VALID_VHOST) {
    return reply;
  }
  if (sasl_method != AMQP_SASL_METHOD_PLAIN) {
      return reply;
  }
  va_list args;
  va_start(args, sasl_method);
  char* user = va_arg(args, char*);
  char* password = va_arg(args, char*);
  va_end(args);
  if (std::string(user) != VALID_USER) {
    return reply;
  }
  if (std::string(password) != VALID_PASSWORD) {
    return reply;
  }
  reply.reply_type = AMQP_RESPONSE_NORMAL;
  state->login_called = true;
  return reply;
}

amqp_channel_open_ok_t* amqp_channel_open(amqp_connection_state_t state, amqp_channel_t channel) {
  state->channel = new amqp_channel_open_ok_t;
  return state->channel;
}

#include <string.h>

// actual implementation copied from rabbitmq-c library
amqp_bytes_t amqp_cstring_bytes(char const *cstr) {
  amqp_bytes_t result;
  result.len = strlen(cstr);
  result.bytes = (void *)cstr;
  return result;
}

amqp_exchange_declare_ok_t* amqp_exchange_declare(
    amqp_connection_state_t state, 
    amqp_channel_t channel,
    amqp_bytes_t exchange, 
    amqp_bytes_t type, 
    amqp_boolean_t passive,
    amqp_boolean_t durable, 
    amqp_boolean_t auto_delete, 
    amqp_boolean_t internal,
    amqp_table_t arguments) {
  state->exchange = new amqp_exchange_declare_ok_t;
  return state->exchange;
}

const char* amqp_error_string2(int code) {
  static const char* str = "amqp error";
  return str;
}

// amqp_parse_url() is linked via the actual rabbitmq-c library code. see: amqp_url.c

amqp_rpc_reply_t amqp_get_rpc_reply(amqp_connection_state_t state) {
  amqp_rpc_reply_t reply;
  reply.reply_type = AMQP_RESPONSE_NORMAL;
  return reply;
}

int amqp_basic_publish(
    amqp_connection_state_t state, 
    amqp_channel_t channel,
    amqp_bytes_t exchange, 
    amqp_bytes_t routing_key, 
    amqp_boolean_t mandatory,
    amqp_boolean_t immediate, 
    struct amqp_basic_properties_t_ const *properties,
    amqp_bytes_t body) {
  // make sure that all calls happened before publish
  if (state->socket && state->socket->open_called &&
      state->login_called && state->channel && state->exchange) {
    return 0;
  }
  return -1;
}

const amqp_table_t amqp_empty_table = {0, NULL};

