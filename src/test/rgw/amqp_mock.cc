// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "amqp_mock.h"
#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <string>
#include <stdarg.h>
#include <iostream>

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
  amqp_channel_open_ok_t* channel1;
  amqp_channel_open_ok_t* channel2;
  amqp_exchange_declare_ok_t* exchange;
  amqp_queue_declare_ok_t* queue;
  amqp_confirm_select_ok_t* confirm;
  amqp_basic_consume_ok_t* consume;
  bool login_called;
  // ctor
  amqp_connection_state_t_() : 
    socket(nullptr), 
    channel1(nullptr),
    channel2(nullptr),
    exchange(nullptr),
    queue(nullptr),
    confirm(nullptr),
    consume(nullptr),
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
  delete state->channel1;
  delete state->channel2;
  delete state->exchange;
  delete state->queue;
  delete state->confirm;
  delete state->consume;
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
  if (state->channel1 == nullptr) {
    state->channel1 = new amqp_channel_open_ok_t;
    return state->channel1;
  }

  state->channel2 = new amqp_channel_open_ok_t;
  return state->channel2;
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
      state->login_called && state->channel1 && state->channel2 && state->exchange) {
    return 0;
  }
  return -1;
}

const amqp_table_t amqp_empty_table = {0, NULL};
const amqp_bytes_t amqp_empty_bytes = {0, NULL};

const char* amqp_error_string2(int code) {
  static const char* str = "mock error";
  return str;
}

char const* amqp_method_name(amqp_method_number_t methodNumber) {
  static const char* str = "mock method";
  return str;
}

amqp_queue_declare_ok_t* amqp_queue_declare(
    amqp_connection_state_t state, amqp_channel_t channel, amqp_bytes_t queue,
    amqp_boolean_t passive, amqp_boolean_t durable, amqp_boolean_t exclusive,
    amqp_boolean_t auto_delete, amqp_table_t arguments) {
  state->queue = new amqp_queue_declare_ok_t;
  static const char* str = "tmp-queue";
  state->queue->queue = amqp_cstring_bytes(str);
  return state->queue;
}

amqp_confirm_select_ok_t* amqp_confirm_select(amqp_connection_state_t state, amqp_channel_t channel) {
  state->confirm = new amqp_confirm_select_ok_t;
  return state->confirm;
}

int amqp_simple_wait_frame(amqp_connection_state_t state, amqp_frame_t *decoded_frame) {
  if (state->socket && state->socket->open_called &&
      state->login_called && state->channel1 && state->channel2 && state->exchange &&
      state->queue && state->consume && state->confirm) {
    return 0;
  }
  return -1;
}

amqp_basic_consume_ok_t* amqp_basic_consume(
    amqp_connection_state_t state, amqp_channel_t channel, amqp_bytes_t queue,
    amqp_bytes_t consumer_tag, amqp_boolean_t no_local, amqp_boolean_t no_ack,
    amqp_boolean_t exclusive, amqp_table_t arguments) {
  state->consume = new amqp_basic_consume_ok_t;
  return state->consume;
}

// amqp_parse_url() is linked via the actual rabbitmq-c library code. see: amqp_url.c

// following functions are the actual implementation copied from rabbitmq-c library

#include <string.h>

amqp_bytes_t amqp_cstring_bytes(const char* cstr) {
  amqp_bytes_t result;
  result.len = strlen(cstr);
  result.bytes = (void *)cstr;
  return result;
}

void amqp_bytes_free(amqp_bytes_t bytes) { free(bytes.bytes); }

amqp_bytes_t amqp_bytes_malloc_dup(amqp_bytes_t src) {
  amqp_bytes_t result;
  result.len = src.len;
  result.bytes = malloc(src.len);
  if (result.bytes != NULL) {
    memcpy(result.bytes, src.bytes, src.len);
  }
  return result;
}

