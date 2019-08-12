// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "amqp_mock.h"
#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <string>
#include <stdarg.h>
#include <mutex>
#include <boost/lockfree/queue.hpp>

namespace amqp_mock {

std::mutex set_valid_lock;
int VALID_PORT(5672);
std::string VALID_HOST("localhost");
std::string VALID_VHOST("/");
std::string VALID_USER("guest");
std::string VALID_PASSWORD("guest");

void set_valid_port(int port) {
  std::lock_guard<std::mutex> lock(set_valid_lock);
  VALID_PORT = port;
}

void set_valid_host(const std::string& host) {
  std::lock_guard<std::mutex> lock(set_valid_lock);
  VALID_HOST = host;
}

void set_valid_vhost(const std::string& vhost) {
  std::lock_guard<std::mutex> lock(set_valid_lock);
  VALID_VHOST = vhost;
}

void set_valid_user(const std::string& user, const std::string& password) {
  std::lock_guard<std::mutex> lock(set_valid_lock);
  VALID_USER = user;
  VALID_PASSWORD = password;
}

bool FAIL_NEXT_WRITE(false);
bool FAIL_NEXT_READ(false);
bool REPLY_ACK(true);
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
  boost::lockfree::queue<amqp_basic_ack_t> ack_list;
  boost::lockfree::queue<amqp_basic_nack_t> nack_list;
  std::atomic<uint64_t> delivery_tag;
  amqp_rpc_reply_t reply;
  amqp_basic_ack_t ack;
  amqp_basic_nack_t nack;
  // ctor
  amqp_connection_state_t_() : 
    socket(nullptr), 
    channel1(nullptr),
    channel2(nullptr),
    exchange(nullptr),
    queue(nullptr),
    confirm(nullptr),
    consume(nullptr),
    login_called(false),
    ack_list(1024),
    nack_list(1024),
    delivery_tag(1) {
      reply.reply_type = AMQP_RESPONSE_NONE;
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
  {
    std::lock_guard<std::mutex> lock(set_valid_lock);
    if (std::string(host) != VALID_HOST) {
      return -2;
    } 
    if (port != VALID_PORT) {
      return -3;
    }
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
  state->reply.reply_type = AMQP_RESPONSE_SERVER_EXCEPTION;
  state->reply.library_error = 0;
  state->reply.reply.decoded = nullptr;
  state->reply.reply.id = 0;
  if (std::string(vhost) != VALID_VHOST) {
    return state->reply;
  }
  if (sasl_method != AMQP_SASL_METHOD_PLAIN) {
      return state->reply;
  }
  va_list args;
  va_start(args, sasl_method);
  char* user = va_arg(args, char*);
  char* password = va_arg(args, char*);
  va_end(args);
  if (std::string(user) != VALID_USER) {
    return state->reply;
  }
  if (std::string(password) != VALID_PASSWORD) {
    return state->reply;
  }
  state->reply.reply_type = AMQP_RESPONSE_NORMAL;
  state->login_called = true;
  return state->reply;
}

amqp_channel_open_ok_t* amqp_channel_open(amqp_connection_state_t state, amqp_channel_t channel) {
  state->reply.reply_type = AMQP_RESPONSE_NORMAL;
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
  state->reply.reply_type = AMQP_RESPONSE_NORMAL;
  return state->exchange;
}

amqp_rpc_reply_t amqp_get_rpc_reply(amqp_connection_state_t state) {
  return state->reply;
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
      state->login_called && state->channel1 && state->channel2 && state->exchange &&
      !FAIL_NEXT_WRITE) {
    state->reply.reply_type = AMQP_RESPONSE_NORMAL;
    if (properties) {
      if (REPLY_ACK) {
        state->ack_list.push(amqp_basic_ack_t{state->delivery_tag++, 0});
      } else {
        state->nack_list.push(amqp_basic_nack_t{state->delivery_tag++, 0});
      }
    }
    return AMQP_STATUS_OK;
  }
  return AMQP_STATUS_CONNECTION_CLOSED;
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
  state->reply.reply_type = AMQP_RESPONSE_NORMAL;
  return state->queue;
}

amqp_confirm_select_ok_t* amqp_confirm_select(amqp_connection_state_t state, amqp_channel_t channel) {
  state->confirm = new amqp_confirm_select_ok_t;
  state->reply.reply_type = AMQP_RESPONSE_NORMAL;
  return state->confirm;
}

int amqp_simple_wait_frame_noblock(amqp_connection_state_t state, amqp_frame_t *decoded_frame, struct timeval* tv) {
  if (state->socket && state->socket->open_called &&
      state->login_called && state->channel1 && state->channel2 && state->exchange &&
      state->queue && state->consume && state->confirm && !FAIL_NEXT_READ) {
    // "wait" for queue
    usleep(tv->tv_sec*1000000+tv->tv_usec);
    // read from queue
    if (REPLY_ACK) {
      if (state->ack_list.pop(state->ack)) {
        decoded_frame->frame_type = AMQP_FRAME_METHOD;
        decoded_frame->payload.method.id = AMQP_BASIC_ACK_METHOD;
        decoded_frame->payload.method.decoded = &state->ack;
        state->reply.reply_type = AMQP_RESPONSE_NORMAL;
        return AMQP_STATUS_OK;
      } else {
        // queue is empty
        return AMQP_STATUS_TIMEOUT;
      }
    } else {
      if (state->nack_list.pop(state->nack)) {
        decoded_frame->frame_type = AMQP_FRAME_METHOD;
        decoded_frame->payload.method.id = AMQP_BASIC_NACK_METHOD;
        decoded_frame->payload.method.decoded = &state->nack;
        state->reply.reply_type = AMQP_RESPONSE_NORMAL;
        return AMQP_STATUS_OK;
      } else {
        // queue is empty
        return AMQP_STATUS_TIMEOUT;
      }
    }
  }
  return AMQP_STATUS_CONNECTION_CLOSED;
}

amqp_basic_consume_ok_t* amqp_basic_consume(
    amqp_connection_state_t state, amqp_channel_t channel, amqp_bytes_t queue,
    amqp_bytes_t consumer_tag, amqp_boolean_t no_local, amqp_boolean_t no_ack,
    amqp_boolean_t exclusive, amqp_table_t arguments) {
  state->consume = new amqp_basic_consume_ok_t;
  state->reply.reply_type = AMQP_RESPONSE_NORMAL;
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

