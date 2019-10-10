// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <librdkafka/rdkafka.h>

const char *rd_kafka_topic_name(const rd_kafka_topic_t *rkt) {
    return "";
}

rd_kafka_resp_err_t rd_kafka_last_error() {
    return rd_kafka_resp_err_t();
}

const char *rd_kafka_err2str(rd_kafka_resp_err_t err) {
    return "";
}

rd_kafka_conf_t *rd_kafka_conf_new() {
    return nullptr;
}

rd_kafka_conf_res_t rd_kafka_conf_set(rd_kafka_conf_t *conf,
				       const char *name,
				       const char *value,
				       char *errstr, size_t errstr_size) {
    return rd_kafka_conf_res_t();
}

void rd_kafka_conf_set_dr_msg_cb(rd_kafka_conf_t *conf,
                                  void (*dr_msg_cb) (rd_kafka_t *rk,
                                                     const rd_kafka_message_t *
                                                     rkmessage,
                                                     void *opaque)) {}

void rd_kafka_conf_set_opaque(rd_kafka_conf_t *conf, void *opaque) {}

rd_kafka_t *rd_kafka_new(rd_kafka_type_t type, rd_kafka_conf_t *conf,
                      char *errstr, size_t errstr_size) {
    return nullptr;
}

void rd_kafka_conf_destroy(rd_kafka_conf_t *conf) {}

rd_kafka_resp_err_t rd_kafka_flush (rd_kafka_t *rk, int timeout_ms) {
    return rd_kafka_resp_err_t();
}

void rd_kafka_destroy(rd_kafka_t *rk) {}

rd_kafka_topic_t *rd_kafka_topic_new(rd_kafka_t *rk, const char *topic,
                              rd_kafka_topic_conf_t *conf) {
    return nullptr;
}

int rd_kafka_produce(rd_kafka_topic_t *rkt, int32_t partition,
		      int msgflags,
		      void *payload, size_t len,
		      const void *key, size_t keylen,
		      void *msg_opaque) {
    return 0;
}

int rd_kafka_poll(rd_kafka_t *rk, int timeout_ms) {
    return 0;
}

void rd_kafka_topic_destroy(rd_kafka_topic_t *rkt) {}

