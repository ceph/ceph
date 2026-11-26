// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Open Edge LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "ChangeNotifyKafka.h"

#include <algorithm>
#include <fstream>
#include <memory>

#include <librdkafka/rdkafka.h>

#include "common/Formatter.h"
#include "common/ceph_context.h"
#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds.change_notifier "

namespace {

/// Overwrite a secret once librdkafka has its own copy (best effort).
void wipe_secret(std::string &s)
{
  std::fill(s.begin(), s.end(), '\0');
  std::atomic_signal_fence(std::memory_order_seq_cst);
}

// The producer's opaque is the KafkaEndpoint, as in rgw_kafka.cc.
void kafka_delivery_report(rd_kafka_t *, const rd_kafka_message_t *msg,
                           void *opaque)
{
  static_cast<KafkaEndpoint *>(opaque)->on_delivery_report(msg->err);
}

void kafka_error(rd_kafka_t *, int, const char *reason, void *opaque)
{
  static_cast<KafkaEndpoint *>(opaque)->on_error(reason);
}

void kafka_log(const rd_kafka_t *rk, int level, const char *fac,
               const char *buf)
{
  if (auto ep = static_cast<const KafkaEndpoint *>(rd_kafka_opaque(rk)))
    ep->on_log(level, fac, buf);
}

} // namespace

std::unique_ptr<NotifyEndpoint> make_kafka_endpoint(CephContext *cct,
                                                    const KafkaOptions &opts,
                                                    std::string *err)
{
  auto ep = std::make_unique<KafkaEndpoint>(cct, opts);
  if (!ep->ok()) {
    *err = ep->last_error();
    return nullptr;
  }
  return ep;
}

bool KafkaEndpoint::read_secret(const std::string &path, std::string &out)
{
  std::ifstream f(path);
  std::getline(f, out);
  while (!out.empty() && (out.back() == '\n' || out.back() == '\r'))
    out.pop_back();
  if (out.empty()) {
    set_error("cannot read secret file " + path);
    return false;
  }
  return true;
}

KafkaEndpoint::KafkaEndpoint(CephContext *cct, const KafkaOptions &o)
  : cct(cct), opts(o), partition_key(notify_stream_key(o.fs_name, o.rank))
{
  char errstr[512] = {0};
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  // rd_kafka_new() takes ownership of conf only on success
  std::unique_ptr<rd_kafka_conf_t, decltype(&rd_kafka_conf_destroy)>
    conf_guard(conf, rd_kafka_conf_destroy);

  auto conf_set = [&](const char *key, const std::string &val) {
    if (rd_kafka_conf_set(conf, key, val.c_str(), errstr, sizeof(errstr)) ==
        RD_KAFKA_CONF_OK)
      return true;
    set_error(std::string(key) + ": " + errstr);
    return false;
  };
  // librdkafka may echo a rejected value; never let a secret reach last_error
  auto conf_set_secret = [&](const char *key, std::string &val) {
    bool ok = rd_kafka_conf_set(conf, key, val.c_str(), errstr,
                                sizeof(errstr)) == RD_KAFKA_CONF_OK;
    wipe_secret(val);
    if (!ok)
      set_error(std::string(key) + ": rejected (value withheld)");
    return ok;
  };

  const std::string proto =
    opts.security_protocol.empty() ? "PLAINTEXT" : opts.security_protocol;
  const bool ssl = proto == "SSL" || proto == "SASL_SSL";
  const bool sasl = proto == "SASL_PLAINTEXT" || proto == "SASL_SSL";
  if (!ssl && !sasl && proto != "PLAINTEXT") {
    set_error("unknown security protocol " + proto);
    return;
  }
  if (sasl && (opts.sasl_username.empty() || opts.sasl_password_file.empty())) {
    set_error(proto + " needs mds_notify_kafka_sasl_username and "
              "mds_notify_kafka_sasl_password_file");
    return;
  }

  // Idempotence keeps produce() order per partition and writes each
  // message once despite librdkafka's internal retries; messages that time
  // out in its queue are skipped without a fatal error.
  if (!conf_set("bootstrap.servers", opts.brokers) ||
      !conf_set("client.id", "ceph-mds") ||
      !conf_set("security.protocol", proto) ||
      !conf_set("message.timeout.ms", std::to_string(opts.message_timeout_ms)) ||
      !conf_set("enable.idempotence", "true") ||
      !conf_set("log.connection.close", "false") ||
      (opts.max_queue > 0 &&
       !conf_set("queue.buffering.max.messages",
                 std::to_string(opts.max_queue))))
    return;

  if (ssl) {
    if ((!opts.ssl_ca_location.empty() &&
         !conf_set("ssl.ca.location", opts.ssl_ca_location)) ||
        (!opts.ssl_certificate_location.empty() &&
         !conf_set("ssl.certificate.location", opts.ssl_certificate_location)) ||
        (!opts.ssl_key_location.empty() &&
         !conf_set("ssl.key.location", opts.ssl_key_location)) ||
        !conf_set("enable.ssl.certificate.verification",
                  opts.ssl_verify ? "true" : "false"))
      return;
    if (!opts.ssl_key_password_file.empty()) {
      std::string pw;
      if (!read_secret(opts.ssl_key_password_file, pw) ||
          !conf_set_secret("ssl.key.password", pw))
        return;
    }
  }
  if (sasl) {
    if ((!opts.sasl_mechanism.empty() &&
         !conf_set("sasl.mechanism", opts.sasl_mechanism)) ||
        !conf_set("sasl.username", opts.sasl_username))
      return;
    std::string pw;
    if (!read_secret(opts.sasl_password_file, pw) ||
        !conf_set_secret("sasl.password", pw))
      return;
  }

  // delivery reports and log lines, served by poll() on the drain thread
  // and routed into the MDS log (as rgw_kafka.cc does)
  rd_kafka_conf_set_opaque(conf, this);
  rd_kafka_conf_set_dr_msg_cb(conf, kafka_delivery_report);
  rd_kafka_conf_set_error_cb(conf, kafka_error);
  rd_kafka_conf_set_log_cb(conf, kafka_log);

  producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
  if (!producer) {
    set_error(std::string("cannot create producer: ") + errstr);
    return;
  }
  conf_guard.release();
  topic = rd_kafka_topic_new(producer, opts.topic.c_str(), nullptr);
  if (!topic) {
    set_error(std::string("cannot create topic handle: ") +
              rd_kafka_err2str(rd_kafka_last_error()));
    rd_kafka_destroy(producer);
    producer = nullptr;
    return;
  }
  dout(1) << "kafka endpoint " << opts.brokers << " topic " << opts.topic
          << " key " << partition_key << " " << proto << dendl;
}

KafkaEndpoint::~KafkaEndpoint()
{
  if (producer)
    rd_kafka_flush(producer, 500);  // bounded: a dead broker must not hang us
  if (topic)
    rd_kafka_topic_destroy(topic);
  if (producer)
    rd_kafka_destroy(producer);
}

bool KafkaEndpoint::send(const std::string &json)
{
  // keyed by rank: a rank's events stay on one partition, in order
  if (rd_kafka_produce(topic, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                       const_cast<char *>(json.data()), json.size(),
                       partition_key.data(), partition_key.size(),
                       nullptr) == -1) {
    set_error(rd_kafka_err2str(rd_kafka_last_error()));
    return false;
  }
  return true;
}

void KafkaEndpoint::poll(int timeout_ms)
{
  rd_kafka_poll(producer, timeout_ms);
}

void KafkaEndpoint::flush(int timeout_ms)
{
  rd_kafka_flush(producer, timeout_ms);
}

void KafkaEndpoint::on_delivery_report(int e)
{
  if (e == RD_KAFKA_RESP_ERR_NO_ERROR) {
    n_delivered.fetch_add(1);
    return;
  }
  // accepted by send(), now lost (message.timeout.ms expired, rejected)
  n_dropped_delivery.fetch_add(1);
  set_error(std::string("delivery failed: ") +
            rd_kafka_err2str(static_cast<rd_kafka_resp_err_t>(e)));
}

void KafkaEndpoint::on_log(int level, const char *fac, const char *buf) const
{
  // syslog levels mapped as in rgw_kafka.cc
  if (level <= 3)
    ldout(cct, 1) << "rdkafka-" << level << "-" << fac << ": " << buf << dendl;
  else if (level <= 5)
    ldout(cct, 2) << "rdkafka-" << level << "-" << fac << ": " << buf << dendl;
  else if (level <= 6)
    ldout(cct, 10) << "rdkafka-" << level << "-" << fac << ": " << buf << dendl;
  else
    ldout(cct, 20) << "rdkafka-" << level << "-" << fac << ": " << buf << dendl;
}

void KafkaEndpoint::set_error(const std::string &e)
{
  std::lock_guard l(err_lock);
  err = e;
}

std::string KafkaEndpoint::last_error() const
{
  std::lock_guard l(err_lock);
  return err;
}

void KafkaEndpoint::dump_status(ceph::Formatter *f) const
{
  // configuration only: secrets are file paths and are never reported
  f->dump_string("brokers", opts.brokers);
  f->dump_string("topic", opts.topic);
  f->dump_string("partition_key", partition_key);
  f->dump_string("security_protocol", opts.security_protocol.empty() ?
                 "PLAINTEXT" : opts.security_protocol);
  f->dump_unsigned("message_timeout_ms", opts.message_timeout_ms);
  f->dump_unsigned("max_queue", opts.max_queue);
  f->dump_unsigned("delivered", n_delivered.load());
  f->dump_unsigned("dropped_delivery", n_dropped_delivery.load());
}
