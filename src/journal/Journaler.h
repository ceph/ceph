// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_JOURNALER_H
#define CEPH_JOURNAL_JOURNALER_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "journal/Future.h"
#include "journal/Payload.h"
#include <string>
#include <map>
#include "include/assert.h"

class SafeTimer;

namespace journal {

class JournalMetadata;
class JournalPlayer;
class JournalRecorder;
class ReplayHandler;

class Journaler {
public:
  Journaler(librados::IoCtx &header_ioctx, librados::IoCtx &data_ioctx,
            uint64_t journal_id, const std::string &client_id);
  ~Journaler();

  int init();

  int create(uint8_t order, uint8_t splay_width);

  int register_client(const std::string &description);
  int unregister_client();

  void start_replay(ReplayHandler *replay_handler);
  void start_live_replay(ReplayHandler *replay_handler, double interval);
  bool try_pop_front(Payload *payload);
  void stop_replay();

  void start_append();
  Future append(const std::string &tag, const bufferlist &bl);
  void stop_append();

private:
  librados::IoCtx m_header_ioctx;
  librados::IoCtx m_data_ioctx;
  CephContext *m_cct;
  std::string m_client_id;

  std::string m_header_oid;
  std::string m_object_oid_prefix;

  JournalMetadata *m_metadata;
  JournalPlayer *m_player;
  JournalRecorder *m_recorder;

  void create_player(ReplayHandler *replay_handler);
};

} // namespace journal

#endif // CEPH_JOURNAL_JOURNALER_H
