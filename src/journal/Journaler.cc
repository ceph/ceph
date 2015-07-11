// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/Journaler.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "journal/Entry.h"
#include "journal/FutureImpl.h"
#include "journal/JournalMetadata.h"
#include "journal/JournalPlayer.h"
#include "journal/JournalRecorder.h"
#include "journal/JournalTrimmer.h"
#include "journal/PayloadImpl.h"
#include "journal/ReplayHandler.h"
#include "cls/journal/cls_journal_client.h"
#include "cls/journal/cls_journal_types.h"

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "Journaler: "

namespace journal {

namespace {

static const std::string JOURNAL_HEADER_PREFIX = "journal.";
static const std::string JOURNAL_OBJECT_PREFIX = "journal_data.";

} // anonymous namespace

using namespace cls::journal;

Journaler::Journaler(librados::IoCtx &header_ioctx, librados::IoCtx &data_ioctx,
                     const std::string &journal_id,
                     const std::string &client_id)
  : m_client_id(client_id), m_metadata(NULL), m_player(NULL), m_recorder(NULL),
    m_trimmer(NULL)
{
  m_header_ioctx.dup(header_ioctx);
  m_data_ioctx.dup(data_ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_header_ioctx.cct());

  m_header_oid = JOURNAL_HEADER_PREFIX + journal_id;
  m_object_oid_prefix = JOURNAL_OBJECT_PREFIX + journal_id + ".";

  // TODO configurable commit interval
  m_metadata = new JournalMetadata(m_header_ioctx, m_header_oid, m_client_id,
                                   5);
  m_metadata->get();

  m_trimmer = new JournalTrimmer(m_data_ioctx, m_object_oid_prefix, m_metadata);
}

Journaler::~Journaler() {
  if (m_metadata != NULL) {
    m_metadata->put();
    m_metadata = NULL;
  }
  delete m_trimmer;
  assert(m_player == NULL);
  assert(m_recorder == NULL);
}

void Journaler::init(Context *on_init) {
  m_metadata->init(on_init);
}

int Journaler::create(uint8_t order, uint8_t splay_width) {
  if (order > 64 || order < 12) {
    lderr(m_cct) << "order must be in the range [12, 64]" << dendl;
    return -EDOM;
  }
  if (splay_width == 0) {
    return -EINVAL;
  }

  ldout(m_cct, 5) << "creating new journal: " << m_header_oid << dendl;
  int r = client::create(m_header_ioctx, m_header_oid, order, splay_width);
  if (r < 0) {
    lderr(m_cct) << "failed to create journal: " << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

int Journaler::register_client(const std::string &description) {
  return m_metadata->register_client(description);
}

int Journaler::unregister_client() {
  return m_metadata->unregister_client();
}

void Journaler::start_replay(ReplayHandler *replay_handler) {
  create_player(replay_handler);
  m_player->prefetch();
}

void Journaler::start_live_replay(ReplayHandler *replay_handler,
                                  double interval) {
  create_player(replay_handler);
  m_player->prefetch_and_watch(interval);
}

bool Journaler::try_pop_front(Payload *payload) {
  assert(m_player != NULL);

  Entry entry;
  ObjectSetPosition object_set_position;
  if (!m_player->try_pop_front(&entry, &object_set_position)) {
    return false;
  }

  *payload = Payload(new PayloadImpl(entry.get_data(), object_set_position));
  return true;
}

void Journaler::stop_replay() {
  assert(m_player != NULL);
  m_player->unwatch();
  m_player->put();
  m_player = NULL;
}

void Journaler::update_commit_position(const Payload &payload) {
  PayloadImplPtr payload_impl = payload.get_payload_impl();
  m_trimmer->update_commit_position(payload_impl->get_object_set_position());
}

void Journaler::start_append() {
  assert(m_recorder == NULL);

  // TODO verify active object set >= current replay object set

  // TODO configurable flush intervals
  m_recorder = new JournalRecorder(m_data_ioctx, m_object_oid_prefix,
                                   m_metadata, 0, 0, 0);
}

int Journaler::stop_append() {
  assert(m_recorder != NULL);

  C_SaferCond cond;
  flush(&cond);
  int r = cond.wait();
  if (r < 0) {
    return r;
  }

  delete m_recorder;
  m_recorder = NULL;
  return 0;
}

Future Journaler::append(const std::string &tag, const bufferlist &payload_bl) {
  return m_recorder->append(tag, payload_bl);
}

void Journaler::flush(Context *on_safe) {
  // TODO pass ctx
  m_recorder->flush();
}

void Journaler::create_player(ReplayHandler *replay_handler) {
  assert(m_player == NULL);
  m_player = new JournalPlayer(m_data_ioctx, m_object_oid_prefix, m_metadata,
                               replay_handler);
}

} // namespace journal
