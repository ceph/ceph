// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_JOURNAL_CLIENT_H
#define CEPH_CLS_JOURNAL_CLIENT_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "cls/journal/cls_journal_types.h"
#include <map>
#include <set>
#include <string>
#include <boost/optional.hpp>

class Context;

namespace cls {
namespace journal {
namespace client {

int create(librados::IoCtx &ioctx, const std::string &oid, uint8_t order,
           uint8_t splay, int64_t pool_id);

void get_immutable_metadata(librados::IoCtx &ioctx, const std::string &oid,
                            uint8_t *order, uint8_t *splay_width,
			    int64_t *pool_id, Context *on_finish);
void get_mutable_metadata(librados::IoCtx &ioctx, const std::string &oid,
                          uint64_t *minimum_set, uint64_t *active_set,
                          std::set<cls::journal::Client> *clients,
                          Context *on_finish);

void set_minimum_set(librados::ObjectWriteOperation *op, uint64_t object_set);
void set_active_set(librados::ObjectWriteOperation *op, uint64_t object_set);

// journal client helpers
int get_client(librados::IoCtx &ioctx, const std::string &oid,
               const std::string &id, cls::journal::Client *client);
void get_client_start(librados::ObjectReadOperation *op,
                      const std::string &id);
int get_client_finish(bufferlist::iterator *iter,
                      cls::journal::Client *client);

int client_register(librados::IoCtx &ioctx, const std::string &oid,
                    const std::string &id, const bufferlist &data);
void client_register(librados::ObjectWriteOperation *op,
                     const std::string &id, const bufferlist &data);

int client_update_data(librados::IoCtx &ioctx, const std::string &oid,
                       const std::string &id, const bufferlist &data);
void client_update_data(librados::ObjectWriteOperation *op,
                        const std::string &id, const bufferlist &data);
int client_update_state(librados::IoCtx &ioctx, const std::string &oid,
                        const std::string &id, cls::journal::ClientState state);

int client_unregister(librados::IoCtx &ioctx, const std::string &oid,
                      const std::string &id);
void client_unregister(librados::ObjectWriteOperation *op,
		       const std::string &id);

void client_commit(librados::ObjectWriteOperation *op, const std::string &id,
                   const cls::journal::ObjectSetPosition &commit_position);

int client_list(librados::IoCtx &ioctx, const std::string &oid,
                std::set<cls::journal::Client> *clients);

// journal tag helpers
int get_next_tag_tid(librados::IoCtx &ioctx, const std::string &oid,
                     uint64_t *tag_tid);
void get_next_tag_tid_start(librados::ObjectReadOperation *op);
int get_next_tag_tid_finish(bufferlist::iterator *iter,
                            uint64_t *tag_tid);

int get_tag(librados::IoCtx &ioctx, const std::string &oid,
            uint64_t tag_tid, cls::journal::Tag *tag);
void get_tag_start(librados::ObjectReadOperation *op,
                   uint64_t tag_tid);
int get_tag_finish(bufferlist::iterator *iter, cls::journal::Tag *tag);

int tag_create(librados::IoCtx &ioctx, const std::string &oid,
               uint64_t tag_tid, uint64_t tag_class,
               const bufferlist &data);
void tag_create(librados::ObjectWriteOperation *op,
                uint64_t tag_tid, uint64_t tag_class,
                const bufferlist &data);

int tag_list(librados::IoCtx &ioctx, const std::string &oid,
             const std::string &client_id, boost::optional<uint64_t> tag_class,
             std::set<cls::journal::Tag> *tags);
void tag_list_start(librados::ObjectReadOperation *op,
                    uint64_t start_after_tag_tid, uint64_t max_return,
                    const std::string &client_id,
                    boost::optional<uint64_t> tag_class);
int tag_list_finish(bufferlist::iterator *iter,
                    std::set<cls::journal::Tag> *tags);

// journal entry helpers
void guard_append(librados::ObjectWriteOperation *op, uint64_t soft_max_size);

} // namespace client
} // namespace journal
} // namespace cls

#endif // CEPH_CLS_JOURNAL_CLIENT_H
