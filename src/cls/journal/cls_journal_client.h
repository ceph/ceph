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

class Context;

namespace cls {
namespace journal {
namespace client {

int create(librados::IoCtx &ioctx, const std::string &oid, uint8_t order,
           uint8_t splay);

void get_immutable_metadata(librados::IoCtx &ioctx, const std::string &oid,
                            uint8_t *order, uint8_t *splay_width,
                            Context *on_finish);
void get_mutable_metadata(librados::IoCtx &ioctx, const std::string &oid,
                          uint64_t *minimum_set, uint64_t *active_set,
                          std::set<cls::journal::Client> *clients,
                          Context *on_finish);

void set_minimum_set(librados::ObjectWriteOperation *op, uint64_t object_set);
void set_active_set(librados::ObjectWriteOperation *op, uint64_t object_set);

void client_register(librados::ObjectWriteOperation *op,
                     const std::string &id, const std::string &description);
int client_register(librados::IoCtx &ioctx, const std::string &oid,
                    const std::string &id, const std::string &description);
int client_unregister(librados::IoCtx &ioctx, const std::string &oid,
                      const std::string &id);
void client_commit(librados::ObjectWriteOperation *op, const std::string &id,
                   const cls::journal::ObjectSetPosition &commit_position);
int client_list(librados::IoCtx &ioctx, const std::string &oid,
                std::set<cls::journal::Client> *clients);

void guard_append(librados::ObjectWriteOperation *op, uint64_t soft_max_size);

} // namespace client
} // namespace journal
} // namespace cls

#endif // CEPH_CLS_JOURNAL_CLIENT_H
