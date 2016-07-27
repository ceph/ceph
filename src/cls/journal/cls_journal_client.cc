// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/journal/cls_journal_client.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "common/Cond.h"
#include <errno.h>
#include <map>

namespace cls {
namespace journal {
namespace client {

namespace {

struct C_AioExec : public Context {
  librados::IoCtx ioctx;
  std::string oid;

  C_AioExec(librados::IoCtx &_ioctx, const std::string &_oid) : oid(_oid) {
    ioctx.dup(_ioctx);
  }

  static void rados_callback(rados_completion_t c, void *arg) {
    Context *ctx = reinterpret_cast<Context *>(arg);
    ctx->complete(rados_aio_get_return_value(c));
  }
};

struct C_ClientList : public C_AioExec {
  std::set<cls::journal::Client> *clients;
  Context *on_finish;
  bufferlist outbl;

  C_ClientList(librados::IoCtx &_ioctx, const std::string &_oid,
               std::set<cls::journal::Client> *_clients,
               Context *_on_finish)
    : C_AioExec(_ioctx, _oid), clients(_clients), on_finish(_on_finish) {}

  void send(const std::string &start_after) {
    bufferlist inbl;
    ::encode(start_after, inbl);
    ::encode(JOURNAL_MAX_RETURN, inbl);

    librados::ObjectReadOperation op;
    op.exec("journal", "client_list", inbl);

    outbl.clear();
    librados::AioCompletion *rados_completion =
       librados::Rados::aio_create_completion(this, rados_callback, NULL);
    int r = ioctx.aio_operate(oid, rados_completion, &op, &outbl);
    assert(r == 0);
    rados_completion->release();
  }

  virtual void complete(int r) {
    if (r < 0) {
      finish(r);
      return;
    }

    try {
      bufferlist::iterator iter = outbl.begin();
      std::set<cls::journal::Client> partial_clients;
      ::decode(partial_clients, iter);

      std::string start_after;
      if (!partial_clients.empty()) {
        start_after = partial_clients.rbegin()->id;
        clients->insert(partial_clients.begin(), partial_clients.end());
      }

      if (partial_clients.size() < JOURNAL_MAX_RETURN) {
        finish(0);
      } else {
        send(start_after);
      }
    } catch (const buffer::error &err) {
      finish(-EBADMSG);
    }
  }

  virtual void finish(int r) {
    on_finish->complete(r);
    delete this;
  }
};

struct C_ImmutableMetadata : public C_AioExec {
  uint8_t *order;
  uint8_t *splay_width;
  int64_t *pool_id;
  Context *on_finish;
  bufferlist outbl;

  C_ImmutableMetadata(librados::IoCtx &_ioctx, const std::string &_oid,
                      uint8_t *_order, uint8_t *_splay_width,
		      int64_t *_pool_id, Context *_on_finish)
    : C_AioExec(_ioctx, _oid), order(_order), splay_width(_splay_width),
      pool_id(_pool_id), on_finish(_on_finish) {
  }

  void send() {
    librados::ObjectReadOperation op;
    bufferlist inbl;
    op.exec("journal", "get_order", inbl);
    op.exec("journal", "get_splay_width", inbl);
    op.exec("journal", "get_pool_id", inbl);

    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(this, rados_callback, NULL);
    int r = ioctx.aio_operate(oid, rados_completion, &op, &outbl);
    assert(r == 0);
    rados_completion->release();
  }

  virtual void finish(int r) {
    if (r == 0) {
      try {
        bufferlist::iterator iter = outbl.begin();
        ::decode(*order, iter);
        ::decode(*splay_width, iter);
        ::decode(*pool_id, iter);
      } catch (const buffer::error &err) {
        r = -EBADMSG;
      }
    }
    on_finish->complete(r);
  }
};

struct C_MutableMetadata : public C_AioExec {
  uint64_t *minimum_set;
  uint64_t *active_set;
  C_ClientList *client_list;
  bufferlist outbl;

  C_MutableMetadata(librados::IoCtx &_ioctx, const std::string &_oid,
                    uint64_t *_minimum_set, uint64_t *_active_set,
                    C_ClientList *_client_list)
    : C_AioExec(_ioctx, _oid), minimum_set(_minimum_set),
      active_set(_active_set), client_list(_client_list) {}

  void send() {
    librados::ObjectReadOperation op;
    bufferlist inbl;
    op.exec("journal", "get_minimum_set", inbl);
    op.exec("journal", "get_active_set", inbl);

    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(this, rados_callback, NULL);
    int r = ioctx.aio_operate(oid, rados_completion, &op, &outbl);
    assert(r == 0);
    rados_completion->release();
  }

  virtual void finish(int r) {
    if (r == 0) {
      try {
        bufferlist::iterator iter = outbl.begin();
        ::decode(*minimum_set, iter);
        ::decode(*active_set, iter);
        client_list->send("");
      } catch (const buffer::error &err) {
        r = -EBADMSG;
      }
    }
    if (r < 0) {
      client_list->complete(r);
    }
  }
};


} // anonymous namespace

int create(librados::IoCtx &ioctx, const std::string &oid, uint8_t order,
           uint8_t splay, int64_t pool_id) {
  bufferlist inbl;
  ::encode(order, inbl);
  ::encode(splay, inbl);
  ::encode(pool_id, inbl);

  bufferlist outbl;
  int r = ioctx.exec(oid, "journal", "create", inbl, outbl);
  if (r < 0) {
    return r;
  }
  return 0;
}

void get_immutable_metadata(librados::IoCtx &ioctx, const std::string &oid,
                            uint8_t *order, uint8_t *splay_width,
                            int64_t *pool_id, Context *on_finish) {
  C_ImmutableMetadata *metadata = new C_ImmutableMetadata(ioctx, oid, order,
                                                          splay_width, pool_id,
                                                          on_finish);
  metadata->send();
}

void get_mutable_metadata(librados::IoCtx &ioctx, const std::string &oid,
                          uint64_t *minimum_set, uint64_t *active_set,
                          std::set<cls::journal::Client> *clients,
                          Context *on_finish) {
  C_ClientList *client_list = new C_ClientList(ioctx, oid, clients, on_finish);
  C_MutableMetadata *metadata = new C_MutableMetadata(
    ioctx, oid, minimum_set, active_set, client_list);
  metadata->send();
}

void set_minimum_set(librados::ObjectWriteOperation *op, uint64_t object_set) {
  bufferlist bl;
  ::encode(object_set, bl);
  op->exec("journal", "set_minimum_set", bl);
}

void set_active_set(librados::ObjectWriteOperation *op, uint64_t object_set) {
  bufferlist bl;
  ::encode(object_set, bl);
  op->exec("journal", "set_active_set", bl);
}

int get_client(librados::IoCtx &ioctx, const std::string &oid,
               const std::string &id, cls::journal::Client *client) {
  librados::ObjectReadOperation op;
  get_client_start(&op, id);

  bufferlist out_bl;
  int r = ioctx.operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  bufferlist::iterator iter = out_bl.begin();
  r = get_client_finish(&iter, client);
  if (r < 0) {
    return r;
  }
  return 0;
}

void get_client_start(librados::ObjectReadOperation *op,
                      const std::string &id) {
  bufferlist bl;
  ::encode(id, bl);
  op->exec("journal", "get_client", bl);
}

int get_client_finish(bufferlist::iterator *iter,
                      cls::journal::Client *client) {
  try {
    ::decode(*client, *iter);
  } catch (const buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int client_register(librados::IoCtx &ioctx, const std::string &oid,
                    const std::string &id, const bufferlist &data) {
  librados::ObjectWriteOperation op;
  client_register(&op, id, data);
  return ioctx.operate(oid, &op);
}

void client_register(librados::ObjectWriteOperation *op,
                     const std::string &id, const bufferlist &data) {
  bufferlist bl;
  ::encode(id, bl);
  ::encode(data, bl);
  op->exec("journal", "client_register", bl);
}

int client_update_data(librados::IoCtx &ioctx, const std::string &oid,
                       const std::string &id, const bufferlist &data) {
  librados::ObjectWriteOperation op;
  client_update_data(&op, id, data);
  return ioctx.operate(oid, &op);
}

void client_update_data(librados::ObjectWriteOperation *op,
                        const std::string &id, const bufferlist &data) {
  bufferlist bl;
  ::encode(id, bl);
  ::encode(data, bl);
  op->exec("journal", "client_update_data", bl);
}

int client_update_state(librados::IoCtx &ioctx, const std::string &oid,
                        const std::string &id, cls::journal::ClientState state) {
  librados::ObjectWriteOperation op;
  client_update_state(&op, id, state);
  return ioctx.operate(oid, &op);
}

void client_update_state(librados::ObjectWriteOperation *op,
                         const std::string &id,
                         cls::journal::ClientState state) {
  bufferlist bl;
  ::encode(id, bl);
  ::encode(static_cast<uint8_t>(state), bl);
  op->exec("journal", "client_update_state", bl);
}

int client_unregister(librados::IoCtx &ioctx, const std::string &oid,
                       const std::string &id) {
  librados::ObjectWriteOperation op;
  client_unregister(&op, id);
  return ioctx.operate(oid, &op);
}

void client_unregister(librados::ObjectWriteOperation *op,
		       const std::string &id) {

  bufferlist bl;
  ::encode(id, bl);
  op->exec("journal", "client_unregister", bl);
}

void client_commit(librados::ObjectWriteOperation *op, const std::string &id,
                   const cls::journal::ObjectSetPosition &commit_position) {
  bufferlist bl;
  ::encode(id, bl);
  ::encode(commit_position, bl);
  op->exec("journal", "client_commit", bl);
}

int client_list(librados::IoCtx &ioctx, const std::string &oid,
                std::set<cls::journal::Client> *clients) {
  C_SaferCond cond;
  C_ClientList *client_list = new C_ClientList(ioctx, oid, clients, &cond);
  client_list->send("");
  return cond.wait();
}

int get_next_tag_tid(librados::IoCtx &ioctx, const std::string &oid,
                     uint64_t *tag_tid) {
  librados::ObjectReadOperation op;
  get_next_tag_tid_start(&op);

  bufferlist out_bl;
  int r = ioctx.operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  bufferlist::iterator iter = out_bl.begin();
  r = get_next_tag_tid_finish(&iter, tag_tid);
  if (r < 0) {
    return r;
  }
  return 0;
}

void get_next_tag_tid_start(librados::ObjectReadOperation *op) {
  bufferlist bl;
  op->exec("journal", "get_next_tag_tid", bl);
}

int get_next_tag_tid_finish(bufferlist::iterator *iter,
                            uint64_t *tag_tid) {
  try {
    ::decode(*tag_tid, *iter);
  } catch (const buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int get_tag(librados::IoCtx &ioctx, const std::string &oid,
            uint64_t tag_tid, cls::journal::Tag *tag) {
  librados::ObjectReadOperation op;
  get_tag_start(&op, tag_tid);

  bufferlist out_bl;
  int r = ioctx.operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  bufferlist::iterator iter = out_bl.begin();
  r = get_tag_finish(&iter, tag);
  if (r < 0) {
    return r;
  }
  return 0;
}

void get_tag_start(librados::ObjectReadOperation *op,
                   uint64_t tag_tid) {
  bufferlist bl;
  ::encode(tag_tid, bl);
  op->exec("journal", "get_tag", bl);
}

int get_tag_finish(bufferlist::iterator *iter, cls::journal::Tag *tag) {
  try {
    ::decode(*tag, *iter);
  } catch (const buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int tag_create(librados::IoCtx &ioctx, const std::string &oid,
               uint64_t tag_tid, uint64_t tag_class,
               const bufferlist &data) {
  librados::ObjectWriteOperation op;
  tag_create(&op, tag_tid, tag_class, data);
  return ioctx.operate(oid, &op);
}

void tag_create(librados::ObjectWriteOperation *op, uint64_t tag_tid,
                uint64_t tag_class, const bufferlist &data) {
  bufferlist bl;
  ::encode(tag_tid, bl);
  ::encode(tag_class, bl);
  ::encode(data, bl);
  op->exec("journal", "tag_create", bl);
}

int tag_list(librados::IoCtx &ioctx, const std::string &oid,
             const std::string &client_id, boost::optional<uint64_t> tag_class,
             std::set<cls::journal::Tag> *tags) {
  tags->clear();
  uint64_t start_after_tag_tid = 0;
  while (true) {
    librados::ObjectReadOperation op;
    tag_list_start(&op, start_after_tag_tid, JOURNAL_MAX_RETURN, client_id,
                   tag_class);

    bufferlist out_bl;
    int r = ioctx.operate(oid, &op, &out_bl);
    if (r < 0) {
      return r;
    }

    bufferlist::iterator iter = out_bl.begin();
    std::set<cls::journal::Tag> decode_tags;
    r = tag_list_finish(&iter, &decode_tags);
    if (r < 0) {
      return r;
    }

    tags->insert(decode_tags.begin(), decode_tags.end());
    if (decode_tags.size() < JOURNAL_MAX_RETURN) {
      break;
    }
  }
  return 0;
}

void tag_list_start(librados::ObjectReadOperation *op,
                    uint64_t start_after_tag_tid, uint64_t max_return,
                    const std::string &client_id,
                    boost::optional<uint64_t> tag_class) {
  bufferlist bl;
  ::encode(start_after_tag_tid, bl);
  ::encode(max_return, bl);
  ::encode(client_id, bl);
  ::encode(tag_class, bl);
  op->exec("journal", "tag_list", bl);
}

int tag_list_finish(bufferlist::iterator *iter,
                    std::set<cls::journal::Tag> *tags) {
  try {
    ::decode(*tags, *iter);
  } catch (const buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

void guard_append(librados::ObjectWriteOperation *op, uint64_t soft_max_size) {
  bufferlist bl;
  ::encode(soft_max_size, bl);
  op->exec("journal", "guard_append", bl);
}

} // namespace client
} // namespace journal
} // namespace cls
