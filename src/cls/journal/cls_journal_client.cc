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

static const uint64_t JOURNAL_MAX_RETURN = 256;

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
           uint8_t splay) {
  bufferlist inbl;
  ::encode(order, inbl);
  ::encode(splay, inbl);

  bufferlist outbl;
  int r = ioctx.exec(oid, "journal", "create", inbl, outbl);
  if (r < 0) {
    return r;
  }
  return 0;
}

int get_immutable_metadata(librados::IoCtx &ioctx, const std::string &oid,
                           uint8_t *order, uint8_t *splay_width) {
  librados::ObjectReadOperation op;
  bufferlist inbl;
  op.exec("journal", "get_order", inbl);
  op.exec("journal", "get_splay_width", inbl);

  bufferlist outbl;
  int r = ioctx.operate(oid, &op, &outbl);
  if (r < 0) {
    return r;
  }

  try {
    bufferlist::iterator iter = outbl.begin();
    ::decode(*order, iter);
    ::decode(*splay_width, iter);
  } catch (const buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
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

void client_register(librados::ObjectWriteOperation *op,
                     const std::string &id, const std::string &description) {
  bufferlist bl;
  ::encode(id, bl);
  ::encode(description, bl);
  op->exec("journal", "client_register", bl);
}

int client_register(librados::IoCtx &ioctx, const std::string &oid,
                    const std::string &id, const std::string &description) {
  librados::ObjectWriteOperation op;
  client_register(&op, id, description);
  return ioctx.operate(oid, &op);
}

int client_unregister(librados::IoCtx &ioctx, const std::string &oid,
                       const std::string &id) {
  bufferlist inbl;
  ::encode(id, inbl);

  bufferlist outbl;
  return ioctx.exec(oid, "journal", "client_unregister", inbl, outbl);
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

void guard_append(librados::ObjectWriteOperation *op, uint64_t soft_max_size) {
  bufferlist bl;
  ::encode(soft_max_size, bl);
  op->exec("journal", "guard_append", bl);
}

} // namespace client
} // namespace journal
} // namespace cls
