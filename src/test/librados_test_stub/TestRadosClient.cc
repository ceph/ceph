// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librados_test_stub/TestRadosClient.h"
#include "test/librados_test_stub/TestIoCtxImpl.h"
#include "librados/AioCompletionImpl.h"
#include "include/assert.h"
#include "common/ceph_json.h"
#include "common/Finisher.h"
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <errno.h>

static int get_concurrency() {
  int concurrency = 0;
  char *env = getenv("LIBRADOS_CONCURRENCY");
  if (env != NULL) {
    concurrency = atoi(env);
  }
  if (concurrency == 0) {
    concurrency = boost::thread::thread::hardware_concurrency();
  }
  if (concurrency == 0) {
    concurrency = 1;
  }
  return concurrency;
}

namespace librados {

static void finish_aio_completion(AioCompletionImpl *c, int r) {
  c->lock.Lock();
  c->ack = true;
  c->safe = true;
  c->rval = r;
  c->lock.Unlock();

  rados_callback_t cb_complete = c->callback_complete;
  void *cb_complete_arg = c->callback_complete_arg;
  if (cb_complete) {
    cb_complete(c, cb_complete_arg);
  }

  rados_callback_t cb_safe = c->callback_safe;
  void *cb_safe_arg = c->callback_safe_arg;
  if (cb_safe) {
    cb_safe(c, cb_safe_arg);
  }

  c->lock.Lock();
  c->callback_complete = NULL;
  c->callback_safe = NULL;
  c->cond.Signal();
  c->put_unlock();
}

class AioFunctionContext : public Context {
public:
  AioFunctionContext(const TestRadosClient::AioFunction &callback,
                     Finisher *finisher, AioCompletionImpl *c)
    : m_callback(callback), m_finisher(finisher), m_comp(c)
  {
    if (m_comp != NULL) {
      m_comp->get();
    }
  }

  virtual void finish(int r) {
    int ret = m_callback();
    if (m_comp != NULL) {
      if (m_finisher != NULL) {
        m_finisher->queue(new FunctionContext(boost::bind(
          &finish_aio_completion, m_comp, ret)));
      } else {
        finish_aio_completion(m_comp, ret);
      }
    }
  }
private:
  TestRadosClient::AioFunction m_callback;
  Finisher *m_finisher;
  AioCompletionImpl *m_comp;
};

TestRadosClient::TestRadosClient(CephContext *cct)
  : m_cct(cct->get()),
    m_watch_notify(m_cct)
{
  get();

  // simulate multiple OSDs
  int concurrency = get_concurrency();
  for (int i = 0; i < concurrency; ++i) {
    m_finishers.push_back(new Finisher(m_cct));
    m_finishers.back()->start();
  }

  // replicate AIO callback processing
  m_aio_finisher = new Finisher(m_cct);
  m_aio_finisher->start();
}

TestRadosClient::~TestRadosClient() {
  flush_aio_operations();

  for (size_t i = 0; i < m_finishers.size(); ++i) {
    m_finishers[i]->stop();
    delete m_finishers[i];
  }
  m_aio_finisher->stop();
  delete m_aio_finisher;

  m_cct->put();
  m_cct = NULL;
}

void TestRadosClient::get() {
  m_refcount.inc();
}

void TestRadosClient::put() {
  if (m_refcount.dec() == 0) {
    shutdown();
    delete this;
  }
}

CephContext *TestRadosClient::cct() {
  return m_cct;
}

uint64_t TestRadosClient::get_instance_id() {
  return 0;
}

int TestRadosClient::connect() {
  return 0;
}

void TestRadosClient::shutdown() {
}

int TestRadosClient::wait_for_latest_osdmap() {
  return 0;
}

int TestRadosClient::mon_command(const std::vector<std::string>& cmd,
                                 const bufferlist &inbl,
                                 bufferlist *outbl, std::string *outs) {
  for (std::vector<std::string>::const_iterator it = cmd.begin();
       it != cmd.end(); ++it) {
    JSONParser parser;
    if (!parser.parse(it->c_str(), it->length())) {
      return -EINVAL;
    }

    JSONObjIter j_it = parser.find("prefix");
    if (j_it.end()) {
      return -EINVAL;
    }

    if ((*j_it)->get_data() == "osd tier add") {
      return 0;
    } else if ((*j_it)->get_data() == "osd tier cache-mode") {
      return 0;
    } else if ((*j_it)->get_data() == "osd tier set-overlay") {
      return 0;
    } else if ((*j_it)->get_data() == "osd tier remove-overlay") {
      return 0;
    } else if ((*j_it)->get_data() == "osd tier remove") {
      return 0;
    }
  }
  return -ENOSYS;
}

void TestRadosClient::add_aio_operation(const std::string& oid,
                                        bool queue_callback,
				        const AioFunction &aio_function,
                                        AioCompletionImpl *c) {
  AioFunctionContext *ctx = new AioFunctionContext(
    aio_function, queue_callback ? m_aio_finisher : NULL, c);
  get_finisher(oid)->queue(ctx);
}

struct WaitForFlush {
  int flushed() {
    if (count.dec() == 0) {
      if (c != NULL) {
	finish_aio_completion(c, 0);
      }
      delete this;
    }
    return 0;
  }

  atomic_t count;
  AioCompletionImpl *c;
};

void TestRadosClient::flush_aio_operations() {
  AioCompletionImpl *comp = new AioCompletionImpl();
  flush_aio_operations(comp);
  comp->wait_for_safe();
  comp->put();
}

void TestRadosClient::flush_aio_operations(AioCompletionImpl *c) {
  c->get();

  WaitForFlush *wait_for_flush = new WaitForFlush();
  wait_for_flush->count.set(m_finishers.size());
  wait_for_flush->c = c;

  for (size_t i = 0; i < m_finishers.size(); ++i) {
    AioFunctionContext *ctx = new AioFunctionContext(
      boost::bind(&WaitForFlush::flushed, wait_for_flush),
      m_aio_finisher, NULL);
    m_finishers[i]->queue(ctx);
  }
}

void TestRadosClient::finish_aio_completion(AioCompletionImpl *c, int r) {
  librados::finish_aio_completion(c, r);
}

Finisher *TestRadosClient::get_finisher(const std::string &oid) {
  std::size_t h = m_hash(oid);
  return m_finishers[h % m_finishers.size()];
}

} // namespace librados
