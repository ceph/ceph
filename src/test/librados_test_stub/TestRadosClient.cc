// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librados_test_stub/TestRadosClient.h"
#include "test/librados_test_stub/TestIoCtxImpl.h"
#include "librados/AioCompletionImpl.h"
#include "include/ceph_assert.h"
#include "common/ceph_json.h"
#include "common/Finisher.h"
#include "common/async/context_pool.h"
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>
#include <errno.h>

#include <atomic>
#include <functional>
#include <sstream>

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

using namespace std::placeholders;

namespace librados {

namespace {

const char *config_keys[] = {
  "librados_thread_count",
  NULL
};

} // anonymous namespace

static void finish_aio_completion(AioCompletionImpl *c, int r) {
  c->lock.lock();
  c->complete = true;
  c->rval = r;
  c->lock.unlock();

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

  c->lock.lock();
  c->callback_complete = NULL;
  c->callback_safe = NULL;
  c->cond.notify_all();
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

  void finish(int r) override {
    int ret = m_callback();
    if (m_comp != NULL) {
      if (m_finisher != NULL) {
        m_finisher->queue(new LambdaContext(std::bind(
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

TestRadosClient::TestRadosClient(CephContext *cct,
                                 TestWatchNotify *watch_notify)
  : m_cct(cct->get()), m_watch_notify(watch_notify),
    m_aio_finisher(new Finisher(m_cct)),
    m_io_context_pool(std::make_unique<ceph::async::io_context_pool>())
{
  get();

  // simulate multiple OSDs
  int concurrency = get_concurrency();
  for (int i = 0; i < concurrency; ++i) {
    m_finishers.push_back(new Finisher(m_cct));
    m_finishers.back()->start();
  }

  // replicate AIO callback processing
  m_aio_finisher->start();

  // replicate neorados callback processing
  m_cct->_conf.add_observer(this);
  m_io_context_pool->start(m_cct->_conf.get_val<uint64_t>(
    "librados_thread_count"));
}

TestRadosClient::~TestRadosClient() {
  flush_aio_operations();

  for (size_t i = 0; i < m_finishers.size(); ++i) {
    m_finishers[i]->stop();
    delete m_finishers[i];
  }
  m_aio_finisher->stop();
  delete m_aio_finisher;

  m_cct->_conf.remove_observer(this);
  m_io_context_pool->stop();

  m_cct->put();
  m_cct = NULL;
}

boost::asio::io_context& TestRadosClient::get_io_context() {
  return m_io_context_pool->get_io_context();
}

const char** TestRadosClient::get_tracked_conf_keys() const {
  return config_keys;
}

void TestRadosClient::handle_conf_change(
    const ConfigProxy& conf, const std::set<std::string> &changed) {
  if (changed.count("librados_thread_count")) {
    m_io_context_pool->stop();
    m_io_context_pool->start(conf.get_val<std::uint64_t>(
      "librados_thread_count"));
  }
}

void TestRadosClient::get() {
  m_refcount++;
}

void TestRadosClient::put() {
  if (--m_refcount == 0) {
    shutdown();
    delete this;
  }
}

CephContext *TestRadosClient::cct() {
  return m_cct;
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
    } else if ((*j_it)->get_data() == "config-key rm") {
      return 0;
    } else if ((*j_it)->get_data() == "config set") {
      return 0;
    } else if ((*j_it)->get_data() == "df") {
      std::stringstream str;
      str << R"({"pools": [)";

      std::list<std::pair<int64_t, std::string>> pools;
      pool_list(pools);
      for (auto& pool : pools) {
        if (pools.begin()->first != pool.first) {
          str << ",";
        }
        str << R"({"name": ")" << pool.second << R"(", "stats": )"
            << R"({"percent_used": 1.0, "bytes_used": 0, "max_avail": 0}})";
      }

      str << "]}";
      outbl->append(str.str());
      return 0;
    } else if ((*j_it)->get_data() == "osd blocklist") {
      auto op_it = parser.find("blocklistop");
      if (!op_it.end() && (*op_it)->get_data() == "add") {
        uint32_t expire = 0;
        auto expire_it = parser.find("expire");
        if (!expire_it.end()) {
          expire = boost::lexical_cast<uint32_t>((*expire_it)->get_data());
        }

        auto addr_it = parser.find("addr");
        return blocklist_add((*addr_it)->get_data(), expire);
      }
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
    if (--count == 0) {
      aio_finisher->queue(new LambdaContext(std::bind(
        &finish_aio_completion, c, 0)));
      delete this;
    }
    return 0;
  }

  std::atomic<int64_t> count = { 0 };
  Finisher *aio_finisher;
  AioCompletionImpl *c;
};

void TestRadosClient::flush_aio_operations() {
  AioCompletionImpl *comp = new AioCompletionImpl();
  flush_aio_operations(comp);
  comp->wait_for_complete();
  comp->put();
}

void TestRadosClient::flush_aio_operations(AioCompletionImpl *c) {
  c->get();

  WaitForFlush *wait_for_flush = new WaitForFlush();
  wait_for_flush->count = m_finishers.size();
  wait_for_flush->aio_finisher = m_aio_finisher;
  wait_for_flush->c = c;

  for (size_t i = 0; i < m_finishers.size(); ++i) {
    AioFunctionContext *ctx = new AioFunctionContext(
      std::bind(&WaitForFlush::flushed, wait_for_flush),
      nullptr, nullptr);
    m_finishers[i]->queue(ctx);
  }
}

int TestRadosClient::aio_watch_flush(AioCompletionImpl *c) {
  c->get();
  Context *ctx = new LambdaContext(std::bind(
    &TestRadosClient::finish_aio_completion, this, c, std::placeholders::_1));
  get_watch_notify()->aio_flush(this, ctx);
  return 0;
}

void TestRadosClient::finish_aio_completion(AioCompletionImpl *c, int r) {
  librados::finish_aio_completion(c, r);
}

Finisher *TestRadosClient::get_finisher(const std::string &oid) {
  std::size_t h = m_hash(oid);
  return m_finishers[h % m_finishers.size()];
}

} // namespace librados
