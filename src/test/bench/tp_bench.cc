// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options/option.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/parsers.hpp>
#include <iostream>
#include <set>
#include <sstream>
#include <stdlib.h>
#include <fstream>

#include "common/Formatter.h"

#include "bencher.h"
#include "rados_backend.h"
#include "detailed_stat_collector.h"
#include "distribution.h"
#include "global/global_init.h"
#include "common/WorkQueue.h"
#include "common/Semaphore.h"
#include "common/Finisher.h"

namespace po = boost::program_options;
using namespace std;
class Queueable {
public:
  virtual void queue(unsigned *) = 0;
  virtual void start() = 0;
  virtual void stop() = 0;
  virtual ~Queueable() {};
};
class Base : public Queueable {
  DetailedStatCollector *col;
  Semaphore *sem;
public:
  Base(DetailedStatCollector *col,
       Semaphore *sem) : col(col), sem(sem) {}
  void queue(unsigned *item) {
    col->read_complete(*item);
    sem->Put();
    delete item;
  }
  void start() {}
  void stop() {}
};
class WQWrapper : public Queueable {
  boost::scoped_ptr<ThreadPool::WorkQueue<unsigned> > wq;
  boost::scoped_ptr<ThreadPool> tp;
public:
  WQWrapper(ThreadPool::WorkQueue<unsigned> *wq, ThreadPool *tp):
    wq(wq), tp(tp) {}
  void queue(unsigned *item) { wq->queue(item); }
  void start() { tp->start(); }
  void stop() { tp->stop(); }
};
class FinisherWrapper : public Queueable {
  class CB : public Context {
    Queueable *next;
    unsigned *item;
  public:
    CB(Queueable *next, unsigned *item) : next(next), item(item) {}
    void finish(int) {
      next->queue(item);
    }
  };
  Finisher f;
  Queueable *next;
public:
  FinisherWrapper(CephContext *cct, Queueable *next) :
    f(cct), next(next) {}
  void queue(unsigned *item) {
    f.queue(new CB(next, item));
  }
  void start() { f.start(); }
  void stop() { f.stop(); }
};
class PassAlong : public ThreadPool::WorkQueue<unsigned> {
  Queueable *next;
  list<unsigned*> q;
  bool _enqueue(unsigned *item) {
    q.push_back(item);
    return true;
  }
  void _dequeue(unsigned *item) { assert(0); }
  unsigned *_dequeue() {
    if (q.empty())
      return 0;
    unsigned *val = q.front();
    q.pop_front();
    return val;
  }
  void _process(unsigned *item, ThreadPool::TPHandle &) override {
    next->queue(item);
  }
  void _clear() { q.clear(); }
  bool _empty() { return q.empty(); }
public:
  PassAlong(ThreadPool *tp, Queueable *_next) :
    ThreadPool::WorkQueue<unsigned>("TestQueue", 100, 100, tp), next(_next) {}
};

int main(int argc, char **argv)
{
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "produce help message")
    ("num-threads", po::value<unsigned>()->default_value(10),
     "set number of threads")
    ("queue-size", po::value<unsigned>()->default_value(30),
     "queue size")
    ("num-items", po::value<unsigned>()->default_value(3000000),
     "num items")
    ("layers", po::value<string>()->default_value(""),
     "layer desc")
    ;

  vector<string> ceph_option_strings;
  po::variables_map vm;
  try {
    po::parsed_options parsed =
      po::command_line_parser(argc, argv).options(desc).allow_unregistered().run();
    po::store(
	      parsed,
	      vm);
    po::notify(vm);

    ceph_option_strings = po::collect_unrecognized(parsed.options,
						   po::include_positional);
  } catch(po::error &e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }
  vector<const char *> ceph_options, def_args;
  ceph_options.reserve(ceph_option_strings.size());
  for (vector<string>::iterator i = ceph_option_strings.begin();
       i != ceph_option_strings.end();
       ++i) {
    ceph_options.push_back(i->c_str());
  }

  auto cct = global_init(
    &def_args, ceph_options, CEPH_ENTITY_TYPE_CLIENT,
    CODE_ENVIRONMENT_UTILITY,
    CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  if (vm.count("help")) {
    cout << desc << std::endl;
    return 1;
  }

  DetailedStatCollector col(1, new JSONFormatter, 0, &cout);
  Semaphore sem;
  for (unsigned i = 0; i < vm["queue-size"].as<unsigned>(); ++i)
    sem.Put();

  typedef list<Queueable*> QQ;
  QQ wqs;
  wqs.push_back(
    new Base(&col, &sem));
  string layers(vm["layers"].as<string>());
  unsigned num = 0;
  for (string::reverse_iterator i = layers.rbegin();
       i != layers.rend(); ++i) {
    stringstream ss;
    ss << "Test " << num;
    if (*i == 'q') {
      ThreadPool *tp =
	new ThreadPool(
	  g_ceph_context, ss.str(), "tp_test",  vm["num-threads"].as<unsigned>(), 0);
      wqs.push_back(
	new WQWrapper(
	  new PassAlong(tp, wqs.back()),
	  tp
	  ));
    } else if (*i == 'f') {
      wqs.push_back(
	new FinisherWrapper(
	  g_ceph_context, wqs.back()));
    }
    ++num;
  }

  for (QQ::iterator i = wqs.begin();
       i != wqs.end();
       ++i) {
    (*i)->start();
  }

  for (uint64_t i = 0; i < vm["num-items"].as<unsigned>(); ++i) {
    sem.Get();
    unsigned *item = new unsigned(col.next_seq());
    col.start_read(*item, 1);
    wqs.back()->queue(item);
  }

  for (QQ::iterator i = wqs.begin();
       i != wqs.end();
       ++i) {
    (*i)->stop();
  }
  for (QQ::iterator i = wqs.begin(); i != wqs.end(); wqs.erase(i++)) {
    delete *i;
  }
  return 0;
}
