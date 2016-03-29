// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "bencher.h"
#include "include/utime.h"
#include <unistd.h>
#include "include/memory.h"
#include "common/Mutex.h"
#include "common/Cond.h"

template<typename T>
struct C_Holder : public Context {
  T obj;
  explicit C_Holder(
    T obj)
    : obj(obj) {}
  void finish(int r) {
    return;
  }
};

struct OnDelete {
  Context *c;
  explicit OnDelete(Context *c) : c(c) {}
  ~OnDelete() { c->complete(0); }
};

struct Cleanup : public Context {
  Bencher *bench;
  explicit Cleanup(Bencher *bench) : bench(bench) {}
  void finish(int r) {
    bench->complete_op();
  }
};

struct OnWriteApplied : public Context {
  Bencher *bench;
  uint64_t seq;
  ceph::shared_ptr<OnDelete> on_delete;
  OnWriteApplied(
    Bencher *bench, uint64_t seq,
    ceph::shared_ptr<OnDelete> on_delete
    ) : bench(bench), seq(seq), on_delete(on_delete) {}
  void finish(int r) {
    bench->stat_collector->write_applied(seq);
  }
};

struct OnWriteCommit : public Context {
  Bencher *bench;
  uint64_t seq;
  ceph::shared_ptr<OnDelete> on_delete;
  OnWriteCommit(
    Bencher *bench, uint64_t seq,
    ceph::shared_ptr<OnDelete> on_delete
    ) : bench(bench), seq(seq), on_delete(on_delete) {}
  void finish(int r) {
    bench->stat_collector->write_committed(seq);
  }
};

struct OnReadComplete : public Context {
  Bencher *bench;
  uint64_t seq;
  boost::scoped_ptr<bufferlist> bl;
  OnReadComplete(Bencher *bench, uint64_t seq, bufferlist *bl) :
    bench(bench), seq(seq), bl(bl) {}
  void finish(int r) {
    bench->stat_collector->read_complete(seq);
    bench->complete_op();
  }
};

void Bencher::start_op() {
  Mutex::Locker l(lock);
  while (open_ops >= max_in_flight)
    open_ops_cond.Wait(lock);
  ++open_ops;
}

void Bencher::drain_ops() {
  Mutex::Locker l(lock);
  while (open_ops)
    open_ops_cond.Wait(lock);
}

void Bencher::complete_op() {
  Mutex::Locker l(lock);
  assert(open_ops > 0);
  --open_ops;
  open_ops_cond.Signal();
}

struct OnFinish {
  bool *done;
  Mutex *lock;
  Cond *cond;
  OnFinish(
    bool *done,
    Mutex *lock,
    Cond *cond) :
    done(done), lock(lock), cond(cond) {}
  ~OnFinish() {
    Mutex::Locker l(*lock);
    *done = true;
    cond->Signal();
  }
};

void Bencher::init(
  const set<std::string> &objects,
  uint64_t size,
  std::ostream *out
  )
{
  bufferlist bl;
  for (uint64_t i = 0; i < size; ++i) {
    bl.append(0);
  }
  Mutex lock("init_lock");
  Cond cond;
  bool done = 0;
  {
    ceph::shared_ptr<OnFinish> on_finish(
      new OnFinish(&done, &lock, &cond));
    uint64_t num = 0;
    for (set<std::string>::const_iterator i = objects.begin();
	 i != objects.end();
	 ++i, ++num) {
      if (!(num % 20))
	*out << "Creating " << num << "/" << objects.size() << std::endl;
      backend->write(
	*i,
	0,
	bl,
	new C_Holder<ceph::shared_ptr<OnFinish> >(on_finish),
	new C_Holder<ceph::shared_ptr<OnFinish> >(on_finish)
	);
    }
  }
  {
    Mutex::Locker l(lock);
    while (!done)
      cond.Wait(lock);
  }
}

void Bencher::run_bench()
{
  time_t end = time(0) + max_duration;
  uint64_t ops = 0;

  bufferlist bl;

  while ((!max_duration || time(0) < end) && (!max_ops || ops < max_ops)) {
    start_op();
    uint64_t seq = stat_collector->next_seq();
    boost::tuple<std::string, uint64_t, uint64_t, OpType> next =
      (*op_dist)();
    string obj_name = next.get<0>();
    uint64_t offset = next.get<1>();
    uint64_t length = next.get<2>();
    OpType op_type = next.get<3>();
    switch (op_type) {
      case WRITE: {
	ceph::shared_ptr<OnDelete> on_delete(
	  new OnDelete(new Cleanup(this)));
	stat_collector->start_write(seq, length);
	while (bl.length() < length) {
	  bl.append(rand());
	}
	backend->write(
	  obj_name,
	  offset,
	  bl,
	  new OnWriteApplied(
	    this, seq, on_delete),
	  new OnWriteCommit(
	    this, seq, on_delete)
	  );
	break;
      }
      case READ: {
	stat_collector->start_read(seq, length);
	bufferlist *read_bl = new bufferlist;
	backend->read(
	  obj_name,
	  offset,
	  length,
	  read_bl,
	  new OnReadComplete(
	    this, seq, read_bl)
	  );
	break;
      }
      default: {
	assert(0);
      }
    } 
    ops++;
  }
  drain_ops();
}
