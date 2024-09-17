// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "Model.h"

#include "DataGenerator.h"

RadosIo::RadosIo(librados::Rados& rados,
        boost::asio::io_context& asio,
        const std::string pool,
        const std::string oid,
        uint64_t block_size,
        int seed,
	int threads,
        ceph::mutex& lock,
        ceph::condition_variable& cond) :
  IoExerciser(oid, block_size),
  rados(rados),
  asio(asio),
  om(std::make_unique<ObjectModel>(oid, block_size, seed)),
  db(Ceph::DataGenerator::DataGenerator::create_generator(
    block_size < 8 ?
    Ceph::DataGenerator::GenerationType::SeededRandom :
    Ceph::DataGenerator::GenerationType::HeaderedSeededRandom, *om)),
  pool(pool),
  threads(threads),
  lock(lock),
  cond(cond),
  outstanding_io(0)
{
  int rc;
  rc = rados.ioctx_create(pool.c_str(), io);
  ceph_assert(rc == 0);
  allow_ec_overwrites(true);
}

RadosIo::~RadosIo()
{
}

void RadosIo::start_io()
{
  std::lock_guard l(lock);
  outstanding_io++;
}

void RadosIo::finish_io()
{
  std::lock_guard l(lock);
  ceph_assert(outstanding_io > 0);
  outstanding_io--;
  cond.notify_all();
}

void RadosIo::wait_for_io(int count)
{
  std::unique_lock l(lock);
  while (outstanding_io > count) {
    cond.wait(l);
  }
}

void RadosIo::allow_ec_overwrites(bool allow)
{
  int rc;
  bufferlist inbl, outbl;
  std::string cmdstr =
    "{\"prefix\": \"osd pool set\", \"pool\": \"" + pool + "\", \
      \"var\": \"allow_ec_overwrites\", \"val\": \"" +
    (allow ? "true" : "false") + "\"}";
  rc = rados.mon_command(cmdstr, inbl, &outbl, nullptr);
  ceph_assert(rc == 0);
}

RadosIo::AsyncOp::AsyncOp(RadosIo *r,
        uint64_t offset1, uint64_t length1,
        uint64_t offset2, uint64_t length2,
        uint64_t offset3, uint64_t length3 ) :
  r(r),
  offset1(offset1), length1(length1),
  offset2(offset2), length2(length2),
  offset3(offset3), length3(length3)
{
  r->start_io();
}

RadosIo::AsyncOp::~AsyncOp() {
  r->finish_io();
}

bool RadosIo::readyForIoOp(IoOp &op)
{
#ifdef CEPH_DEBUG_MUTEX
  ceph_assert(lock.is_locked_by_me()); //Must be called with lock held
#endif
  if (!om->readyForIoOp(op)) {
    return false;
  }
  switch (op.op) {
  case IO_OP_DONE:
  case IO_OP_BARRIER:
    return outstanding_io == 0;
  default:
    return outstanding_io < threads;
  }
}

void RadosIo::applyIoOp(IoOp &op)
{
  AsyncOp *aop;
  
  om->applyIoOp(op);

  // If there are thread concurrent I/Os in flight then wait for
  // at least one I/O to complete
  wait_for_io(threads-1);
  
  switch (op.op) {
  case IO_OP_DONE:
  case IO_OP_BARRIER:
    // Wait for all outstanding I/O to complete
    wait_for_io(0);
    break;

  case IO_OP_CREATE:
    {
      aop = new AsyncOp(this);
      db->generate_data(0, op.length1, aop->bl1);
      aop->wop.write_full(aop->bl1);
      auto create_cb = [aop] (boost::system::error_code ec) {
        ceph_assert(ec == boost::system::errc::success);
        delete aop;
      };
      librados::async_operate(asio, io, oid,
                              &aop->wop, 0, nullptr, create_cb);
    }
    break;

  case IO_OP_REMOVE:
    {
      aop = new AsyncOp(this);
      aop->wop.remove();
      auto remove_cb = [aop] (boost::system::error_code ec) {
        ceph_assert(ec == boost::system::errc::success);
        delete aop;
      };
      librados::async_operate(asio, io, oid,
                              &aop->wop, 0, nullptr, remove_cb);
    }
    break;

  case IO_OP_READ:
    {
      aop = new AsyncOp(this, op.offset1, op.length1);
      aop->rop.read(op.offset1 * block_size,
                    op.length1 * block_size,
                    &aop->bl1, nullptr);
      auto read_cb = [aop] (boost::system::error_code ec, bufferlist bl) {
        ceph_assert(ec == boost::system::errc::success);
        aop->r->db->validate(aop->bl1, aop->offset1, aop->length1);
        delete aop;
      };
      librados::async_operate(asio, io, oid,
                              &aop->rop, 0, nullptr, read_cb);
      num_io++;
    }
    break;

  case IO_OP_READ2:
    {
      aop = new AsyncOp(this, op.offset1, op.length1, op.offset2, op.length2);
      aop->rop.read(op.offset1 * block_size,
                    op.length1 * block_size,
                    &aop->bl1, nullptr);
      aop->rop.read(op.offset2 * block_size,
                    op.length2 * block_size,
                    &aop->bl2, nullptr);
      auto read2_cb = [aop] (boost::system::error_code ec, bufferlist bl) {
        ceph_assert(ec == boost::system::errc::success);
        aop->r->db->validate(aop->bl1, aop->offset1, aop->length1);
        aop->r->db->validate(aop->bl2, aop->offset2, aop->length2);
        delete aop;
      };
      librados::async_operate(asio, io, oid,
                              &aop->rop, 0, nullptr, read2_cb);
      num_io++;
    }
    break;

  case IO_OP_READ3:
    {
      aop = new AsyncOp(this, op.offset1, op.length1, op.offset2, op.length2,
			op.offset3, op.length3);
      aop->rop.read(op.offset1 * block_size,
                    op.length1 * block_size,
                    &aop->bl1, nullptr);
      aop->rop.read(op.offset2 * block_size,
                    op.length2 * block_size,
                    &aop->bl2, nullptr);
      aop->rop.read(op.offset3 * block_size,
                    op.length3 * block_size,
                    &aop->bl3, nullptr);
      auto read3_cb = [aop] (boost::system::error_code ec, bufferlist bl) {
        ceph_assert(ec == boost::system::errc::success);
        aop->r->db->validate(aop->bl1, aop->offset1, aop->length1);
        aop->r->db->validate(aop->bl2, aop->offset2, aop->length2);
        aop->r->db->validate(aop->bl3, aop->offset3, aop->length3);
        delete aop;
      };
      librados::async_operate(asio, io, oid,
                              &aop->rop, 0, nullptr, read3_cb);
      num_io++;
    }
    break;

  case IO_OP_WRITE:
    {
      aop = new AsyncOp(this, op.offset1, op.length1);
      db->generate_data(op.offset1, op.length1, aop->bl1);
      aop->wop.write(op.offset1 * block_size, aop->bl1);
      auto write_cb = [aop] (boost::system::error_code ec) {
        ceph_assert(ec == boost::system::errc::success);
        delete aop;
      };
      librados::async_operate(asio, io, oid,
                              &aop->wop, 0, nullptr, write_cb);
      num_io++;
    }
    break;

  case IO_OP_WRITE2:
    {
      aop = new AsyncOp(this, op.offset1, op.length1, op.offset2, op.length2);
      db->generate_data(op.offset1, op.length1, aop->bl1);
      db->generate_data(op.offset2, op.length2, aop->bl2);
      aop->wop.write(op.offset1 * block_size, aop->bl1);
      aop->wop.write(op.offset2 * block_size, aop->bl2);
      auto write2_cb = [aop] (boost::system::error_code ec) {
        ceph_assert(ec == boost::system::errc::success);
        delete aop;
      };
      librados::async_operate(asio, io, oid,
                              &aop->wop, 0, nullptr, write2_cb);
      num_io++;
    }
    break;

  case IO_OP_WRITE3:
    {
      aop = new AsyncOp(this, op.offset1, op.length1, op.offset2, op.length2,
			op.offset3, op.length3);
      db->generate_data(op.offset1, op.length1, aop->bl1);
      db->generate_data(op.offset2, op.length2, aop->bl2);
      db->generate_data(op.offset3, op.length3, aop->bl3);
      aop->wop.write(op.offset1 * block_size, aop->bl1);
      aop->wop.write(op.offset2 * block_size, aop->bl2);
      aop->wop.write(op.offset3 * block_size, aop->bl3);
      auto write3_cb = [aop] (boost::system::error_code ec) {
        ceph_assert(ec == boost::system::errc::success);
        delete aop;
      };
      librados::async_operate(asio, io, oid,
                              &aop->wop, 0, nullptr, write3_cb);
      num_io++;
    }
    break;

  default:
    break;
  }
}
