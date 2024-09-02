#include "RadosIo.h"

#include "DataGenerator.h"

ceph::io_exerciser::RadosIo::RadosIo(librados::Rados& rados,
        boost::asio::io_context& asio,
        const std::string pool,
        const std::string oid,
        uint64_t block_size,
        int seed,
	int threads,
        ceph::mutex& lock,
        ceph::condition_variable& cond) :
  Model(oid, block_size),
  rados(rados),
  asio(asio),
  om(std::make_unique<ObjectModel>(oid, block_size, seed)),
  db(data_generation::DataGenerator::create_generator(
      data_generation::GenerationType::HeaderedSeededRandom, *om)),
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

ceph::io_exerciser::RadosIo::~RadosIo()
{
}

void ceph::io_exerciser::RadosIo::start_io()
{
  std::lock_guard l(lock);
  outstanding_io++;
}

void ceph::io_exerciser::RadosIo::finish_io()
{
  std::lock_guard l(lock);
  ceph_assert(outstanding_io > 0);
  outstanding_io--;
  cond.notify_all();
}

void ceph::io_exerciser::RadosIo::wait_for_io(int count)
{
  std::unique_lock l(lock);
  while (outstanding_io > count) {
    cond.wait(l);
  }
}

void ceph::io_exerciser::RadosIo::allow_ec_overwrites(bool allow)
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

ceph::io_exerciser::RadosIo::AsyncOpInfo::AsyncOpInfo(
        uint64_t offset1, uint64_t length1,
        uint64_t offset2, uint64_t length2,
        uint64_t offset3, uint64_t length3 ) :
  offset1(offset1), length1(length1),
  offset2(offset2), length2(length2),
  offset3(offset3), length3(length3)
{

}

bool ceph::io_exerciser::RadosIo::readyForIoOp(IoOp &op)
{
  ceph_assert(lock.is_locked_by_me()); //Must be called with lock held
  if (!om->readyForIoOp(op)) {
    return false;
  }
  switch (op.op) {
  case Done:
  case BARRIER:
    return outstanding_io == 0;
  default:
    return outstanding_io < threads;
  }
}

void ceph::io_exerciser::RadosIo::applyIoOp(IoOp &op)
{
  std::shared_ptr<AsyncOpInfo> op_info;

  om->applyIoOp(op);

  // If there are thread concurrent I/Os in flight then wait for
  // at least one I/O to complete
  wait_for_io(threads-1);
  
  switch (op.op) {
  case Done:
  [[ fallthrough ]];
  case BARRIER:
    // Wait for all outstanding I/O to complete
    wait_for_io(0);
    break;    

  case CREATE:
    {
      start_io();
      op_info = std::make_shared<AsyncOpInfo>(0, op.length1);
      db->generate_data(0, op.length1, op_info->bl1);
      op_info->wop.write_full(op_info->bl1);
      auto create_cb = [this] (boost::system::error_code ec) {
        ceph_assert(ec == boost::system::errc::success);
        finish_io();
      };
      librados::async_operate(asio, io, oid,
                              &op_info->wop, 0, nullptr, create_cb);
    }
    break;

  case REMOVE:
    {
      start_io();
      op_info = std::make_shared<AsyncOpInfo>();
      op_info->wop.remove();
      auto remove_cb = [this] (boost::system::error_code ec) {
        ceph_assert(ec == boost::system::errc::success);
        finish_io();
      };
      librados::async_operate(asio, io, oid,
                              &op_info->wop, 0, nullptr, remove_cb);
    }
    break;

  case READ:
    {
      start_io();
      op_info = std::make_shared<AsyncOpInfo>(op.offset1, op.length1);
      op_info->rop.read(op.offset1 * block_size,
                        op.length1 * block_size,
                        &op_info->bl1, nullptr);
      auto read_cb = [this, op_info] (boost::system::error_code ec, bufferlist bl) {
        ceph_assert(ec == boost::system::errc::success);
        db->validate(op_info->bl1, op_info->offset1, op_info->length1);
        finish_io();
      };
      librados::async_operate(asio, io, oid,
                              &op_info->rop, 0, nullptr, read_cb);
      num_io++;
    }
    break;

  case READ2:
    {
      start_io();
      op_info = std::make_shared<AsyncOpInfo>(op.offset1,
                                              op.length1,
                                              op.offset2,
                                              op.length2);

      op_info->rop.read(op.offset1 * block_size,
                        op.length1 * block_size,
                        &op_info->bl1, nullptr);
      op_info->rop.read(op.offset2 * block_size,
                    op.length2 * block_size,
                    &op_info->bl2, nullptr);
      auto read2_cb = [this, op_info] (boost::system::error_code ec,
                                       bufferlist bl) {
        ceph_assert(ec == boost::system::errc::success);
        db->validate(op_info->bl1, op_info->offset1, op_info->length1);
        db->validate(op_info->bl2, op_info->offset2, op_info->length2);
        finish_io();
      };
      librados::async_operate(asio, io, oid,
                              &op_info->rop, 0, nullptr, read2_cb);
      num_io++;
    }
    break;

  case READ3:
    {
      start_io();
      op_info = std::make_shared<AsyncOpInfo>(op.offset1, op.length1,
                                              op.offset2, op.length2,
                                              op.offset3, op.length3);
      op_info->rop.read(op.offset1 * block_size,
                    op.length1 * block_size,
                    &op_info->bl1, nullptr);
      op_info->rop.read(op.offset2 * block_size,
                    op.length2 * block_size,
                    &op_info->bl2, nullptr);
      op_info->rop.read(op.offset3 * block_size,
                    op.length3 * block_size,
                    &op_info->bl3, nullptr);
      auto read3_cb = [this, op_info] (boost::system::error_code ec,
                                       bufferlist bl) {
        ceph_assert(ec == boost::system::errc::success);
        db->validate(op_info->bl1, op_info->offset1, op_info->length1);
        db->validate(op_info->bl2, op_info->offset2, op_info->length2);
        db->validate(op_info->bl3, op_info->offset3, op_info->length3);
        finish_io();
      };
      librados::async_operate(asio, io, oid,
                              &op_info->rop, 0, nullptr, read3_cb);
      num_io++;
    }
    break;

  case WRITE:
    {
      start_io();
      op_info = std::make_shared<AsyncOpInfo>(op.offset1, op.length1);
      db->generate_data(op.offset1, op.length1, op_info->bl1);

      op_info->wop.write(op.offset1 * block_size, op_info->bl1);
      auto write_cb = [this] (boost::system::error_code ec) {
        ceph_assert(ec == boost::system::errc::success);
        finish_io();
      };
      librados::async_operate(asio, io, oid,
                              &op_info->wop, 0, nullptr, write_cb);
      num_io++;
    }
    break;

  case WRITE2:
    {
      start_io();
      op_info = std::make_shared<AsyncOpInfo>(op.offset1, op.length1,
                                              op.offset2, op.length2);
      db->generate_data(op.offset1, op.length1, op_info->bl1);
      db->generate_data(op.offset2, op.length2, op_info->bl2);
      op_info->wop.write(op.offset1 * block_size, op_info->bl1);
      op_info->wop.write(op.offset2 * block_size, op_info->bl2);
      auto write2_cb = [this] (boost::system::error_code ec) {
        ceph_assert(ec == boost::system::errc::success);
        finish_io();
      };
      librados::async_operate(asio, io, oid,
                              &op_info->wop, 0, nullptr, write2_cb);
      num_io++;
    }
    break;

  case WRITE3:
    {
      start_io();
      op_info = std::make_shared<AsyncOpInfo>(op.offset1, op.length1,
                                              op.offset2, op.length2,
                                              op.offset3, op.length3);
      db->generate_data(op.offset1, op.length1, op_info->bl1);
      db->generate_data(op.offset2, op.length2, op_info->bl2);
      db->generate_data(op.offset3, op.length3, op_info->bl3);
      op_info->wop.write(op.offset1 * block_size, op_info->bl1);
      op_info->wop.write(op.offset2 * block_size, op_info->bl2);
      op_info->wop.write(op.offset3 * block_size, op_info->bl3);
      auto write3_cb = [this] (boost::system::error_code ec) {
        ceph_assert(ec == boost::system::errc::success);
        finish_io();
      };
      librados::async_operate(asio, io, oid,
                              &op_info->wop, 0, nullptr, write3_cb);
      num_io++;
    }
    break;

  default:
    break;
  }
}
