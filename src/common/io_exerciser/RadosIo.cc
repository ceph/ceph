#include "RadosIo.h"

#include "DataGenerator.h"

#include <ranges>

using RadosIo = ceph::io_exerciser::RadosIo;

RadosIo::RadosIo(librados::Rados& rados,
        boost::asio::io_context& asio,
        const std::string& pool,
        const std::string& oid,
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

template <int N>
RadosIo::AsyncOpInfo<N>::AsyncOpInfo(const std::array<uint64_t, N>& offset, const std::array<uint64_t, N>& length) :
  offset(offset), length(length)
{

}

bool RadosIo::readyForIoOp(IoOp &op)
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock)); //Must be called with lock held
  if (!om->readyForIoOp(op))
  {
    return false;
  }

  switch (op.getOpType())
  {
  case OpType::Done:
  case OpType::Barrier:
    return outstanding_io == 0;
  default:
    return outstanding_io < threads;
  }
}

void RadosIo::applyIoOp(IoOp& op)
{
  om->applyIoOp(op);

  // If there are thread concurrent I/Os in flight then wait for
  // at least one I/O to complete
  wait_for_io(threads-1);

  auto applyReadOp = [this]<OpType opType, int N>(ReadWriteOp<opType, N> readOp)
  {
    auto op_info = std::make_shared<AsyncOpInfo<N>>(readOp.offset, readOp.length);

      for (int i = 0; i < N; i++)
      {
        op_info->rop.read(readOp.offset[i] * block_size,
                          readOp.length[i] * block_size,
                          &op_info->bufferlist[i], nullptr);
      }
      auto read_cb = [this, op_info] (boost::system::error_code ec,
                                       version_t ver,
                                       bufferlist bl)
      {
        ceph_assert(ec == boost::system::errc::success);
        for (int i = 0; i < N; i++)
        {
          ceph_assert(db->validate(op_info->bufferlist[i],
                                   op_info->offset[i],
                                   op_info->length[i]));
        }
        finish_io();
      };
      librados::async_operate(asio, io, oid,
                              &op_info->rop, 0, nullptr, read_cb);
      num_io++;
  };

  auto applyWriteOp = [this]<OpType opType, int N>(ReadWriteOp<opType, N> writeOp)
  {
    auto op_info = std::make_shared<AsyncOpInfo<N>>(writeOp.offset, writeOp.length);
    for (int i = 0; i < N; i++)
    {
      op_info->bufferlist[i] = db->generate_data(writeOp.offset[i], writeOp.length[i]);
      op_info->wop.write(writeOp.offset[i] * block_size, op_info->bufferlist[i]);
    }
    auto write_cb = [this] (boost::system::error_code ec,
                             version_t ver)
    {
      ceph_assert(ec == boost::system::errc::success);
      finish_io();
    };
    librados::async_operate(asio, io, oid,
                            &op_info->wop, 0, nullptr, write_cb);
    num_io++;
  };

  switch (op.getOpType())
  {
  case OpType::Done:
  [[ fallthrough ]];
  case OpType::Barrier:
    // Wait for all outstanding I/O to complete
    wait_for_io(0);
    break;

  case OpType::Create:
    {
      start_io();
      uint64_t opSize = static_cast<CreateOp&>(op).size;
      std::shared_ptr<AsyncOpInfo<1>> op_info = std::make_shared<AsyncOpInfo<1>>(std::array<uint64_t,1>{0}, std::array<uint64_t,1>{opSize});
      op_info->bufferlist[0] = db->generate_data(0, opSize);
      op_info->wop.write_full(op_info->bufferlist[0]);
      auto create_cb = [this] (boost::system::error_code ec,
                               version_t ver) {
        ceph_assert(ec == boost::system::errc::success);
        finish_io();
      };
      librados::async_operate(asio, io, oid,
                              &op_info->wop, 0, nullptr, create_cb);
    }
    break;

  case OpType::Remove:
    {
      start_io();
      auto op_info = std::make_shared<AsyncOpInfo<0>>();
      op_info->wop.remove();
      auto remove_cb = [this] (boost::system::error_code ec,
                               version_t ver) {
        ceph_assert(ec == boost::system::errc::success);
        finish_io();
      };
      librados::async_operate(asio, io, oid,
                              &op_info->wop, 0, nullptr, remove_cb);
    }
    break;

  case OpType::Read:
    {
      start_io();
      SingleReadOp& readOp = static_cast<SingleReadOp&>(op);
      applyReadOp(readOp);
    }
    break;

  case OpType::Read2:
    {
      start_io();
      DoubleReadOp& readOp = static_cast<DoubleReadOp&>(op);
      applyReadOp(readOp);
    }
    break;

  case OpType::Read3:
    {
      start_io();
      TripleReadOp& readOp = static_cast<TripleReadOp&>(op);
      applyReadOp(readOp);
    }
    break;

  case OpType::Write:
    {
      start_io();
      SingleWriteOp& writeOp = static_cast<SingleWriteOp&>(op);
      applyWriteOp(writeOp);
    }
    break;

  case OpType::Write2:
    {
      start_io();
      DoubleWriteOp& writeOp = static_cast<DoubleWriteOp&>(op);
      applyWriteOp(writeOp);
    }
    break;

  case OpType::Write3:
    {
      start_io();
      TripleWriteOp& writeOp = static_cast<TripleWriteOp&>(op);
      applyWriteOp(writeOp);
    }
    break;

  default:
    break;
  }
}
