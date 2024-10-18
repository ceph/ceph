#include "RadosIo.h"

#include "DataGenerator.h"

#include <json_spirit/json_spirit.h>
#include "common/ceph_json.h"

#include "JsonStructures.h"

#include <ranges>

using RadosIo = ceph::io_exerciser::RadosIo;

RadosIo::RadosIo(librados::Rados& rados,
        boost::asio::io_context& asio,
        const std::string& pool,
        const std::string& oid,
        const std::optional<std::vector<int>>& cached_shard_order,
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
  cached_shard_order(cached_shard_order),
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
RadosIo::AsyncOpInfo<N>::AsyncOpInfo(std::array<uint64_t, N> offset, std::array<uint64_t, N> length) :
  offset(offset), length(length)
{

}

bool RadosIo::readyForIoOp(IoOp &op)
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock)); //Must be called with lock held
  if (!om->readyForIoOp(op)) {
    return false;
  }

  switch (op.getOpType()) {
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
  switch (op.getOpType()) {
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
    break;
  }

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
    break;
  }
  case OpType::Read:
    [[ fallthrough ]];
  case OpType::Read2:
    [[ fallthrough ]];
  case OpType::Read3:
    [[ fallthrough ]];
  case OpType::Write:
    [[ fallthrough ]];
  case OpType::Write2:
    [[ fallthrough ]];
  case OpType::Write3:
    applyReadWriteOp(op);
    break;
  case OpType::InjectReadError:
    [[ fallthrough ]];
  case OpType::InjectWriteError:
    [[ fallthrough ]];
  case OpType::ClearReadErrorInject:
    [[ fallthrough ]];
  case OpType::ClearWriteErrorInject:
    applyInjectOp(op);
    break;
  default:
    ceph_abort_msg("Unrecognised Op");
    break;
  }
}

void RadosIo::applyReadWriteOp(IoOp& op)
{
  switch (op.getOpType()) {
  case OpType::Done:
    [[ fallthrough ]];
  case OpType::Barrier:
    [[ fallthrough ]];
  case OpType::Create:
    [[ fallthrough ]];
  case OpType::Remove:
    // Should not reach this point
    ceph_abort();
    break;
  case OpType::Read:
  {
    start_io();
    SingleReadOp& readOp = static_cast<SingleReadOp&>(op);
    auto op_info = std::make_shared<AsyncOpInfo<1>>(readOp.offset, readOp.length);
    op_info->rop.read(readOp.offset[0] * block_size,
                      readOp.length[0] * block_size,
                      &op_info->bufferlist[0], nullptr);
    auto read_cb = [this, op_info] (boost::system::error_code ec,
                                    version_t ver,
                                    bufferlist bl) {
      ceph_assert(ec == boost::system::errc::success);
      ceph_assert(db->validate(op_info->bufferlist[0],
                               op_info->offset[0],
                               op_info->length[0]));
      finish_io();
    };
    librados::async_operate(asio, io, oid,
                            &op_info->rop, 0, nullptr, read_cb);
    num_io++;
    break;
  }

  case OpType::Read2:
  {
    start_io();
    DoubleReadOp& readOp = static_cast<DoubleReadOp&>(op);
    auto op_info = std::make_shared<AsyncOpInfo<2>>(readOp.offset, readOp.length);

    for (const int i : std::views::iota(0,2))
    {
      op_info->rop.read(readOp.offset[i] * block_size,
                        readOp.length[i] * block_size,
                        &op_info->bufferlist[i], nullptr);
    }
    auto read2_cb = [this, op_info] (boost::system::error_code ec,
                                     version_t ver,
                                     bufferlist bl) {
      ceph_assert(ec == boost::system::errc::success);
      for (const int i : std::views::iota(0,2))
      {
        ceph_assert(db->validate(op_info->bufferlist[i],
                                 op_info->offset[i],
                                 op_info->length[i]));
      }
      finish_io();
    };
    librados::async_operate(asio, io, oid,
                            &op_info->rop, 0, nullptr, read2_cb);
    num_io++;
    break;
  }

  case OpType::Read3:
    {
      start_io();
      TripleReadOp& readOp = static_cast<TripleReadOp&>(op);
      auto op_info = std::make_shared<AsyncOpInfo<3>>(readOp.offset, readOp.length);
      for (const int i : std::views::iota(0,3))
      {
        op_info->rop.read(readOp.offset[i] * block_size,
                          readOp.length[i] * block_size,
                          &op_info->bufferlist[i], nullptr);
      }
      auto read3_cb = [this, op_info] (boost::system::error_code ec,
                                       version_t ver,
                                       bufferlist bl) {
        ceph_assert(ec == boost::system::errc::success);
        for (const int i : std::views::iota(0,3))
        {
          ceph_assert(db->validate(op_info->bufferlist[i],
                                   op_info->offset[i],
                                   op_info->length[i]));
        }
        finish_io();
      };
      librados::async_operate(asio, io, oid,
                              &op_info->rop, 0, nullptr, read3_cb);
      num_io++;
      break;
    }

  case OpType::Write:
  {
    start_io();
    SingleWriteOp& writeOp = static_cast<SingleWriteOp&>(op);
    auto op_info = std::make_shared<AsyncOpInfo<1>>(writeOp.offset, writeOp.length);
    op_info->bufferlist[0] = db->generate_data(writeOp.offset[0], writeOp.length[0]);
    op_info->wop.write(writeOp.offset[0] * block_size, op_info->bufferlist[0]);
    auto write_cb = [this] (boost::system::error_code ec,
                            version_t ver) {
      ceph_assert(ec == boost::system::errc::success);
      finish_io();
    };
    librados::async_operate(asio, io, oid,
                            &op_info->wop, 0, nullptr, write_cb);
    num_io++;
    break;
  }

  case OpType::Write2:
  {
    start_io();
    DoubleWriteOp& writeOp = static_cast<DoubleWriteOp&>(op);
    auto op_info = std::make_shared<AsyncOpInfo<2>>(writeOp.offset, writeOp.length);
    for (const int i : std::views::iota(0,2))
    {
      op_info->bufferlist[i] = db->generate_data(writeOp.offset[i], writeOp.length[i]);
      op_info->wop.write(writeOp.offset[i] * block_size, op_info->bufferlist[i]);
    }
    auto write2_cb = [this] (boost::system::error_code ec,
                             version_t ver) {
      ceph_assert(ec == boost::system::errc::success);
      finish_io();
    };
    librados::async_operate(asio, io, oid,
                            &op_info->wop, 0, nullptr, write2_cb);
    num_io++;
    break;
  }

  case OpType::Write3:
  {
    start_io();
    TripleWriteOp& writeOp = static_cast<TripleWriteOp&>(op);
    auto op_info = std::make_shared<AsyncOpInfo<3>>(writeOp.offset, writeOp.length);
    for (const int i : std::views::iota(0,3))
    {
      op_info->bufferlist[i] = db->generate_data(writeOp.offset[i], writeOp.length[i]);
      op_info->wop.write(writeOp.offset[i] * block_size, op_info->bufferlist[i]);
    }
    auto write3_cb = [this] (boost::system::error_code ec,
                             version_t ver) {
      ceph_assert(ec == boost::system::errc::success);
      finish_io();
    };
    librados::async_operate(asio, io, oid,
                            &op_info->wop, 0, nullptr, write3_cb);
    num_io++;
    break;
  }

  case OpType::InjectReadError:
    [[ fallthrough ]];
  case OpType::InjectWriteError:
    // Should not reach this point
    ceph_abort();
    break;
  default:
    // Unknown case
    ceph_abort();
    break;
  }
}

void RadosIo::applyInjectOp(IoOp& op)
{
  bufferlist osdmap_inbl, inject_inbl, osdmap_outbl, inject_outbl;
  auto formatter = std::make_shared<JSONFormatter>(false);

  int osd = -1;
  std::vector<int> shard_order;

  if (cached_shard_order)
  {
    osd = cached_shard_order->front();
    shard_order = *cached_shard_order;
  }
  else
  {
    ceph::io_exerciser::json::OSDMapRequest osdMapRequest(pool, formatter);
    int rc = rados.mon_command(osdMapRequest.encode_json(), osdmap_inbl, &osdmap_outbl, nullptr);
    ceph_assert(rc == 0);

    JSONParser p;
    bool success = p.parse(osdmap_outbl.c_str(), osdmap_outbl.length());
    ceph_assert(success);

    ceph::io_exerciser::json::OSDMapReply reply(&p, formatter);

    osd = reply.acting_primary;
    shard_order = reply.acting;
  }

  InjectOpType injectOpType;

  switch(op.getOpType())
  {
      case OpType::Done:
    [[ fallthrough ]];
  case OpType::Barrier:
    [[ fallthrough ]];
  case OpType::Create:
    [[ fallthrough ]];
  case OpType::Remove:
    [[ fallthrough ]];
  case OpType::Read:
    [[ fallthrough ]];
  case OpType::Read2:
    [[ fallthrough ]];
  case OpType::Read3:
    [[ fallthrough ]];
  case OpType::Write:
    [[ fallthrough ]];
  case OpType::Write2:
    [[ fallthrough ]];
  case OpType::Write3:
    // Should not reach this point
    ceph_abort();
    break;
    case OpType::InjectReadError:
    {
      InjectReadErrorOp& errorOp = static_cast<InjectReadErrorOp&>(op);

      if (errorOp.type == 0)
      {
        injectOpType = InjectOpType::ReadEIO;
      }
      else if (errorOp.type == 1)
      {
        injectOpType = InjectOpType::ReadMissingShard;
      }
      else
      {
        ceph_abort(); // Unsupported inject type
      }

      ceph::io_exerciser::json::InjectECErrorRequest injectErrorRequest(injectOpType,
                                                                        pool,
                                                                        oid,
                                                                        errorOp.shard,
                                                                        errorOp.type,
                                                                        errorOp.when,
                                                                        errorOp.duration,
                                                                        formatter);

      int rc = rados.osd_command(osd, injectErrorRequest.encode_json(), inject_inbl, &inject_outbl, nullptr);
      ceph_assert(rc == 0);
      break;
    }
    case OpType::InjectWriteError:
    {
      InjectWriteErrorOp& errorOp = static_cast<InjectWriteErrorOp&>(op);

      if (errorOp.type == 0)
      {
        injectOpType = InjectOpType::WriteFailAndRollback;
      }
      else if (errorOp.type == 3)
      {
        injectOpType = InjectOpType::WriteOSDAbort;

         // This inject is sent directly to the shard we want to inject the error on
        osd = shard_order[errorOp.shard];
      }
      else
      {
        ceph_abort(); // Unsupported inject type
      }

      ceph::io_exerciser::json::InjectECErrorRequest injectErrorRequest(injectOpType,
                                                                        pool,
                                                                        oid,
                                                                        errorOp.shard,
                                                                        errorOp.type,
                                                                        errorOp.when,
                                                                        errorOp.duration,
                                                                        formatter);

      int rc = rados.osd_command(osd, injectErrorRequest.encode_json(), inject_inbl, &inject_outbl, nullptr);
      ceph_assert(rc == 0);
      break;
    }
    case OpType::ClearReadErrorInject:
    {
      ClearReadErrorInjectOp& errorOp = static_cast<ClearReadErrorInjectOp&>(op);

            if (errorOp.type == 0)
      {
        injectOpType = InjectOpType::ReadEIO;
      }
      else if (errorOp.type == 1)
      {
        injectOpType = InjectOpType::ReadMissingShard;
      }
      else
      {
        ceph_abort(); // Unsupported inject type
      }

      ceph::io_exerciser::json::InjectECClearErrorRequest clearErrorInject(injectOpType,
                                                                           pool,
                                                                           oid,
                                                                           errorOp.shard,
                                                                           errorOp.type);

      int rc = rados.osd_command(osd, clearErrorInject.encode_json(), inject_inbl, &inject_outbl, nullptr);
      ceph_assert(rc == 0);
      break;
    }
    case OpType::ClearWriteErrorInject:
    {
      ClearReadErrorInjectOp& errorOp = static_cast<ClearReadErrorInjectOp&>(op);

            if (errorOp.type == 0)
      {
        injectOpType = InjectOpType::WriteFailAndRollback;
      }
      else if (errorOp.type == 3)
      {
        injectOpType = InjectOpType::WriteOSDAbort;
      }
      else
      {
        ceph_abort(); // Unsupported inject type
      }

      ceph::io_exerciser::json::InjectECClearErrorRequest clearErrorInject(injectOpType,
                                                                           pool,
                                                                           oid,
                                                                           errorOp.shard,
                                                                           errorOp.type);

      int rc = rados.osd_command(osd, clearErrorInject.encode_json(), inject_inbl, &inject_outbl, nullptr);
      ceph_assert(rc == 0);
      break;
    }
  }
}