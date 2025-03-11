#include "RadosIo.h"

#include <fmt/format.h>
#include <json_spirit/json_spirit.h>

#include <ranges>

#include "DataGenerator.h"
#include "common/ceph_json.h"
#include "common/json/OSDStructures.h"

using RadosIo = ceph::io_exerciser::RadosIo;

namespace {
template <typename S>
int send_osd_command(int osd, S& s, librados::Rados& rados, const char* name,
                     ceph::buffer::list& inbl, ceph::buffer::list* outbl,
                     Formatter* f) {
  encode_json(name, s, f);

  std::ostringstream oss;
  f->flush(oss);
  int rc = rados.osd_command(osd, oss.str(), inbl, outbl, nullptr);
  return rc;
}

template <typename S>
int send_mon_command(S& s, librados::Rados& rados, const char* name,
                     ceph::buffer::list& inbl, ceph::buffer::list* outbl,
                     Formatter* f) {
  encode_json(name, s, f);

  std::ostringstream oss;
  f->flush(oss);
  int rc = rados.mon_command(oss.str(), inbl, outbl, nullptr);
  return rc;
}
}  // namespace

RadosIo::RadosIo(librados::Rados& rados, boost::asio::io_context& asio,
                 const std::string& pool, const std::string& oid,
                 const std::optional<std::vector<int>>& cached_shard_order,
                 uint64_t block_size, int seed, int threads, ceph::mutex& lock,
                 ceph::condition_variable& cond, bool ec_optimizations)
    : Model(oid, block_size),
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
      outstanding_io(0) {
  int rc;
  rc = rados.ioctx_create(pool.c_str(), io);
  ceph_assert(rc == 0);
  allow_ec_overwrites(true);
  if (ec_optimizations) {
    allow_ec_optimizations();
  }
}

RadosIo::~RadosIo() {}

void RadosIo::start_io() {
  std::lock_guard l(lock);
  outstanding_io++;
}

void RadosIo::finish_io() {
  std::lock_guard l(lock);
  ceph_assert(outstanding_io > 0);
  outstanding_io--;
  cond.notify_all();
}

void RadosIo::wait_for_io(int count) {
  std::unique_lock l(lock);
  while (outstanding_io > count) {
    cond.wait(l);
  }
}

void RadosIo::allow_ec_overwrites(bool allow) {
  int rc;
  bufferlist inbl, outbl;
  std::string cmdstr = "{\"prefix\": \"osd pool set\", \"pool\": \"" + pool +
                       "\", \
      \"var\": \"allow_ec_overwrites\", \"val\": \"" +
                       (allow ? "true" : "false") + "\"}";
  rc = rados.mon_command(cmdstr, inbl, &outbl, nullptr);
  ceph_assert(rc == 0);
}

void RadosIo::allow_ec_optimizations()
{
  int rc;
  bufferlist inbl, outbl;
  std::string cmdstr =
    "{\"prefix\": \"osd pool set\", \"pool\": \"" + pool + "\", \
      \"var\": \"allow_ec_optimizations\", \"val\": \"true\"}";
  rc = rados.mon_command(cmdstr, inbl, &outbl, nullptr);
  ceph_assert(rc == 0);
}

template <int N>
RadosIo::AsyncOpInfo<N>::AsyncOpInfo(const std::array<uint64_t, N>& offset,
                                     const std::array<uint64_t, N>& length)
    : offset(offset), length(length) {}

bool RadosIo::readyForIoOp(IoOp& op) {
  ceph_assert(
      ceph_mutex_is_locked_by_me(lock));  // Must be called with lock held
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

void RadosIo::applyIoOp(IoOp& op) {
  om->applyIoOp(op);

  // If there are thread concurrent I/Os in flight then wait for
  // at least one I/O to complete
  wait_for_io(threads - 1);

  switch (op.getOpType()) {
    case OpType::Done:
      [[fallthrough]];
    case OpType::Barrier:
      // Wait for all outstanding I/O to complete
      wait_for_io(0);
      break;

    case OpType::Create: {
      start_io();
      uint64_t opSize = static_cast<CreateOp&>(op).size;
      std::shared_ptr<AsyncOpInfo<1>> op_info =
          std::make_shared<AsyncOpInfo<1>>(std::array<uint64_t, 1>{0},
                                           std::array<uint64_t, 1>{opSize});
      op_info->bufferlist[0] = db->generate_data(0, opSize);
      librados::ObjectWriteOperation wop;
      wop.write_full(op_info->bufferlist[0]);
      auto create_cb = [this](boost::system::error_code ec, version_t ver) {
        ceph_assert(ec == boost::system::errc::success);
        finish_io();
      };
      librados::async_operate(asio.get_executor(), io, oid,
                              std::move(wop), 0, nullptr, create_cb);
      break;
    }

    case OpType::Truncate: {
      start_io();
      uint64_t opSize = static_cast<TruncateOp&>(op).size;
      auto op_info = std::make_shared<AsyncOpInfo<0>>();
      librados::ObjectWriteOperation wop;
      wop.truncate(opSize * block_size);
      auto truncate_cb = [this](boost::system::error_code ec, version_t ver) {
        ceph_assert(ec == boost::system::errc::success);
        finish_io();
      };
      librados::async_operate(asio.get_executor(), io, oid, std::move(wop), 0,
                              nullptr, truncate_cb);
      break;
    }

    case OpType::Remove: {
      start_io();
      auto op_info = std::make_shared<AsyncOpInfo<0>>();
      librados::ObjectWriteOperation wop;
      wop.remove();
      auto remove_cb = [this](boost::system::error_code ec, version_t ver) {
        ceph_assert(ec == boost::system::errc::success);
        finish_io();
      };
      librados::async_operate(asio.get_executor(), io, oid,
                              std::move(wop), 0, nullptr, remove_cb);
      break;
    }
    case OpType::Read:
      [[fallthrough]];
    case OpType::Read2:
      [[fallthrough]];
    case OpType::Read3:
      [[fallthrough]];
    case OpType::Write:
      [[fallthrough]];
    case OpType::Write2:
      [[fallthrough]];
    case OpType::Write3:
      [[fallthrough]];
    case OpType::Append:
      [[fallthrough]];
    case OpType::FailedWrite:
      [[fallthrough]];
    case OpType::FailedWrite2:
      [[fallthrough]];
    case OpType::FailedWrite3:
      applyReadWriteOp(op);
      break;
    case OpType::InjectReadError:
      [[fallthrough]];
    case OpType::InjectWriteError:
      [[fallthrough]];
    case OpType::ClearReadErrorInject:
      [[fallthrough]];
    case OpType::ClearWriteErrorInject:
      applyInjectOp(op);
      break;
    default:
      ceph_abort_msg("Unrecognised Op");
      break;
  }
}

void RadosIo::applyReadWriteOp(IoOp& op) {
  auto applyReadOp = [this]<OpType opType, int N>(
                         ReadWriteOp<opType, N> readOp) {
    auto op_info =
        std::make_shared<AsyncOpInfo<N>>(readOp.offset, readOp.length);

    librados::ObjectReadOperation rop;
    for (int i = 0; i < N; i++) {
      rop.read(readOp.offset[i] * block_size,
               readOp.length[i] * block_size, &op_info->bufferlist[i],
               nullptr);
    }
    auto read_cb = [this, op_info](boost::system::error_code ec, version_t ver,
                                   bufferlist bl) {
      ceph_assert(ec == boost::system::errc::success);
      for (int i = 0; i < N; i++) {
        ceph_assert(db->validate(op_info->bufferlist[i], op_info->offset[i],
                                 op_info->length[i]));
      }
      finish_io();
    };
    librados::async_operate(asio.get_executor(), io, oid,
                            std::move(rop), 0, nullptr, read_cb);
    num_io++;
  };

  auto applyWriteOp = [this]<OpType opType, int N>(
                          ReadWriteOp<opType, N> writeOp) {
    auto op_info =
        std::make_shared<AsyncOpInfo<N>>(writeOp.offset, writeOp.length);
    librados::ObjectWriteOperation wop;
    for (int i = 0; i < N; i++) {
      op_info->bufferlist[i] =
          db->generate_data(writeOp.offset[i], writeOp.length[i]);
      wop.write(writeOp.offset[i] * block_size,
                op_info->bufferlist[i]);
    }
    auto write_cb = [this](boost::system::error_code ec, version_t ver) {
      ceph_assert(ec == boost::system::errc::success);
      finish_io();
    };
    librados::async_operate(asio.get_executor(), io, oid,
                            std::move(wop), 0, nullptr, write_cb);
    num_io++;
  };

  auto applyFailedWriteOp = [this]<OpType opType, int N>(
                                ReadWriteOp<opType, N> writeOp) {
    auto op_info =
        std::make_shared<AsyncOpInfo<N>>(writeOp.offset, writeOp.length);
    librados::ObjectWriteOperation wop;
    for (int i = 0; i < N; i++) {
      op_info->bufferlist[i] =
          db->generate_data(writeOp.offset[i], writeOp.length[i]);
      wop.write(writeOp.offset[i] * block_size,
                op_info->bufferlist[i]);
    }
    auto write_cb = [this, writeOp](boost::system::error_code ec,
                                    version_t ver) {
      ceph_assert(ec != boost::system::errc::success);
      finish_io();
    };
    librados::async_operate(asio.get_executor(), io, oid,
                            std::move(wop), 0, nullptr, write_cb);
    num_io++;
  };

  switch (op.getOpType()) {
    case OpType::Read: {
      start_io();
      SingleReadOp& readOp = static_cast<SingleReadOp&>(op);
      applyReadOp(readOp);
      break;
    }
    case OpType::Read2: {
      start_io();
      DoubleReadOp& readOp = static_cast<DoubleReadOp&>(op);
      applyReadOp(readOp);
      break;
    }
    case OpType::Read3: {
      start_io();
      TripleReadOp& readOp = static_cast<TripleReadOp&>(op);
      applyReadOp(readOp);
      break;
    }
    case OpType::Write: {
      start_io();
      SingleWriteOp& writeOp = static_cast<SingleWriteOp&>(op);
      applyWriteOp(writeOp);
      break;
    }
    case OpType::Write2: {
      start_io();
      DoubleWriteOp& writeOp = static_cast<DoubleWriteOp&>(op);
      applyWriteOp(writeOp);
      break;
    }
    case OpType::Write3: {
      start_io();
      TripleWriteOp& writeOp = static_cast<TripleWriteOp&>(op);
      applyWriteOp(writeOp);
      break;
    }

    case OpType::Append: {
      start_io();
      SingleAppendOp& appendOp = static_cast<SingleAppendOp&>(op);
      applyWriteOp(appendOp);
      break;
    }

    case OpType::FailedWrite: {
      start_io();
      SingleFailedWriteOp& writeOp = static_cast<SingleFailedWriteOp&>(op);
      applyFailedWriteOp(writeOp);
      break;
    }
    case OpType::FailedWrite2: {
      start_io();
      DoubleFailedWriteOp& writeOp = static_cast<DoubleFailedWriteOp&>(op);
      applyFailedWriteOp(writeOp);
      break;
    }
    case OpType::FailedWrite3: {
      start_io();
      TripleFailedWriteOp& writeOp = static_cast<TripleFailedWriteOp&>(op);
      applyFailedWriteOp(writeOp);
      break;
    }

    default:
      ceph_abort_msg(
          fmt::format("Unsupported Read/Write operation ({})", op.getOpType()));
      break;
  }
}

void RadosIo::applyInjectOp(IoOp& op) {
  bufferlist osdmap_inbl, inject_inbl, osdmap_outbl, inject_outbl;
  auto formatter = std::make_unique<JSONFormatter>(false);

  int osd = -1;
  std::vector<int> shard_order;

  ceph::messaging::osd::OSDMapRequest osdMapRequest{pool, get_oid(), ""};
  int rc = send_mon_command(osdMapRequest, rados, "OSDMapRequest", osdmap_inbl,
                            &osdmap_outbl, formatter.get());
  ceph_assert(rc == 0);

  JSONParser p;
  bool success = p.parse(osdmap_outbl.c_str(), osdmap_outbl.length());
  ceph_assert(success);

  ceph::messaging::osd::OSDMapReply reply;
  reply.decode_json(&p);

  osd = reply.acting_primary;
  shard_order = reply.acting;

  switch (op.getOpType()) {
    case OpType::InjectReadError: {
      InjectReadErrorOp& errorOp = static_cast<InjectReadErrorOp&>(op);

      if (errorOp.type == 0) {
        ceph::messaging::osd::InjectECErrorRequest<InjectOpType::ReadEIO>
            injectErrorRequest{pool,         oid,          errorOp.shard,
                               errorOp.type, errorOp.when, errorOp.duration};
        int rc = send_osd_command(osd, injectErrorRequest, rados,
                                  "InjectECErrorRequest", inject_inbl,
                                  &inject_outbl, formatter.get());
        ceph_assert(rc == 0);
      } else if (errorOp.type == 1) {
        ceph::messaging::osd::InjectECErrorRequest<
            InjectOpType::ReadMissingShard>
            injectErrorRequest{pool,         oid,          errorOp.shard,
                               errorOp.type, errorOp.when, errorOp.duration};
        int rc = send_osd_command(osd, injectErrorRequest, rados,
                                  "InjectECErrorRequest", inject_inbl,
                                  &inject_outbl, formatter.get());
        ceph_assert(rc == 0);
      } else {
        ceph_abort_msg("Unsupported inject type");
      }
      break;
    }
    case OpType::InjectWriteError: {
      InjectWriteErrorOp& errorOp = static_cast<InjectWriteErrorOp&>(op);

      if (errorOp.type == 0) {
        ceph::messaging::osd::InjectECErrorRequest<
            InjectOpType::WriteFailAndRollback>
            injectErrorRequest{pool,         oid,          errorOp.shard,
                               errorOp.type, errorOp.when, errorOp.duration};
        int rc = send_osd_command(osd, injectErrorRequest, rados,
                                  "InjectECErrorRequest", inject_inbl,
                                  &inject_outbl, formatter.get());
        ceph_assert(rc == 0);
      } else if (errorOp.type == 3) {
        ceph::messaging::osd::InjectECErrorRequest<InjectOpType::WriteOSDAbort>
            injectErrorRequest{pool,         oid,          errorOp.shard,
                               errorOp.type, errorOp.when, errorOp.duration};
        int rc = send_osd_command(osd, injectErrorRequest, rados,
                                  "InjectECErrorRequest", inject_inbl,
                                  &inject_outbl, formatter.get());
        ceph_assert(rc == 0);

        // This inject is sent directly to the shard we want to inject the error
        // on
        osd = shard_order[errorOp.shard];
      } else {
        ceph_abort("Unsupported inject type");
      }

      break;
    }
    case OpType::ClearReadErrorInject: {
      ClearReadErrorInjectOp& errorOp =
          static_cast<ClearReadErrorInjectOp&>(op);

      if (errorOp.type == 0) {
        ceph::messaging::osd::InjectECClearErrorRequest<InjectOpType::ReadEIO>
            clearErrorInject{pool, oid, errorOp.shard, errorOp.type};
        int rc = send_osd_command(osd, clearErrorInject, rados,
                                  "InjectECClearErrorRequest", inject_inbl,
                                  &inject_outbl, formatter.get());
        ceph_assert(rc == 0);
      } else if (errorOp.type == 1) {
        ceph::messaging::osd::InjectECClearErrorRequest<
            InjectOpType::ReadMissingShard>
            clearErrorInject{pool, oid, errorOp.shard, errorOp.type};
        int rc = send_osd_command(osd, clearErrorInject, rados,
                                  "InjectECClearErrorRequest", inject_inbl,
                                  &inject_outbl, formatter.get());
        ceph_assert(rc == 0);
      } else {
        ceph_abort("Unsupported inject type");
      }

      break;
    }
    case OpType::ClearWriteErrorInject: {
      ClearReadErrorInjectOp& errorOp =
          static_cast<ClearReadErrorInjectOp&>(op);

      if (errorOp.type == 0) {
        ceph::messaging::osd::InjectECClearErrorRequest<
            InjectOpType::WriteFailAndRollback>
            clearErrorInject{pool, oid, errorOp.shard, errorOp.type};
        int rc = send_osd_command(osd, clearErrorInject, rados,
                                  "InjectECClearErrorRequest", inject_inbl,
                                  &inject_outbl, formatter.get());
        ceph_assert(rc == 0);
      } else if (errorOp.type == 3) {
        ceph::messaging::osd::InjectECClearErrorRequest<
            InjectOpType::WriteOSDAbort>
            clearErrorInject{pool, oid, errorOp.shard, errorOp.type};
        int rc = send_osd_command(osd, clearErrorInject, rados,
                                  "InjectECClearErrorRequest", inject_inbl,
                                  &inject_outbl, formatter.get());
        ceph_assert(rc == 0);
      } else {
        ceph_abort("Unsupported inject type");
      }

      break;
    }
    default:
      ceph_abort_msg(
          fmt::format("Unsupported inject operation ({})", op.getOpType()));
      break;
  }
}
