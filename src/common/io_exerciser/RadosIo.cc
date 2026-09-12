#include "RadosIo.h"

#include <fmt/format.h>
#include <map>
#include <json_spirit/json_spirit.h>

#include <ranges>

#include "DataGenerator.h"
#include "IoOp.h"
#include "common/ceph_json.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/io_exerciser/IoSequence.h"
#include "common/json/OSDStructures.h"
#include "librados/librados_asio.h"

#include <boost/asio/io_context.hpp>

#define dout_subsys ceph_subsys_rados
#define dout_context g_ceph_context

using RadosIo = ceph::io_exerciser::RadosIo;
using ConsistencyChecker = ceph::consistency::ConsistencyChecker;

using GenerationType = ceph::io_exerciser::data_generation::GenerationType;
using MapextOp = ceph::io_exerciser::MapextOp;
using WriteZeroDataOp = ceph::io_exerciser::WriteZeroDataOp;

namespace {
template <typename S>
int send_osd_command(int osd, S& s, librados::Rados& rados, const char* name,
                     ceph::buffer::list&& inbl, ceph::buffer::list* outbl,
                     Formatter* f) {
  encode_json(name, s, f);

  std::ostringstream oss;
  f->flush(oss);
  int rc = rados.osd_command(osd, oss.str(), std::move(inbl), outbl, nullptr);
  return rc;
}

template <typename S>
int send_mon_command(S& s, librados::Rados& rados, const char* name,
                     ceph::buffer::list&& inbl, ceph::buffer::list* outbl,
                     Formatter* f) {
  encode_json(name, s, f);

  std::ostringstream oss;
  f->flush(oss);
  int rc = rados.mon_command(oss.str(), std::move(inbl), outbl, nullptr);
  return rc;
}
}  // namespace

RadosIo::RadosIo(librados::Rados& rados, boost::asio::io_context& asio,
                 const std::string& pool, const std::string& primary_oid, const std::string& secondary_oid,
                 uint64_t block_size, int seed, int threads, ceph::mutex& lock,
                 ceph::condition_variable& cond, bool is_replicated_pool,
                 bool ec_optimizations, int balanced_read_percentage,
                 GenerationType data_generation_type,
                 std::shared_ptr<ceph::io_exerciser::IoSequence> seq, bool delete_objects)
    : Model(primary_oid, secondary_oid, block_size, delete_objects),
      rados(rados),
      asio(asio),
      om(std::make_unique<ObjectModel>(primary_oid, secondary_oid, block_size, seed,
                                       is_replicated_pool, delete_objects)),
      db(data_generation::DataGenerator::create_generator(
          data_generation_type, *om)),
      pool(pool),
      threads(threads),
      lock(lock),
      cond(cond),
      outstanding_io(0),
      seq(seq),
      rng(seed),
      balanced_read_percentage(balanced_read_percentage) {
  int rc;
  rc = rados.ioctx_create(pool.c_str(), io);
  ceph_assert(rc == 0);
  if (!is_replicated_pool) {
    cc = std::make_unique<ConsistencyChecker>(rados, asio, pool);
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

    case OpType::Swap:
      swap_primary_secondary_oid();
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
      librados::async_operate(asio.get_executor(), io, primary_oid,
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
      librados::async_operate(asio.get_executor(), io, primary_oid, std::move(wop), 0,
                              nullptr, truncate_cb);
      break;
    }

    case OpType::Remove: {
      if (!delete_objects) {
        const std::string new_primary_oid = primary_oid_base + "_" + std::to_string(++num_objects);
        set_primary_oid(new_primary_oid);
        break;
      }
      start_io();
      auto op_info = std::make_shared<AsyncOpInfo<0>>();
      librados::ObjectWriteOperation wop;
      wop.remove();
      auto remove_cb = [this](boost::system::error_code ec, version_t ver) {
        ceph_assert(ec == boost::system::errc::success);
        finish_io();
      };
      librados::async_operate(asio.get_executor(), io, primary_oid,
                              std::move(wop), 0, nullptr, remove_cb);
      break;
    }

    case OpType::Consistency: {
      start_io();
      ceph_assert(cc);
      bool is_consistent =
        cc->single_read_and_check_consistency(primary_oid, block_size, 0, 0);
      if (!is_consistent) {
        std::stringstream strstream;
        cc->print_results(strstream);
        std::cerr << strstream.str() << std::endl;
      }
      ceph_assert(is_consistent);
      finish_io();
    }

    case OpType::Copy: {
      start_io();
      auto op_info = std::make_shared<AsyncOpInfo<0>>();
      librados::ObjectWriteOperation wop;
      wop.copy_from(primary_oid.c_str(), io, io.get_last_version(), 0);
      auto write_cb = [this](boost::system::error_code ec, version_t ver) {
        ceph_assert(ec == boost::system::errc::success);
        finish_io();
      };
      librados::async_operate(asio.get_executor(), io, secondary_oid,
                              std::move(wop), 0, nullptr, write_cb);
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
      [[fallthrough]];
    case OpType::Zero:
      [[fallthrough]];
    case OpType::Zero2:
      [[fallthrough]];
    case OpType::WriteZeroData:
      [[fallthrough]];
    case OpType::Mapext:
      applyReadWriteOp(op);
      break;
    case OpType::TruncateWrite:
      [[fallthrough]];
    case OpType::TruncateWrite2:
      [[fallthrough]];
    case OpType::TruncateWrite3:
      applyTruncateWriteOp(op);
      break;
    case OpType::WriteAndZero: {
      start_io();
      WriteAndZeroOp& wzOp = static_cast<WriteAndZeroOp&>(op);
      auto write_op_info = std::make_shared<AsyncOpInfo<1>>(
          std::array<uint64_t, 1>{wzOp.write_offset},
          std::array<uint64_t, 1>{wzOp.write_length});
      if (wzOp.write_offset + wzOp.write_length <= om->get_primary_size()) {
        write_op_info->bufferlist[0] =
            db->generate_data(wzOp.write_offset, wzOp.write_length);
      } else {
        write_op_info->bufferlist[0].append_zero(wzOp.write_length * block_size);
      }
      librados::ObjectWriteOperation wop;
      wop.write(wzOp.write_offset * block_size, write_op_info->bufferlist[0]);
      wop.zero(wzOp.zero_offset * block_size, wzOp.zero_length * block_size);
      auto writeandzero_cb = [this](boost::system::error_code ec, version_t ver) {
        ceph_assert(ec == boost::system::errc::success);
        finish_io();
      };
      librados::async_operate(asio.get_executor(), io, primary_oid,
                              std::move(wop), 0, nullptr, writeandzero_cb);
      num_io++;
      break;
    }
    case OpType::ZeroAndTruncate: {
      start_io();
      ZeroAndTruncateOp& ztOp = static_cast<ZeroAndTruncateOp&>(op);
      auto op_info = std::make_shared<AsyncOpInfo<0>>();
      librados::ObjectWriteOperation wop;
      wop.zero(ztOp.zero_offset * block_size, ztOp.zero_length * block_size);
      wop.truncate(ztOp.truncate_size * block_size);
      auto zeroandtruncate_cb = [this](boost::system::error_code ec, version_t ver) {
        ceph_assert(ec == boost::system::errc::success);
        finish_io();
      };
      librados::async_operate(asio.get_executor(), io, primary_oid,
                              std::move(wop), 0, nullptr, zeroandtruncate_cb);
      num_io++;
      break;
    }
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
                                 op_info->length[i], pool,
                                 seq ? seq->get_id() : Sequence::SEQUENCE_END,
                                 seq ? seq->get_step() : -1));
      }
      finish_io();
    };

    int flags = 0;
    if (readOp.balanced_read.has_value()) {
      if (*readOp.balanced_read) {
        flags = librados::OPERATION_BALANCE_READS;
      }
      // Else: keep flags == 0
    } else {
      ceph_assert(balanced_read_percentage >= 0);
      ceph_assert(balanced_read_percentage <= 100);
      uint64_t range = 100;
      uint64_t rand_value = rng();
      int index = rand_value % range;
      if (index <= balanced_read_percentage) {
        flags = librados::OPERATION_BALANCE_READS;
      }
    }
    librados::async_operate(asio.get_executor(), io, primary_oid,
                            std::move(rop), flags, nullptr, read_cb);
    num_io++;
  };

  auto applyZeroOp = [this]<OpType opType, int N>(
                         ReadWriteOp<opType, N> zeroOp) {
    auto op_info =
        std::make_shared<AsyncOpInfo<N>>(zeroOp.offset, zeroOp.length);
    librados::ObjectWriteOperation wop;
    for (int i = 0; i < N; i++) {
      wop.zero(zeroOp.offset[i] * block_size,
               zeroOp.length[i] * block_size);
    }
    auto zero_cb = [this](boost::system::error_code ec, version_t ver) {
      ceph_assert(ec == boost::system::errc::success);
      finish_io();
    };
    librados::async_operate(asio.get_executor(), io, primary_oid,
                            std::move(wop), 0, nullptr, zero_cb);
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
    librados::async_operate(asio.get_executor(), io, primary_oid,
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
    librados::async_operate(asio.get_executor(), io, primary_oid,
                            std::move(wop), 0, nullptr, write_cb);
    num_io++;
  };

  auto applyMapextOp = [this]<OpType opType, int N>(
                           ReadWriteOp<opType, N> mop) {
    // Shared state kept alive until the async callback fires.
    struct MapextOpState {
      std::map<uint64_t, uint64_t> actual_map;
      std::array<uint64_t, N> offset;
      std::array<uint64_t, N> length;
      int prval{0};
    };
    auto state = std::make_shared<MapextOpState>();
    state->offset = mop.offset;
    state->length = mop.length;

    librados::ObjectReadOperation rop;
    rop.mapext(mop.offset[0] * block_size, mop.length[0] * block_size,
               &state->actual_map, &state->prval);

    auto mapext_cb = [this, state](boost::system::error_code ec,
                                   version_t ver, bufferlist bl) {
      ceph_assert(ec == boost::system::errc::success);
      ceph_assert(state->prval >= 0);

      // Compute expected extent map from the model and filter to the
      // queried [offset_bytes, offset_bytes+length_bytes) range.
      const uint64_t off_bytes = state->offset[0] * block_size;
      const uint64_t end_bytes = off_bytes + state->length[0] * block_size;
      std::map<uint64_t, uint64_t> expected;
      for (auto& [ext_off, ext_len] : om->get_expected_extent_map()) {
        const uint64_t ext_end = ext_off + ext_len;
        if (ext_off >= end_bytes || ext_end <= off_bytes) {
          continue;  // outside query range
        }
        const uint64_t clamp_off = std::max(ext_off, off_bytes);
        const uint64_t clamp_end = std::min(ext_end, end_bytes);
        expected.emplace(clamp_off, clamp_end - clamp_off);
      }

      if (state->actual_map != expected) {
        auto fmt_extents = [](const std::map<uint64_t, uint64_t>& m) {
          if (m.empty()) return std::string("(none)");
          std::string s;
          for (auto& [o, l] : m) {
            if (!s.empty()) s += ", ";
            s += fmt::format("[{}+{})", o, l);
          }
          return s;
        };

        std::string msg;
        msg += "Mapext mismatch!\n";
        msg += fmt::format("  Query range : [{}+{})\n",
                           off_bytes, state->length[0] * block_size);
        msg += fmt::format("  Actual   ({}): {}\n",
                           state->actual_map.size(),
                           fmt_extents(state->actual_map));
        msg += fmt::format("  Expected ({}): {}\n",
                           expected.size(),
                           fmt_extents(expected));

        // Show extents present in actual but not in expected (unexpected)
        std::map<uint64_t, uint64_t> unexpected;
        for (auto& [o, l] : state->actual_map) {
          if (expected.find(o) == expected.end() || expected.at(o) != l) {
            unexpected.emplace(o, l);
          }
        }
        if (!unexpected.empty()) {
          msg += fmt::format("  Unexpected extents (in actual, not in expected): {}\n",
                             fmt_extents(unexpected));
        }

        // Show extents present in expected but not in actual (missing)
        std::map<uint64_t, uint64_t> missing;
        for (auto& [o, l] : expected) {
          if (state->actual_map.find(o) == state->actual_map.end() ||
              state->actual_map.at(o) != l) {
            missing.emplace(o, l);
          }
        }
        if (!missing.empty()) {
          msg += fmt::format("  Missing extents (in expected, not in actual): {}\n",
                             fmt_extents(missing));
        }

        lderr(g_ceph_context) << msg << dendl;
        ceph_abort_msg("Mapext result does not match expected extent map");
      }

      finish_io();
    };

    librados::async_operate(asio.get_executor(), io, primary_oid,
                            std::move(rop), 0, nullptr, mapext_cb);
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
    case OpType::Zero: {
      start_io();
      ZeroOp& zeroOp = static_cast<ZeroOp&>(op);
      applyZeroOp(zeroOp);
      break;
    }
    case OpType::Zero2: {
      start_io();
      DoubleZeroOp& zeroOp = static_cast<DoubleZeroOp&>(op);
      applyZeroOp(zeroOp);
      break;
    }
    case OpType::WriteZeroData: {
      start_io();
      WriteZeroDataOp& wzdOp = static_cast<WriteZeroDataOp&>(op);
      auto op_info = std::make_shared<AsyncOpInfo<1>>(wzdOp.offset, wzdOp.length);
      op_info->bufferlist[0].append_zero(wzdOp.length[0] * block_size);
      librados::ObjectWriteOperation wop;
      wop.write(wzdOp.offset[0] * block_size, op_info->bufferlist[0]);
      auto writezerodata_cb = [this](boost::system::error_code ec, version_t ver) {
        ceph_assert(ec == boost::system::errc::success);
        finish_io();
      };
      librados::async_operate(asio.get_executor(), io, primary_oid,
                              std::move(wop), 0, nullptr, writezerodata_cb);
      num_io++;
      break;
    }
    case OpType::Mapext: {
      start_io();
      MapextOp& mop = static_cast<MapextOp&>(op);
      applyMapextOp(mop);
      break;
    }

    default:
      ceph_abort_msg(
          fmt::format("Unsupported Read/Write operation ({})", op.getOpType()));
      break;
  }
}

void RadosIo::applyTruncateWriteOp(IoOp& op) {
  auto applyTruncateWriteOp = [this]<OpType opType, int N>(
                                  TruncateWriteOp<opType, N> truncWriteOp) {
    auto op_info =
        std::make_shared<AsyncOpInfo<N>>(truncWriteOp.offset, truncWriteOp.length);
    librados::ObjectWriteOperation wop;

    // First, add the truncate operation
    wop.truncate(truncWriteOp.size * block_size);

    // Then, add the write operations
    for (int i = 0; i < N; i++) {
      op_info->bufferlist[i] =
          db->generate_data(truncWriteOp.offset[i], truncWriteOp.length[i]);
      wop.write(truncWriteOp.offset[i] * block_size,
                op_info->bufferlist[i]);
    }

    auto write_cb = [this](boost::system::error_code ec, version_t ver) {
      ceph_assert(ec == boost::system::errc::success);
      finish_io();
    };
    librados::async_operate(asio.get_executor(), io, primary_oid,
                            std::move(wop), 0, nullptr, write_cb);
    num_io++;
  };

  switch (op.getOpType()) {
    case OpType::TruncateWrite: {
      start_io();
      SingleTruncateWriteOp& truncWriteOp = static_cast<SingleTruncateWriteOp&>(op);
      applyTruncateWriteOp(truncWriteOp);
      break;
    }
    case OpType::TruncateWrite2: {
      start_io();
      DoubleTruncateWriteOp& truncWriteOp = static_cast<DoubleTruncateWriteOp&>(op);
      applyTruncateWriteOp(truncWriteOp);
      break;
    }
    case OpType::TruncateWrite3: {
      start_io();
      TripleTruncateWriteOp& truncWriteOp = static_cast<TripleTruncateWriteOp&>(op);
      applyTruncateWriteOp(truncWriteOp);
      break;
    }
    default:
      ceph_abort_msg(
          fmt::format("Unsupported TruncateWrite operation ({})", op.getOpType()));
      break;
  }
}

void RadosIo::applyInjectOp(IoOp& op) {
  bufferlist osdmap_outbl, inject_outbl;
  auto formatter = std::make_unique<JSONFormatter>(false);

  int osd = -1;
  std::vector<int> shard_order;

  ceph::messaging::osd::OSDMapRequest osdMapRequest{pool, get_primary_oid(), ""};
  int rc = send_mon_command(osdMapRequest, rados, "OSDMapRequest", {},
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
            injectErrorRequest{pool,         primary_oid,          errorOp.shard,
                               errorOp.type, errorOp.when, errorOp.duration};
        int rc = send_osd_command(osd, injectErrorRequest, rados,
                                  "InjectECErrorRequest", {},
                                  &inject_outbl, formatter.get());
        ceph_assert(rc == 0);
      } else if (errorOp.type == 1) {
        ceph::messaging::osd::InjectECErrorRequest<
            InjectOpType::ReadMissingShard>
            injectErrorRequest{pool,         primary_oid,          errorOp.shard,
                               errorOp.type, errorOp.when, errorOp.duration};
        int rc = send_osd_command(osd, injectErrorRequest, rados,
                                  "InjectECErrorRequest", {},
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
            injectErrorRequest{pool,         primary_oid,          errorOp.shard,
                               errorOp.type, errorOp.when, errorOp.duration};
        int rc = send_osd_command(osd, injectErrorRequest, rados,
                                  "InjectECErrorRequest", {},
                                  &inject_outbl, formatter.get());
        ceph_assert(rc == 0);
      } else if (errorOp.type == 3) {
        ceph::messaging::osd::InjectECErrorRequest<InjectOpType::WriteOSDAbort>
            injectErrorRequest{pool,         primary_oid,          errorOp.shard,
                               errorOp.type, errorOp.when, errorOp.duration};
        int rc = send_osd_command(osd, injectErrorRequest, rados,
                                  "InjectECErrorRequest", {},
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
            clearErrorInject{pool, primary_oid, errorOp.shard, errorOp.type};
        int rc = send_osd_command(osd, clearErrorInject, rados,
                                  "InjectECClearErrorRequest", {},
                                  &inject_outbl, formatter.get());
        ceph_assert(rc == 0);
      } else if (errorOp.type == 1) {
        ceph::messaging::osd::InjectECClearErrorRequest<
            InjectOpType::ReadMissingShard>
            clearErrorInject{pool, primary_oid, errorOp.shard, errorOp.type};
        int rc = send_osd_command(osd, clearErrorInject, rados,
                                  "InjectECClearErrorRequest", {},
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
            clearErrorInject{pool, primary_oid, errorOp.shard, errorOp.type};
        int rc = send_osd_command(osd, clearErrorInject, rados,
                                  "InjectECClearErrorRequest", {},
                                  &inject_outbl, formatter.get());
        ceph_assert(rc == 0);
      } else if (errorOp.type == 3) {
        ceph::messaging::osd::InjectECClearErrorRequest<
            InjectOpType::WriteOSDAbort>
            clearErrorInject{pool, primary_oid, errorOp.shard, errorOp.type};
        int rc = send_osd_command(osd, clearErrorInject, rados,
                                  "InjectECClearErrorRequest", {},
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
