// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/spdk_nvme_io.h"

#include <cerrno>
#include <chrono>
#include <map>
#include <mutex>
#include <stdexcept>

#include <cstring>

#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>

#include <spdk/env.h>
#include <spdk/nvme.h>
#include <spdk/version.h>

#include "include/ceph_assert.h"
#include "include/buffer.h"
#include "crimson/common/config_proxy.h"
#include "crimson/common/errorator-utils.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/spdk_dma_buffer.h"

SET_SUBSYS(seastore_device);

namespace crimson::os::seastore {

namespace {

// SPDK environment is process-wide and must be initialized exactly once.
std::once_flag env_once;

void ensure_env()
{
  std::call_once(env_once, [] {
    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    opts.name = "crimson-seastore";
    // IOVA mode. Left empty by default so DPDK auto-detects: virtual-address
    // (VA) when an IOMMU is present (the recommended vfio-pci production setup),
    // physical-address (PA) otherwise. PA needs privileged access to physical
    // addresses; environments without an IOMMU that run unprivileged (e.g. an
    // NVMe-oF/TCP initiator) can force "va" via seastore_spdk_iova_mode.
    auto iova_mode = crimson::common::local_conf().get_val<std::string>(
      "seastore_spdk_iova_mode");
    if (!iova_mode.empty()) {
      opts.iova_mode = iova_mode.c_str();
    }
    if (spdk_env_init(&opts) < 0) {
      throw std::runtime_error("spdk_env_init failed");
    }
  });
}

// spdk_nvme_connect_async() passes the attach callback no user context (cb_ctx
// is the opts pointer), so stash the freshly-attached controller in a per-shard
// slot that the callback fills synchronously from within
// spdk_nvme_probe_poll_async() on the same reactor thread.
thread_local spdk_nvme_ctrlr* tls_attached_ctrlr = nullptr;
void connect_attach_cb(void*, const spdk_nvme_transport_id*,
                       spdk_nvme_ctrlr* ctrlr, const spdk_nvme_ctrlr_opts*)
{
  tls_attached_ctrlr = ctrlr;
}


// A single NVMe controller is attached once per process, then shared across all
// Seastar shards (each shard owns its own qpair, not its own controller). The
// per-shard spdk_nvme_io driver instances therefore resolve the controller
// through this registry rather than each calling spdk_nvme_probe().
struct shared_ctrlr {
  spdk_nvme_ctrlr* ctrlr = nullptr;
  spdk_nvme_ns* ns = nullptr;
  uint32_t sector_size = 0;
  uint32_t nsid = 0;
  unsigned refcount = 0;
  // Single-flight gate: exactly one shard connects an endpoint; peers wait for
  // the registry entry instead of racing into a second concurrent connect. The
  // vfio-user transport exposes a single controller, and a concurrent
  // spdk_nvme_connect_async() to it deadlocks in the version handshake.
  bool connecting = false;
};
std::mutex registry_mutex;
std::map<std::string, shared_ctrlr> registry;

std::mutex admin_mutex;

} // anonymous namespace

spdk_nvme_io::~spdk_nvme_io()
{
  // qpair/poller must already be released via stop_shard(); the controller via
  // detach(). Nothing async to do here.
}

seastar::future<> spdk_nvme_io::probe(const std::string& trid_str)
{
  LOG_PREFIX(spdk_nvme_io::probe);
  transport_id = trid_str;

  auto adopt = [this](shared_ctrlr& entry) {
    ++entry.refcount;
    ctrlr = entry.ctrlr;
    ns = entry.ns;
    sector_size = entry.sector_size;
    nsid = entry.nsid;
  };

  // Serialize connects per endpoint: the vfio-user transport exposes a single
  // controller and a concurrent spdk_nvme_connect_async() to it deadlocks in
  // the version handshake, so only one shard may connect at a time. Each shard
  // loops: adopt an already-attached controller, become the sole connector, or
  // wait 1ms and re-evaluate. Re-evaluation is essential — at mkfs each shard
  // closes (and the last detach erases the registry entry) right after its
  // work, so a connector can attach, register, AND vanish before a peer ever
  // observes it. A waiter that finds no controller and no connect in flight
  // must therefore become the connector itself rather than wait forever.
  while (true) {
    {
      std::lock_guard l(registry_mutex);
      auto& entry = registry[trid_str];
      if (entry.ctrlr != nullptr) {
        adopt(entry);
        co_return;
      }
      if (!entry.connecting) {
        entry.connecting = true;   // claim: this shard is the sole connector
        break;
      }
      // else: another shard is connecting — fall through to wait + re-check.
    }
    co_await seastar::sleep(std::chrono::milliseconds(1));
  }

  // On failure, erase the registry entry entirely rather than leaving a sticky
  // "failed" flag: a waiting peer -- or a later probe() after a transient
  // failure -- then re-evaluates from scratch and may retry, instead of the
  // endpoint being poisoned for the lifetime of the process.
  auto abandon_connect = [trid_str] {
    std::lock_guard l(registry_mutex);
    registry.erase(trid_str);
  };

  // First shard to reach this controller: initialize env and attach.
  ensure_env();

  struct spdk_nvme_transport_id trid = {};
  // Accept a bare PCI address ("0000:01:00.0") or a full SPDK transport id
  // ("trtype:PCIe traddr:0000:01:00.0").
  std::string parse_str = trid_str;
  if (parse_str.find("trtype:") == std::string::npos) {
    parse_str = "trtype:PCIe traddr:" + trid_str;
  }
  if (spdk_nvme_transport_id_parse(&trid, parse_str.c_str()) < 0) {
    ERROR("failed to parse transport id '{}'", parse_str);
    abandon_connect();
    throw std::runtime_error("spdk_nvme_transport_id_parse failed");
  }

  // Connect asynchronously and drive it from the reactor: spdk_nvme_connect()
  // is a synchronous ~1s call for fabrics (TCP/vfio-user) at mount. Running it
  // off-reactor on a raw std::thread is not safe (the thread is not a DPDK
  // lcore / spdk_thread, so the fabrics connect hangs). Instead kick off the
  // async probe and poll it cooperatively, yielding the reactor between polls.
  tls_attached_ctrlr = nullptr;
  auto* probe_ctx = spdk_nvme_connect_async(&trid, nullptr, connect_attach_cb);
  if (probe_ctx == nullptr) {
    ERROR("spdk_nvme_connect_async failed for '{}'", parse_str);
    abandon_connect();
    throw std::runtime_error("spdk_nvme_connect_async failed");
  }

  co_await seastar::repeat([&probe_ctx] {
    // 0 = complete (SPDK frees probe_ctx), <0 = error, -EAGAIN = in progress.
    int rc = spdk_nvme_probe_poll_async(probe_ctx);
    if (rc != -EAGAIN) {
      return seastar::make_ready_future<seastar::stop_iteration>(
        seastar::stop_iteration::yes);
    }
    return seastar::sleep(std::chrono::milliseconds(1)).then([] {
      return seastar::stop_iteration::no;
    });
  });

  spdk_nvme_ctrlr* new_ctrlr = tls_attached_ctrlr;
  tls_attached_ctrlr = nullptr;

  std::lock_guard l(registry_mutex);
  auto& entry = registry[trid_str];
  if (new_ctrlr == nullptr) {
    ERROR("spdk_nvme_connect_async failed for '{}'", parse_str);
    registry.erase(trid_str);   // invalidates `entry`; we throw immediately
    throw std::runtime_error("spdk_nvme_connect failed");
  }
  uint32_t new_nsid = spdk_nvme_ctrlr_get_first_active_ns(new_ctrlr);
  if (new_nsid == 0) {
    ERROR("no active namespace on '{}'", parse_str);
    spdk_nvme_detach(new_ctrlr);
    registry.erase(trid_str);   // invalidates `entry`; we throw immediately
    throw std::runtime_error("no active nvme namespace");
  }
  entry.connecting = false;
  entry.ctrlr = new_ctrlr;
  entry.ns = spdk_nvme_ctrlr_get_ns(new_ctrlr, new_nsid);
  entry.sector_size = spdk_nvme_ns_get_sector_size(entry.ns);
  entry.nsid = new_nsid;
  INFO("attached '{}' nsid={} sector_size={} size={}",
       parse_str, new_nsid, entry.sector_size, spdk_nvme_ns_get_size(entry.ns));
  adopt(entry);
  co_return;
}

seastar::future<> spdk_nvme_io::detach()
{
  std::lock_guard l(registry_mutex);
  auto it = registry.find(transport_id);
  if (it != registry.end() && --it->second.refcount == 0) {
    spdk_nvme_detach(it->second.ctrlr);
    registry.erase(it);
  }
  ctrlr = nullptr;
  ns = nullptr;
  return seastar::make_ready_future<>();
}

void spdk_nvme_io::start_shard()
{
  ceph_assert(ctrlr);
  owning_shard = seastar::this_shard_id();
  qpair = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, nullptr, 0);
  ceph_assert(qpair);
  poller = seastar::reactor::poller::simple([this] {
    return spdk_nvme_qpair_process_completions(qpair, 0) > 0;
  });
  if (owning_shard == 0) {
    admin_timer.emplace();
    admin_timer->set_callback([this] {
      std::unique_lock l(admin_mutex, std::try_to_lock);
      if (l.owns_lock()) {
        spdk_nvme_ctrlr_process_admin_completions(ctrlr);
      }
    });
    admin_timer->arm_periodic(std::chrono::milliseconds(500));
  }
}

void spdk_nvme_io::stop_shard()
{
  poller.reset();
  if (admin_timer) {
    admin_timer->cancel();
    admin_timer.reset();
  }
  if (qpair) {
    spdk_nvme_ctrlr_free_io_qpair(qpair);
    qpair = nullptr;
  }
}

void spdk_nvme_io::io_complete(void* arg, const spdk_nvme_cpl* cpl)
{
  auto* pr = static_cast<seastar::promise<>*>(arg);
  if (spdk_nvme_cpl_is_error(cpl)) {
    pr->set_exception(std::runtime_error("spdk nvme I/O error"));
  } else {
    pr->set_value();
  }
  delete pr;
}

seastar::future<> spdk_nvme_io::submit_io(
  bool is_write, uint64_t offset, void* buf, size_t len, uint32_t io_flags)
{
  ceph_assert(qpair);
  uint64_t lba = offset / sector_size;
  uint32_t lba_count = len / sector_size;
  auto* pr = new seastar::promise<>();
  auto fut = pr->get_future();
  return seastar::repeat(
    [this, is_write, buf, lba, lba_count, io_flags, pr] {
      int rc = is_write
        ? spdk_nvme_ns_cmd_write(ns, qpair, buf, lba, lba_count,
                                 &spdk_nvme_io::io_complete, pr, io_flags)
        : spdk_nvme_ns_cmd_read(ns, qpair, buf, lba, lba_count,
                                &spdk_nvme_io::io_complete, pr, io_flags);
      if (rc == -ENOMEM) {
        // qpair request pool exhausted — deep cleaner/journal-trim bursts can
        // exceed the queue depth. Queue-full is flow control, not an error:
        // drain completions and resubmit.
        spdk_nvme_qpair_process_completions(qpair, 0);
        return seastar::yield().then([] {
          return seastar::stop_iteration::no;
        });
      }
      if (rc != 0) {
        pr->set_exception(std::make_exception_ptr(
          std::runtime_error("spdk_nvme_ns_cmd submit failed")));
        delete pr;
      }
      return seastar::make_ready_future<seastar::stop_iteration>(
        seastar::stop_iteration::yes);
    }
  ).then([fut = std::move(fut)] () mutable {
    return std::move(fut);
  });
}

spdk_nvme_io::io_ertr::future<> spdk_nvme_io::do_io(
  bool is_write, uint64_t offset, char* data, size_t len, uint32_t io_flags)
{
  LOG_PREFIX(spdk_nvme_io::do_io);
  ceph_assert(seastar::this_shard_id() == owning_shard);

  if (len == 0) {
    return io_ertr::now();
  }

  static thread_local uint64_t success_count = 0;
  static thread_local uint64_t fallback_count = 0;

  if (spdk_vtophys(data, nullptr) != SPDK_VTOPHYS_ERROR) {
    success_count++;
    // Already DMA-safe (e.g. allocated via raw_spdk_dma) — submit directly.
    return submit_io(is_write, offset, data, len, io_flags
    ).handle_exception([FNAME, offset, len](auto e) -> io_ertr::future<> {
      ERROR("poffset=0x{:x}~0x{:x} spdk io error -- {}", offset, len, e);
      return crimson::ct_error::input_output_error::make();
    }).then([] () -> io_ertr::future<> {
      return io_ertr::now();
    });
  }

  fallback_count++;
  if (fallback_count % 1000 == 1) {
    WARN("do_io: non-DMA-safe buffer detected at offset 0x{:x}~0x{:x}; "
         "falling back to bounce buffer (fallbacks: {}, successes: {})",
         offset, len, fallback_count, success_count);
  }

  // Bounce through a hugepage buffer.
  return seastar::do_with(
    bufferptr(create_spdk_dma(len)),
    [this, is_write, offset, data, len, io_flags, FNAME](bufferptr& bounce)
  {
    if (is_write) {
      std::memcpy(bounce.c_str(), data, len);
    }
    return submit_io(is_write, offset, bounce.c_str(), len, io_flags
    ).handle_exception([FNAME, offset, len](auto e) -> io_ertr::future<> {
      ERROR("poffset=0x{:x}~0x{:x} spdk io error -- {}", offset, len, e);
      return crimson::ct_error::input_output_error::make();
    }).then([is_write, data, len, &bounce] () -> io_ertr::future<> {
      if (!is_write) {
        std::memcpy(data, bounce.c_str(), len);
      }
      return io_ertr::now();
    });
  });
}

namespace {
// Per-command SGL iteration state for spdk_nvme_ns_cmd_writev. `segs` is one
// command's ordered list of sector-aligned, DMA-safe (ptr,len) segments. Every
// segment length is a sector multiple — NVMe SGL rejects sub-sector segments.
// The buffers the segments point into (the source bufferlist referenced
// zero-copy, plus any coalesced hugepage buffers) are owned by the do_writev
// state that outlives all commands, not by this ctx. Heap-owned; freed in the
// completion callback.
struct writev_ctx {
  std::vector<std::pair<char*, uint32_t>> segs;
  size_t idx = 0;
  uint32_t seg_off = 0;
  seastar::promise<> pr;
};

void writev_reset_sgl(void* arg, uint32_t offset)
{
  auto* c = static_cast<writev_ctx*>(arg);
  c->idx = 0;
  uint32_t rem = offset;
  while (c->idx < c->segs.size() && rem >= c->segs[c->idx].second) {
    rem -= c->segs[c->idx].second;
    ++c->idx;
  }
  c->seg_off = rem;
}

int writev_next_sge(void* arg, void** address, uint32_t* length)
{
  auto* c = static_cast<writev_ctx*>(arg);
  if (c->idx >= c->segs.size()) {
    *address = nullptr;
    *length = 0;
    return 0;
  }
  auto& s = c->segs[c->idx];
  *address = s.first + c->seg_off;
  *length = s.second - c->seg_off;
  ++c->idx;
  c->seg_off = 0;
  return 0;
}

void writev_complete(void* arg, const spdk_nvme_cpl* cpl)
{
  auto* c = static_cast<writev_ctx*>(arg);
  if (spdk_nvme_cpl_is_error(cpl)) {
    c->pr.set_exception(std::make_exception_ptr(
      std::runtime_error("spdk nvme writev error")));
  } else {
    c->pr.set_value();
  }
  delete c;
}
}

seastar::future<> spdk_nvme_io::submit_writev(
  void* writev_ctx_v, uint64_t lba, uint32_t lba_count, uint32_t io_flags)
{
  ceph_assert(qpair);
  auto* ctx = static_cast<writev_ctx*>(writev_ctx_v);
  auto fut = ctx->pr.get_future();
  return seastar::repeat(
    [this, ctx, lba, lba_count, io_flags] {
      int rc = spdk_nvme_ns_cmd_writev(
        ns, qpair, lba, lba_count, &writev_complete, ctx, io_flags,
        &writev_reset_sgl, &writev_next_sge);
      if (rc == -ENOMEM) {
        // qpair request pool exhausted — same flow control as submit_io().
        spdk_nvme_qpair_process_completions(qpair, 0);
        return seastar::yield().then([] {
          return seastar::stop_iteration::no;
        });
      }
      if (rc != 0) {
        ctx->pr.set_exception(std::make_exception_ptr(
          std::runtime_error("spdk_nvme_ns_cmd_writev submit failed")));
        delete ctx;
      }
      return seastar::make_ready_future<seastar::stop_iteration>(
        seastar::stop_iteration::yes);
    }
  ).then([fut = std::move(fut)] () mutable {
    return std::move(fut);
  });
}

spdk_nvme_io::io_ertr::future<> spdk_nvme_io::do_writev(
  uint64_t offset, ceph::bufferlist&& bl, uint32_t io_flags)
{
  LOG_PREFIX(spdk_nvme_io::do_writev);
  ceph_assert(seastar::this_shard_id() == owning_shard);

  size_t len = bl.length();
  if (len == 0) {
    return io_ertr::now();
  }
  ceph_assert(len % sector_size == 0);

  ceph_assert(ctrlr != nullptr);
#if SPDK_VERSION >= SPDK_VERSION_NUM(24, 9, 0)
  const uint16_t max_sges = spdk_nvme_ctrlr_get_max_sges(ctrlr);
#else
  // Older SPDK (e.g. the built-in 20.x) has no public max_sges accessor, and
  // the field lives in the private struct spdk_nvme_ctrlr (nvme_internal.h) --
  // reaching into it is UB and breaks across versions/build configs. Default to
  // the safe, deadlock-free value of 1: every contiguous run becomes its own
  // command. That matches what old SPDK reports anyway (its TCP and vfio-user
  // transports both cap at 1); modern TCP reports 16, so this just costs a few
  // extra commands there while staying correct.
  const uint16_t max_sges = 1;
#endif

  // Pack the record's SGL into commands of at most max_sges segments each.
  // Bounding the segment count is what keeps SPDK from splitting a command
  // across more SGL descriptors than the transport supports -- the internal
  // splitting that exhausts the request pool and deadlocks under load. vfio-user
  // reports max_sges==1 (so each contiguous run is its own command -- a 1 MB
  // extent rides one command instead of 256 sector-sized ones); NVMe/TCP reports
  // 1 on older SPDK and 16 on recent; PCIe reports ~250, so a whole record's SGL
  // rides a single multi-segment command.
  struct command_t {
    uint64_t off;
    std::vector<std::pair<char*, uint32_t>> segs;
    uint32_t bytes = 0;
  };
  // Owns everything the in-flight commands reference: the source bufferlist
  // (zero-copy segments point into it) and the coalesced hugepage buffers
  // (sub-sector / non-DMA runs). Outlives all commands.
  struct state_t {
    ceph::bufferlist keep;
    std::vector<ceph::bufferptr> coalesced;
    std::vector<command_t> cmds;
  };
  auto state = seastar::make_lw_shared<state_t>();
  state->keep = bl;

  uint64_t run_off = offset;   // device offset of the next byte to place
  command_t cur;
  cur.off = offset;
  auto flush_cmd = [&]() {
    if (cur.segs.empty()) {
      return;
    }
    state->cmds.push_back(std::move(cur));
    cur = command_t{};
    cur.off = run_off;
  };
  auto add_seg = [&](char* ptr, uint32_t slen) {
    if (cur.segs.size() == max_sges) {
      flush_cmd();
    }
    cur.segs.emplace_back(ptr, slen);
    cur.bytes += slen;
    run_off += slen;
  };

  // seastore lays records out as [block-aligned metadata][block-aligned data],
  // so each data extent already starts on a sector boundary and is referenced
  // in place (zero-copy); the sub-sector encoded-metadata fragments preceding
  // it are coalesced into one sector-aligned hugepage buffer. NVMe SGL rejects
  // sub-sector segments, so coalescing to sector granularity is what makes the
  // submit valid. On-disk bytes are unchanged.
  size_t cum = 0;
  std::vector<std::pair<char*, uint32_t>> pending;  // current sub-sector run
  size_t pending_len = 0;
  auto flush_pending = [&]() {
    if (pending_len == 0) {
      return;
    }
    ceph_assert(pending_len % sector_size == 0);
    ceph::bufferptr b(create_spdk_dma(pending_len));
    size_t off = 0;
    for (auto& [p, l] : pending) {
      std::memcpy(b.c_str() + off, p, l);
      off += l;
    }
    add_seg(b.c_str(), (uint32_t)pending_len);
    state->coalesced.push_back(std::move(b));
    pending.clear();
    pending_len = 0;
  };
  for (const auto& frag : bl.buffers()) {
    if (frag.length() == 0) {
      continue;
    }
    char* a = const_cast<char*>(frag.c_str());
    const bool passthrough =
      pending_len == 0 &&
      (cum % sector_size) == 0 &&
      (frag.length() % sector_size) == 0 &&
      spdk_vtophys(a, nullptr) != SPDK_VTOPHYS_ERROR;
    if (passthrough) {
      add_seg(a, frag.length());
    } else {
      pending.emplace_back(a, frag.length());
      pending_len += frag.length();
    }
    cum += frag.length();
    if (pending_len != 0 && (cum % sector_size) == 0) {
      flush_pending();
    }
  }
  flush_pending();
  flush_cmd();
  ceph_assert(cum == len);
  ceph_assert(pending_len == 0);

  return io_ertr::parallel_for_each(
    state->cmds,
    [this, io_flags, FNAME](const command_t& c) -> io_ertr::future<> {
      // A single-segment command is a plain contiguous write -- skip the SGL
      // callback machinery (this is every command on NVMe/TCP).
      if (c.segs.size() == 1) {
        return submit_io(true, c.off, c.segs[0].first, c.segs[0].second,
                         io_flags
        ).handle_exception([FNAME, off = c.off, bytes = c.bytes](auto e)
                           -> io_ertr::future<> {
          ERROR("poffset=0x{:x}~0x{:x} spdk io error -- {}", off, bytes, e);
          return crimson::ct_error::input_output_error::make();
        }).then([] () -> io_ertr::future<> {
          return io_ertr::now();
        });
      }
      auto ctx = std::make_unique<writev_ctx>();
      ctx->segs = c.segs;
      uint64_t lba = c.off / sector_size;
      uint32_t lba_count = c.bytes / sector_size;
      return submit_writev(ctx.release(), lba, lba_count, io_flags
      ).handle_exception([FNAME, off = c.off, bytes = c.bytes](auto e)
                         -> io_ertr::future<> {
        ERROR("poffset=0x{:x}~0x{:x} spdk writev error -- {}", off, bytes, e);
        return crimson::ct_error::input_output_error::make();
      }).then([] () -> io_ertr::future<> {
        return io_ertr::now();
      });
    }
  ).safe_then([state] {
    return io_ertr::now();
  });
}

namespace {
// Completion callback for raw admin/io commands: stores the status (0 ok, -1 err).
void raw_complete(void* arg, const struct spdk_nvme_cpl* cpl)
{
  *static_cast<int*>(arg) = spdk_nvme_cpl_is_error(cpl) ? -1 : 0;
}
} // anonymous namespace

seastar::future<int> spdk_nvme_io::admin_raw(
  spdk_nvme_cmd& cmd, void* buf, size_t len)
{
  auto result = seastar::make_lw_shared<int>(1);  // 1 == not-yet-complete
  int rc;
  {
    std::lock_guard l(admin_mutex);
    rc = spdk_nvme_ctrlr_cmd_admin_raw(ctrlr, &cmd, buf, (uint32_t)len,
                                       raw_complete, result.get());
  }
  if (rc != 0) {
    return seastar::make_ready_future<int>(-1);
  }
  // Bound the wait: a controller that never completes the command must not hang
  // the operation (and thus mount) forever.
  const auto deadline =
    std::chrono::steady_clock::now() + std::chrono::seconds(30);
  return seastar::do_until(
    [result, deadline] {
      return *result != 1 ||
             std::chrono::steady_clock::now() >= deadline;
    },
    [this] {
      {
        std::unique_lock l(admin_mutex, std::try_to_lock);
        if (l.owns_lock()) {
          spdk_nvme_ctrlr_process_admin_completions(ctrlr);
        }
      }
      return seastar::yield();
    }
  ).then([result] {
    if (*result == 1) {
      // Timed out with the command still outstanding: SPDK may yet complete it
      // and write through result.get(), so pin result (a tiny leak on this
      // fatal, dead-controller path) rather than risk a use-after-free.
      new seastar::lw_shared_ptr<int>(result);
      return -1;
    }
    return *result;
  });
}

seastar::future<int> spdk_nvme_io::io_raw(
  spdk_nvme_cmd& cmd, void* buf, size_t len)
{
  ceph_assert(qpair);
  auto result = seastar::make_lw_shared<int>(1);
  return seastar::repeat([this, &cmd, buf, len, result] {
    int rc = spdk_nvme_ctrlr_cmd_io_raw(ctrlr, qpair, &cmd, buf, (uint32_t)len,
                                        raw_complete, result.get());
    if (rc == -ENOMEM) {
      // Queue-full is flow control, not an error: drain completions and retry.
      spdk_nvme_qpair_process_completions(qpair, 0);
      return seastar::yield().then([] {
        return seastar::stop_iteration::no;
      });
    }
    if (rc != 0) {
      *result = -1;
    }
    return seastar::make_ready_future<seastar::stop_iteration>(
      seastar::stop_iteration::yes);
  }).then([this, result] {
    if (*result == -1) {
      return seastar::make_ready_future<int>(-1);
    }
    // The shard poller drives spdk_nvme_qpair_process_completions(). Bound the
    // wait so a controller that never completes the command cannot hang forever.
    const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(30);
    return seastar::do_until(
      [result, deadline] {
        return *result != 1 ||
               std::chrono::steady_clock::now() >= deadline;
      },
      [] { return seastar::yield(); }
    ).then([result] {
      if (*result == 1) {
        // Outstanding at timeout: pin result (tiny fatal-path leak) so a late
        // completion writing through result.get() is not a use-after-free.
        new seastar::lw_shared_ptr<int>(result);
        return -1;
      }
      return *result;
    });
  });
}

uint64_t spdk_nvme_io::size_bytes() const
{
  return spdk_nvme_ns_get_size(ns);
}

uint32_t spdk_nvme_io::block_size() const
{
  return sector_size;
}

}
