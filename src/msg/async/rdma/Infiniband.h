// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 XSKY <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_INFINIBAND_H
#define CEPH_INFINIBAND_H

#include <boost/pool/pool.hpp>
// need this because boost messes with ceph log/assert definitions
#include "include/ceph_assert.h"

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#include <atomic>
#include <functional>
#include <string>
#include <vector>

#include "include/int_types.h"
#include "include/page.h"
#include "include/scope_guard.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/ceph_mutex.h"
#include "common/perf_counters.h"
#include "msg/msg_types.h"
#include "msg/async/net_handler.h"

#define HUGE_PAGE_SIZE_2MB (2 * 1024 * 1024)
#define ALIGN_TO_PAGE_2MB(x) \
    (((x) + (HUGE_PAGE_SIZE_2MB - 1)) & ~(HUGE_PAGE_SIZE_2MB - 1))

#define PSN_LEN 24
#define PSN_MSK ((1 << PSN_LEN) - 1)

#define BEACON_WRID 0xDEADBEEF

struct ib_cm_meta_t {
  uint16_t lid;
  uint32_t local_qpn;
  uint32_t psn;
  uint32_t peer_qpn;
  union ibv_gid gid;
} __attribute__((packed));

class RDMAStack;
class CephContext;

class Port {
  struct ibv_context* ctxt;
  int port_num;
  struct ibv_port_attr port_attr;
  uint16_t lid;
  int gid_idx;
  union ibv_gid gid;

 public:
  explicit Port(CephContext *cct, struct ibv_context* ictxt, uint8_t ipn);
  uint16_t get_lid() { return lid; }
  ibv_gid  get_gid() { return gid; }
  int get_port_num() { return port_num; }
  ibv_port_attr* get_port_attr() { return &port_attr; }
  int get_gid_idx() { return gid_idx; }
};


class Device {
  ibv_device *device;
  const char* name;
  uint8_t  port_cnt = 0;
 public:
  explicit Device(CephContext *c, ibv_device* ib_dev);
  explicit Device(CephContext *c, ibv_context *ib_ctx);
  ~Device() {
    if (active_port) {
      delete active_port;
      ceph_assert(ibv_close_device(ctxt) == 0);
    }
  }
  const char* get_name() { return name;}
  uint16_t get_lid() { return active_port->get_lid(); }
  ibv_gid get_gid() { return active_port->get_gid(); }
  int get_gid_idx() { return active_port->get_gid_idx(); }
  void binding_port(CephContext *c, int port_num);
  struct ibv_context *ctxt;
  ibv_device_attr device_attr;
  Port* active_port;
};


class DeviceList {
  struct ibv_device ** device_list;
  struct ibv_context ** device_context_list;
  int num;
  Device** devices;
 public:
  explicit DeviceList(CephContext *cct): device_list(nullptr), device_context_list(nullptr),
                                         num(0), devices(nullptr) {
    device_list = ibv_get_device_list(&num);
    ceph_assert(device_list);
    ceph_assert(num);
    if (cct->_conf->ms_async_rdma_cm) {
        device_context_list = rdma_get_devices(NULL);
        ceph_assert(device_context_list);
    }
    devices = new Device*[num];

    for (int i = 0;i < num; ++i) {
      if (cct->_conf->ms_async_rdma_cm) {
         devices[i] = new Device(cct, device_context_list[i]);
      } else {
         devices[i] = new Device(cct, device_list[i]);
      }
    }
  }
  ~DeviceList() {
    for (int i=0; i < num; ++i) {
      delete devices[i];
    }
    delete []devices;
    ibv_free_device_list(device_list);
    rdma_free_devices(device_context_list);
  }

  Device* get_device(const char* device_name) {
    for (int i = 0; i < num; ++i) {
      if (!strlen(device_name) || !strcmp(device_name, devices[i]->get_name())) {
        return devices[i];
      }
    }
    return NULL;
  }
};

// stat counters
enum {
  l_msgr_rdma_dispatcher_first = 94000,

  l_msgr_rdma_polling,
  l_msgr_rdma_inflight_tx_chunks,
  l_msgr_rdma_rx_bufs_in_use,
  l_msgr_rdma_rx_bufs_total,

  l_msgr_rdma_tx_total_wc,
  l_msgr_rdma_tx_total_wc_errors,
  l_msgr_rdma_tx_wc_retry_errors,
  l_msgr_rdma_tx_wc_wr_flush_errors,

  l_msgr_rdma_rx_total_wc,
  l_msgr_rdma_rx_total_wc_errors,
  l_msgr_rdma_rx_fin,

  l_msgr_rdma_handshake_errors,

  l_msgr_rdma_total_async_events,
  l_msgr_rdma_async_last_wqe_events,

  l_msgr_rdma_created_queue_pair,
  l_msgr_rdma_active_queue_pair,

  l_msgr_rdma_dispatcher_last,
};

enum {
  l_msgr_rdma_first = 95000,

  l_msgr_rdma_tx_no_mem,
  l_msgr_rdma_tx_parital_mem,
  l_msgr_rdma_tx_failed,

  l_msgr_rdma_tx_chunks,
  l_msgr_rdma_tx_bytes,
  l_msgr_rdma_rx_chunks,
  l_msgr_rdma_rx_bytes,
  l_msgr_rdma_pending_sent_conns,

  l_msgr_rdma_last,
};

class RDMADispatcher;

class Infiniband {
 public:
  class ProtectionDomain {
   public:
    explicit ProtectionDomain(CephContext *cct, Device *device);
    ~ProtectionDomain();

    ibv_pd* const pd;
  };

  class QueuePair;
  class MemoryManager {
   public:
    class Chunk {
     public:
      Chunk(ibv_mr* m, uint32_t bytes, char* buffer, uint32_t offset = 0, uint32_t bound = 0, uint32_t lkey = 0, QueuePair* qp = nullptr);
      ~Chunk();

      uint32_t get_offset();
      uint32_t get_size() const;
      void prepare_read(uint32_t b);
      uint32_t get_bound();
      uint32_t read(char* buf, uint32_t len);
      uint32_t write(char* buf, uint32_t len);
      bool full();
      void reset_read_chunk();
      void reset_write_chunk();
      void set_qp(QueuePair *qp) { this->qp = qp; }
      void clear_qp() { set_qp(nullptr); }
      QueuePair* get_qp() { return qp; }

     public:
      ibv_mr* mr;
      QueuePair *qp;
      uint32_t lkey;
      uint32_t bytes;
      uint32_t offset;
      uint32_t bound;
      char* buffer; // TODO: remove buffer/refactor TX
      char  data[0];
    };

    class Cluster {
     public:
      Cluster(MemoryManager& m, uint32_t s);
      ~Cluster();

      int fill(uint32_t num);
      void take_back(std::vector<Chunk*> &ck);
      int get_buffers(std::vector<Chunk*> &chunks, size_t bytes);
      Chunk *get_chunk_by_buffer(const char *c) {
        uint32_t idx = (c - base) / buffer_size;
        Chunk *chunk = chunk_base + idx;
        return chunk;
      }
      bool is_my_buffer(const char *c) const {
        return c >= base && c < end;
      }

      MemoryManager& manager;
      uint32_t buffer_size;
      uint32_t num_chunk = 0;
      ceph::mutex lock = ceph::make_mutex("cluster_lock");
      std::vector<Chunk*> free_chunks;
      char *base = nullptr;
      char *end = nullptr;
      Chunk* chunk_base = nullptr;
    };

    class MemPoolContext {
      PerfCounters *perf_logger;

     public:
      MemoryManager *manager;
      unsigned n_bufs_allocated;
      // true if it is possible to alloc
      // more memory for the pool
      explicit MemPoolContext(MemoryManager *m) :
        perf_logger(nullptr),
        manager(m),
        n_bufs_allocated(0) {}
      bool can_alloc(unsigned nbufs);
      void update_stats(int val);
      void set_stat_logger(PerfCounters *logger);
    };

    class PoolAllocator {
      struct mem_info {
        ibv_mr   *mr;
        MemPoolContext *ctx;
        unsigned nbufs;
        Chunk    chunks[0];
      };
     public:
      typedef std::size_t size_type;
      typedef std::ptrdiff_t difference_type;

      static char * malloc(const size_type bytes);
      static void free(char * const block);

      template<typename Func>
      static std::invoke_result_t<Func> with_context(MemPoolContext* ctx,
						     Func&& func) {
	std::lock_guard l{get_lock()};
	g_ctx = ctx;
	scope_guard reset_ctx{[] { g_ctx = nullptr; }};
	return std::move(func)();
      }
    private:
      static ceph::mutex& get_lock();
      static MemPoolContext* g_ctx;
    };

    /**
     * modify boost pool so that it is possible to
     * have a thread safe 'context' when allocating/freeing
     * the memory. It is needed to allow a different pool
     * configurations and bookkeeping per CephContext and
     * also to be able to use same allocator to deal with
     * RX and TX pool.
     * TODO: use boost pool to allocate TX chunks too
     */
    class mem_pool : public boost::pool<PoolAllocator> {
     private:
      MemPoolContext *ctx;
      void *slow_malloc();

     public:
      ceph::mutex lock = ceph::make_mutex("mem_pool_lock");
      explicit mem_pool(MemPoolContext *ctx, const size_type nrequested_size,
          const size_type nnext_size = 32,
          const size_type nmax_size = 0) :
        pool(nrequested_size, nnext_size, nmax_size),
        ctx(ctx) { }

      void *malloc() {
        if (!store().empty())
          return (store().malloc)();
        // need to alloc more memory...
        // slow path code
        return slow_malloc();
      }
    };

    MemoryManager(CephContext *c, Device *d, ProtectionDomain *p);
    ~MemoryManager();

    void* malloc(size_t size);
    void  free(void *ptr);

    void create_tx_pool(uint32_t size, uint32_t tx_num);
    void return_tx(std::vector<Chunk*> &chunks);
    int get_send_buffers(std::vector<Chunk*> &c, size_t bytes);
    bool is_tx_buffer(const char* c) { return send->is_my_buffer(c); }
    Chunk *get_tx_chunk_by_buffer(const char *c) {
      return send->get_chunk_by_buffer(c);
    }
    uint32_t get_tx_buffer_size() const {
      return send->buffer_size;
    }

    Chunk *get_rx_buffer() {
       std::lock_guard l{rxbuf_pool.lock};
       return reinterpret_cast<Chunk *>(rxbuf_pool.malloc());
    }

    void release_rx_buffer(Chunk *chunk) {
      std::lock_guard l{rxbuf_pool.lock};
      chunk->clear_qp();
      rxbuf_pool.free(chunk);
    }

    void set_rx_stat_logger(PerfCounters *logger) {
      rxbuf_pool_ctx.set_stat_logger(logger);
    }

    CephContext  *cct;
   private:
    // TODO: Cluster -> TxPool txbuf_pool
    // chunk layout fix
    //  
    Cluster* send = nullptr;// SEND
    Device *device;
    ProtectionDomain *pd;
    MemPoolContext rxbuf_pool_ctx;
    mem_pool     rxbuf_pool;


    void* huge_pages_malloc(size_t size);
    void  huge_pages_free(void *ptr);
  };

 private:
  uint32_t tx_queue_len = 0;
  uint32_t rx_queue_len = 0;
  uint32_t max_sge = 0;
  uint8_t  ib_physical_port = 0;
  MemoryManager* memory_manager = nullptr;
  ibv_srq* srq = nullptr;             // shared receive work queue
  Device *device = NULL;
  ProtectionDomain *pd = NULL;
  DeviceList *device_list = nullptr;
  CephContext *cct;
  ceph::mutex lock = ceph::make_mutex("IB lock");
  bool initialized = false;
  const std::string &device_name;
  uint8_t port_num;
  bool support_srq = false;

 public:
  explicit Infiniband(CephContext *c);
  ~Infiniband();
  void init();
  static void verify_prereq(CephContext *cct);

  class CompletionChannel {
    static const uint32_t MAX_ACK_EVENT = 5000;
    CephContext *cct;
    Infiniband& infiniband;
    ibv_comp_channel *channel;
    ibv_cq *cq;
    uint32_t cq_events_that_need_ack;

   public:
    CompletionChannel(CephContext *c, Infiniband &ib);
    ~CompletionChannel();
    int init();
    bool get_cq_event();
    int get_fd() { return channel->fd; }
    ibv_comp_channel* get_channel() { return channel; }
    void bind_cq(ibv_cq *c) { cq = c; }
    void ack_events();
  };

  // this class encapsulates the creation, use, and destruction of an RC
  // completion queue.
  //
  // You need to call init and it will create a cq and associate to comp channel
  class CompletionQueue {
   public:
    CompletionQueue(CephContext *c, Infiniband &ib,
                    const uint32_t qd, CompletionChannel *cc)
      : cct(c), infiniband(ib), channel(cc), cq(NULL), queue_depth(qd) {}
    ~CompletionQueue();
    int init();
    int poll_cq(int num_entries, ibv_wc *ret_wc_array);

    ibv_cq* get_cq() const { return cq; }
    int rearm_notify(bool solicited_only=true);
    CompletionChannel* get_cc() const { return channel; }
   private:
    CephContext *cct;
    Infiniband&  infiniband;     // Infiniband to which this QP belongs
    CompletionChannel *channel;
    ibv_cq *cq;
    uint32_t queue_depth;
  };

  // this class encapsulates the creation, use, and destruction of an RC
  // queue pair.
  //
  // you need call init and it will create a qp and bring it to the INIT state.
  // after obtaining the lid, qpn, and psn of a remote queue pair, one
  // must call plumb() to bring the queue pair to the RTS state.
  class QueuePair {
   public:
    typedef MemoryManager::Chunk Chunk;
    QueuePair(CephContext *c, Infiniband& infiniband, ibv_qp_type type,
              int ib_physical_port,  ibv_srq *srq,
              Infiniband::CompletionQueue* txcq,
              Infiniband::CompletionQueue* rxcq,
              uint32_t tx_queue_len, uint32_t max_recv_wr, struct rdma_cm_id *cid, uint32_t q_key = 0);
    ~QueuePair();

    int modify_qp_to_error();
    int modify_qp_to_rts();
    int modify_qp_to_rtr();
    int modify_qp_to_init();
    int init();

    /**
     * Get the initial packet sequence number for this QueuePair.
     * This is randomly generated on creation. It should not be confused
     * with the remote side's PSN, which is set in #plumb(). 
     */
    uint32_t get_initial_psn() const { return initial_psn; };
    /**
     * Get the local queue pair number for this QueuePair.
     * QPNs are analogous to UDP/TCP port numbers.
     */
    uint32_t get_local_qp_number() const { return qp->qp_num; };
    /**
     * Get the remote queue pair number for this QueuePair, as set in #plumb().
     * QPNs are analogous to UDP/TCP port numbers.
     */
    int get_remote_qp_number(uint32_t *rqp) const;
    /**
     * Get the remote infiniband address for this QueuePair, as set in #plumb().
     * LIDs are "local IDs" in infiniband terminology. They are short, locally
     * routable addresses.
     */
    int get_remote_lid(uint16_t *lid) const;
    /**
     * Get the state of a QueuePair.
     */
    int get_state() const;
    /*
     * send/receive connection management meta data
     */
    int send_cm_meta(CephContext *cct, int socket_fd);
    int recv_cm_meta(CephContext *cct, int socket_fd);
    void wire_gid_to_gid(const char *wgid, ib_cm_meta_t* cm_meta_data);
    void gid_to_wire_gid(const ib_cm_meta_t& cm_meta_data, char wgid[]);
    ibv_qp* get_qp() const { return qp; }
    Infiniband::CompletionQueue* get_tx_cq() const { return txcq; }
    Infiniband::CompletionQueue* get_rx_cq() const { return rxcq; }
    int to_dead();
    bool is_dead() const { return dead; }
    ib_cm_meta_t& get_peer_cm_meta() { return peer_cm_meta; }
    ib_cm_meta_t& get_local_cm_meta() { return local_cm_meta; }
    void add_rq_wr(Chunk* chunk)
    {
      if (srq) return;

      std::lock_guard l{lock};
      recv_queue.push_back(chunk);
    }

    void remove_rq_wr(Chunk* chunk) {
      if (srq) return;

      std::lock_guard l{lock};
      auto it = std::find(recv_queue.begin(), recv_queue.end(), chunk);
      ceph_assert(it != recv_queue.end());
      recv_queue.erase(it);
    }
    ibv_srq* get_srq() const { return srq; }

   private:
    CephContext  *cct;
    Infiniband&  infiniband;     // Infiniband to which this QP belongs
    ibv_qp_type  type;           // QP type (IBV_QPT_RC, etc.)
    ibv_context* ctxt;           // device context of the HCA to use
    int ib_physical_port;
    ibv_pd*      pd;             // protection domain
    ibv_srq*     srq;            // shared receive queue
    ibv_qp*      qp;             // infiniband verbs QP handle
    struct rdma_cm_id *cm_id;
    ib_cm_meta_t peer_cm_meta;
    ib_cm_meta_t local_cm_meta;
    Infiniband::CompletionQueue* txcq;
    Infiniband::CompletionQueue* rxcq;
    uint32_t     initial_psn;    // initial packet sequence number
    uint32_t     max_send_wr;
    uint32_t     max_recv_wr;
    uint32_t     q_key;
    bool dead;
    vector<Chunk*> recv_queue;
    ceph::mutex lock = ceph::make_mutex("queue_pair_lock");
  };

 public:
  typedef MemoryManager::Cluster Cluster;
  typedef MemoryManager::Chunk Chunk;
  QueuePair* create_queue_pair(CephContext *c, CompletionQueue*, CompletionQueue*,
      ibv_qp_type type, struct rdma_cm_id *cm_id);
  ibv_srq* create_shared_receive_queue(uint32_t max_wr, uint32_t max_sge);
  // post rx buffers to srq, return number of buffers actually posted
  int post_chunks_to_rq(int num, QueuePair *qp = nullptr);
  void post_chunk_to_pool(Chunk* chunk) {
    QueuePair *qp = chunk->get_qp();
    if (qp != nullptr) {
      qp->remove_rq_wr(chunk);
    }
    get_memory_manager()->release_rx_buffer(chunk);
  }
  int get_tx_buffers(std::vector<Chunk*> &c, size_t bytes);
  CompletionChannel *create_comp_channel(CephContext *c);
  CompletionQueue *create_comp_queue(CephContext *c, CompletionChannel *cc=NULL);
  uint8_t get_ib_physical_port() { return ib_physical_port; }
  uint16_t get_lid() { return device->get_lid(); }
  ibv_gid get_gid() { return device->get_gid(); }
  MemoryManager* get_memory_manager() { return memory_manager; }
  Device* get_device() { return device; }
  int get_async_fd() { return device->ctxt->async_fd; }
  bool is_tx_buffer(const char* c) { return memory_manager->is_tx_buffer(c);}
  Chunk *get_tx_chunk_by_buffer(const char *c) { return memory_manager->get_tx_chunk_by_buffer(c); }
  static const char* wc_status_to_string(int status);
  static const char* qp_state_string(int status);
  uint32_t get_rx_queue_len() const { return rx_queue_len; }
};

#endif
