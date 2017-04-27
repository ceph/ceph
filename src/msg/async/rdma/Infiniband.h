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

#include <infiniband/verbs.h>

#include <string>
#include <vector>

#include "include/int_types.h"
#include "include/page.h"
#include "common/debug.h"
#include "common/errno.h"
#include "msg/msg_types.h"
#include "msg/async/net_handler.h"
#include "common/Mutex.h"

#define RDMA_DEBUG 0

#if RDMA_DEBUG
#include "ib_dbg.h"
#endif

#define HUGE_PAGE_SIZE (2 * 1024 * 1024)
#define ALIGN_TO_PAGE_SIZE(x) \
  (((x) + HUGE_PAGE_SIZE -1) / HUGE_PAGE_SIZE * HUGE_PAGE_SIZE)

struct IBSYNMsg {
  uint16_t lid;
  uint32_t qpn;
  uint32_t psn;
  uint32_t peer_qpn;
  union ibv_gid gid;
} __attribute__((packed));

class RDMAStack;
class CephContext;
class Port;
class Device;
class DeviceList;
class RDMADispatcher;

class Infiniband {
 public:
  class ProtectionDomain {
   public:
    explicit ProtectionDomain(CephContext *cct, Device *device);
    ~ProtectionDomain();

    ibv_pd* const pd;
  };


  class MemoryManager {
   public:
    class Chunk {
     public:
      Chunk(ibv_mr* m, uint32_t len, char* b);
      ~Chunk();

      void set_offset(uint32_t o);
      uint32_t get_offset();
      void set_bound(uint32_t b);
      void prepare_read(uint32_t b);
      uint32_t get_bound();
      uint32_t read(char* buf, uint32_t len);
      uint32_t write(char* buf, uint32_t len);
      bool full();
      bool over();
      void clear();

     public:
      ibv_mr* mr;
      uint32_t bytes;
      uint32_t bound;
      uint32_t offset;
      char* buffer;
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
      uint32_t num_chunk;
      Mutex lock;
      std::vector<Chunk*> free_chunks;
      char *base = nullptr;
      char *end = nullptr;
      Chunk* chunk_base = nullptr;
    };

    MemoryManager(Device *d, ProtectionDomain *p, bool hugepage);
    ~MemoryManager();

    void* malloc_huge_pages(size_t size);
    void free_huge_pages(void *ptr);
    void register_rx_tx(uint32_t size, uint32_t rx_num, uint32_t tx_num);
    void return_tx(std::vector<Chunk*> &chunks);
    int get_send_buffers(std::vector<Chunk*> &c, size_t bytes);
    int get_channel_buffers(std::vector<Chunk*> &chunks, size_t bytes);
    bool is_tx_buffer(const char* c) { return send->is_my_buffer(c); }
    bool is_rx_buffer(const char* c) { return channel->is_my_buffer(c); }
    Chunk *get_tx_chunk_by_buffer(const char *c) {
      return send->get_chunk_by_buffer(c);
    }
    uint32_t get_tx_buffer_size() const {
      return send->buffer_size;
    }

    bool enabled_huge_page;

   private:
    Cluster* channel;//RECV
    Cluster* send;// SEND
    Device *device;
    ProtectionDomain *pd;
  };

 private:
  CephContext *cct;
  Mutex lock;
  bool initialized = false;
  DeviceList *device_list = nullptr;
  RDMADispatcher *dispatcher = nullptr;

 public:
  explicit Infiniband(CephContext *c);
  ~Infiniband();
  void init();

  void set_dispatcher(RDMADispatcher *d);

  class CompletionChannel {
    static const uint32_t MAX_ACK_EVENT = 5000;
    CephContext *cct;
    Device &ibdev;
    ibv_comp_channel *channel;
    ibv_cq *cq;
    uint32_t cq_events_that_need_ack;

   public:
    CompletionChannel(CephContext *c, Device &ibdev);
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
    CompletionQueue(CephContext *c, Device &ibdev,
                    const uint32_t qd, CompletionChannel *cc)
      : cct(c), ibdev(ibdev), channel(cc), cq(NULL), queue_depth(qd) {}
    ~CompletionQueue();
    int init();
    int poll_cq(int num_entries, ibv_wc *ret_wc_array);

    ibv_cq* get_cq() const { return cq; }
    int rearm_notify(bool solicited_only=true);
    CompletionChannel* get_cc() const { return channel; }
   private:
    CephContext *cct;
    Device &ibdev;
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
    QueuePair(CephContext *c, Device &device, ibv_qp_type type,
              int ib_physical_port,  ibv_srq *srq,
              Infiniband::CompletionQueue* txcq,
              Infiniband::CompletionQueue* rxcq,
              uint32_t max_send_wr, uint32_t max_recv_wr, uint32_t q_key = 0);
    ~QueuePair();

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
    /**
     * Return true if the queue pair is in an error state, false otherwise.
     */
    bool is_error() const;
    ibv_qp* get_qp() const { return qp; }
    Infiniband::CompletionQueue* get_tx_cq() const { return txcq; }
    Infiniband::CompletionQueue* get_rx_cq() const { return rxcq; }
    int to_dead();
    bool is_dead() const { return dead; }

   private:
    CephContext  *cct;
    Device       &ibdev;     // Infiniband to which this QP belongs
    ibv_qp_type  type;           // QP type (IBV_QPT_RC, etc.)
    ibv_context* ctxt;           // device context of the HCA to use
    int ib_physical_port;
    ibv_pd*      pd;             // protection domain
    ibv_srq*     srq;            // shared receive queue
    ibv_qp*      qp;             // infiniband verbs QP handle
    Infiniband::CompletionQueue* txcq;
    Infiniband::CompletionQueue* rxcq;
    uint32_t     initial_psn;    // initial packet sequence number
    uint32_t     max_send_wr;
    uint32_t     max_recv_wr;
    uint32_t     q_key;
    bool dead;
  };

 public:
  static const char* wc_status_to_string(int status);
  static const char* qp_state_string(int status);

  void handle_pre_fork();

  Device* get_device(const char* device_name);
  Device* get_device(const struct ibv_context *ctxt);

  int poll_tx(int n, Device **d, ibv_wc *wc);
  int poll_rx(int n, Device **d, ibv_wc *wc);
  int poll_blocking(bool &done);
  void rearm_notify();
  void handle_async_event();
  RDMADispatcher *get_dispatcher() { return dispatcher; }
};

inline ostream& operator<<(ostream& out, const Infiniband::QueuePair &qp)
{
    return out << qp.get_local_qp_number();
}

#endif
