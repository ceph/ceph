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

#include <string>
#include <vector>

#include <infiniband/verbs.h>

#include "include/int_types.h"
#include "include/page.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Mutex.h"
#include "msg/msg_types.h"
#include "msg/async/net_handler.h"

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

class Port {
  struct ibv_context* ctxt;
  int port_num;
  struct ibv_port_attr* port_attr;
  uint16_t lid;
  int gid_idx;
  union ibv_gid gid;

 public:
  explicit Port(CephContext *cct, struct ibv_context* ictxt, uint8_t ipn);
  uint16_t get_lid() { return lid; }
  ibv_gid  get_gid() { return gid; }
  int get_port_num() { return port_num; }
  ibv_port_attr* get_port_attr() { return port_attr; }
  int get_gid_idx() { return gid_idx; }
};


class Device {
  ibv_device *device;
  const char* name;
  uint8_t  port_cnt;
 public:
  explicit Device(CephContext *c, ibv_device* d);
  ~Device() {
    if (active_port) {
      delete active_port;
      assert(ibv_close_device(ctxt) == 0);
    }
  }
  const char* get_name() { return name;}
  uint16_t get_lid() { return active_port->get_lid(); }
  ibv_gid get_gid() { return active_port->get_gid(); }
  int get_gid_idx() { return active_port->get_gid_idx(); }
  void binding_port(CephContext *c, int port_num);
  struct ibv_context *ctxt;
  ibv_device_attr *device_attr;
  Port* active_port;
};


class DeviceList {
  struct ibv_device ** device_list;
  int num;
  Device** devices;
 public:
  DeviceList(CephContext *cct): device_list(ibv_get_device_list(&num)) {
    if (device_list == NULL || num == 0) {
      lderr(cct) << __func__ << " failed to get rdma device list.  " << cpp_strerror(errno) << dendl;
      ceph_abort();
    }
    devices = new Device*[num];

    for (int i = 0;i < num; ++i) {
      devices[i] = new Device(cct, device_list[i]);
    }
  }
  ~DeviceList() {
    for (int i=0; i < num; ++i) {
      delete devices[i];
    }
    delete []devices;
    ibv_free_device_list(device_list);
  }

  Device* get_device(const char* device_name) {
    assert(devices);
    for (int i = 0; i < num; ++i) {
      if (!strlen(device_name) || !strcmp(device_name, devices[i]->get_name())) {
        return devices[i];
      }
    }
    return NULL;
  }
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
      void post_srq(Infiniband *ib);

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
  uint32_t max_send_wr;
  uint32_t max_recv_wr;
  uint32_t max_sge;
  uint8_t  ib_physical_port;
  MemoryManager* memory_manager;
  ibv_srq* srq;             // shared receive work queue
  Device *device;
  ProtectionDomain *pd;
  DeviceList *device_list = nullptr;
  RDMADispatcher *dispatcher = nullptr;
  void wire_gid_to_gid(const char *wgid, union ibv_gid *gid);
  void gid_to_wire_gid(const union ibv_gid *gid, char wgid[]);
  CephContext *cct;
  Mutex lock;
  bool initialized = false;
  const std::string &device_name;
  uint8_t port_num;

 public:
  explicit Infiniband(CephContext *c, const std::string &device_name, uint8_t p);
  ~Infiniband();
  void init();

  void set_dispatcher(RDMADispatcher *d);

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
    QueuePair(CephContext *c, Infiniband& infiniband, ibv_qp_type type,
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
    Infiniband&  infiniband;     // Infiniband to which this QP belongs
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
  typedef MemoryManager::Cluster Cluster;
  typedef MemoryManager::Chunk Chunk;
  QueuePair* create_queue_pair(CephContext *c, CompletionQueue*, CompletionQueue*, ibv_qp_type type);
  ibv_srq* create_shared_receive_queue(uint32_t max_wr, uint32_t max_sge);
  int post_chunk(Chunk* chunk);
  int post_channel_cluster();
  int get_tx_buffers(std::vector<Chunk*> &c, size_t bytes);
  CompletionChannel *create_comp_channel(CephContext *c);
  CompletionQueue *create_comp_queue(CephContext *c, CompletionChannel *cc=NULL);
  uint8_t get_ib_physical_port() { return ib_physical_port; }
  int send_msg(CephContext *cct, int sd, IBSYNMsg& msg);
  int recv_msg(CephContext *cct, int sd, IBSYNMsg& msg);
  uint16_t get_lid() { return device->get_lid(); }
  ibv_gid get_gid() { return device->get_gid(); }
  MemoryManager* get_memory_manager() { return memory_manager; }
  Device* get_device() { return device; }
  int get_async_fd() { return device->ctxt->async_fd; }
  bool is_tx_buffer(const char* c) { return memory_manager->is_tx_buffer(c);}
  bool is_rx_buffer(const char* c) { return memory_manager->is_rx_buffer(c);}
  Chunk *get_tx_chunk_by_buffer(const char *c) { return memory_manager->get_tx_chunk_by_buffer(c); }
  static const char* wc_status_to_string(int status);
  static const char* qp_state_string(int status);
};

#endif
