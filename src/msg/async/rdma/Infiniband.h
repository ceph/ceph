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
  CephContext *cct;
  struct ibv_context* ctxt;
  uint8_t port_num;
  struct ibv_port_attr* port_attr;
  int gid_tbl_len;
  uint16_t lid;
  union ibv_gid gid;

 public:
  explicit Port(CephContext *c, struct ibv_context* ictxt, uint8_t ipn): cct(c), ctxt(ictxt), port_num(ipn), port_attr(new ibv_port_attr) {
    int r = ibv_query_port(ctxt, port_num, port_attr);
    if (r == -1) {
      lderr(cct) << __func__  << " query port failed  " << cpp_strerror(errno) << dendl;
      assert(0);
    }

    lid = port_attr->lid;
    r = ibv_query_gid(ctxt, port_num, 0, &gid);
    if (r) {
      lderr(cct) << __func__  << " query gid failed  " << cpp_strerror(errno) << dendl;
      assert(0);
    }
  }

  uint16_t get_lid() { return lid; }
  ibv_gid  get_gid() { return gid; }
  uint8_t get_port_num() { return port_num; }
  ibv_port_attr* get_port_attr() { return port_attr; }
};


class Device {
  CephContext *cct;
  ibv_device *device;
  const char* name;
  uint8_t  port_cnt;
  Port** ports;
 public:
  explicit Device(CephContext *c, ibv_device* d);
  ~Device() {
    for (uint8_t i = 0; i < port_cnt; ++i)
      delete ports[i];
    delete []ports;
    assert(ibv_close_device(ctxt) == 0);
  }
  const char* get_name() { return name;}
  uint16_t get_lid() { return active_port->get_lid(); }
  ibv_gid get_gid() { return active_port->get_gid(); }
  void binding_port(uint8_t port_num);
  struct ibv_context *ctxt;
  ibv_device_attr *device_attr;
  Port* active_port;
};


class DeviceList {
  CephContext *cct;
  struct ibv_device ** device_list;
  int num;
  Device** devices;
 public:
  DeviceList(CephContext *c): cct(c), device_list(ibv_get_device_list(&num)) {
    if (device_list == NULL || num == 0) {
      lderr(cct) << __func__ << " failed to get rdma device list.  " << cpp_strerror(errno) << dendl;
      assert(0);
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


class Infiniband {
 public:
  class ProtectionDomain {
   public:
    explicit ProtectionDomain(CephContext *c, Device *device)
      : cct(c), pd(ibv_alloc_pd(device->ctxt))
    {
      if (pd == NULL) {
        lderr(cct) << __func__ << " failed to allocate infiniband protection domain: " << cpp_strerror(errno) << dendl;
        assert(0);
      }
    }
    ~ProtectionDomain() {
      int rc = ibv_dealloc_pd(pd);
      if (rc != 0) {
        lderr(cct) << __func__ << " ibv_dealloc_pd failed: "
          << cpp_strerror(errno) << dendl;
      }
    }
    CephContext *cct;
    ibv_pd* const pd;
  };


  class MemoryManager {
   public:
    class Chunk {
     public:
      Chunk(char* b, uint32_t len, ibv_mr* m) : buffer(b), bytes(len), offset(0), mr(m) {}
      ~Chunk() {
        assert(ibv_dereg_mr(mr) == 0);
      }

      void set_offset(uint32_t o) {
        offset = o;
      }

      uint32_t get_offset() {
        return offset;
      }

      void set_bound(uint32_t b) {
        bound = b;
      }

      void prepare_read(uint32_t b) {
        offset = 0;
        bound = b;
      }

      uint32_t get_bound() {
        return bound;
      }

      uint32_t read(char* buf, uint32_t len) {
        uint32_t left = bound - offset;
        if (left >= len) {
          memcpy(buf, buffer+offset, len);
          offset += len;
          return len;
        } else {
          memcpy(buf, buffer+offset, left);
          offset = 0;
          bound = 0;
          return left;
        }
      }

      uint32_t write(char* buf, uint32_t len) {
        uint32_t left = bytes - offset;
        if (left >= len) {
          memcpy(buffer+offset, buf, len);
          offset += len;
          return len;
        } else {
          memcpy(buffer+offset, buf, left);
          offset = bytes;
          return left;
        }
      }

      bool full() {
        return offset == bytes;
      }

      bool over() {
        return offset == bound;
      }

      void clear() {
        offset = 0;
        bound = 0;
      }

      void post_srq(Infiniband *ib) {
        ib->post_chunk(this);
      }

      void set_owner(uint64_t o) {
        owner = o;
      }

      uint64_t get_owner() {
        return owner;
      }

     public:
      char* buffer;
      uint32_t bytes;
      uint32_t bound;
      uint32_t offset;
      ibv_mr* mr;
      uint64_t owner;
    };

    class Cluster {
     public:
      Cluster(MemoryManager& m, uint32_t s) : manager(m), chunk_size(s), lock("cluster_lock"){}
      Cluster(MemoryManager& m, uint32_t s, uint32_t n) : manager(m), chunk_size(s), lock("cluster_lock"){
        add(n);
      }

      ~Cluster() {
        set<Chunk*>::iterator c = all_chunks.begin();
        while(c != all_chunks.end()) {
          delete *c;
          ++c;
        }
        if (manager.enabled_huge_page)
          delete base;
        else
          manager.free_huge_pages(base);
      }
      int add(uint32_t num) {
        uint32_t bytes = chunk_size * num;
        //cihar* base = (char*)malloc(bytes);
        if (!manager.enabled_huge_page) {
          base = (char*)memalign(CEPH_PAGE_SIZE, bytes);
        } else {
          base = (char*)manager.malloc_huge_pages(bytes);
        }
        assert(base);
        for (uint32_t offset = 0; offset < bytes; offset += chunk_size){
          ibv_mr* m = ibv_reg_mr(manager.pd->pd, base+offset, chunk_size, IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
          assert(m);
          Chunk* c = new Chunk(base+offset,chunk_size,m);
          free_chunks.push_back(c);
          all_chunks.insert(c);
        }
        return 0;
      }

      void take_back(Chunk* ck) {
        Mutex::Locker l(lock);
        free_chunks.push_back(ck);
      }

      int get_buffers(std::vector<Chunk*> &chunks, size_t bytes) {
        uint32_t num = bytes / chunk_size + 1;
        if (bytes % chunk_size == 0)
          --num;
        int r = num;
        Mutex::Locker l(lock);
        if (free_chunks.empty())
          return 0;
        if (!bytes) {
          free_chunks.swap(chunks);
          r = chunks.size();
          return r;
        }
        if (free_chunks.size() < num) {
          num = free_chunks.size();
          r = num;
        }
        for (uint32_t i = 0; i < num; ++i) {
          chunks.push_back(free_chunks.back());
          free_chunks.pop_back();
        }
        return r;
      }
      MemoryManager& manager;
      uint32_t chunk_size;
      Mutex lock;
      std::vector<Chunk*> free_chunks;
      std::set<Chunk*> all_chunks;
      char* base;
    };

    MemoryManager(CephContext *cct, Device *d, ProtectionDomain *p) : cct(cct), device(d), pd(p) {
      enabled_huge_page = cct->_conf->ms_async_rdma_enable_hugepage;
    }
    ~MemoryManager() {
      if (channel)
        delete channel;
      if (send)
        delete send;
    }
    void* malloc_huge_pages(size_t size) {
      size_t real_size = ALIGN_TO_PAGE_SIZE(size + HUGE_PAGE_SIZE);
      char *ptr = (char *)mmap(NULL, real_size, PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS |MAP_POPULATE | MAP_HUGETLB,-1, 0);
      if (ptr == MAP_FAILED) {
        lderr(cct) << __func__ << " MAP_FAILED" << dendl;
        ptr = (char *)malloc(real_size);
        if (ptr == NULL) return NULL;
        real_size = 0;
      }
      *((size_t *)ptr) = real_size;
      lderr(cct) << __func__ << " bingo!" << dendl;
      return ptr + HUGE_PAGE_SIZE;
    }
    void free_huge_pages(void *ptr) {
      if (ptr == NULL) return;
      void *real_ptr = (char *)ptr -HUGE_PAGE_SIZE;
      size_t real_size = *((size_t *)real_ptr);
      assert(real_size % HUGE_PAGE_SIZE == 0);
      if (real_size != 0)
        munmap(real_ptr, real_size);
      else
        free(real_ptr);
    }
    void register_rx_tx(uint32_t size, uint32_t rx_num, uint32_t tx_num) {
      assert(device);
      assert(pd);
      channel = new Cluster(*this, size);
      channel->add(rx_num);

      send = new Cluster(*this, size);
      send->add(tx_num);
    }
    void return_tx(std::vector<Chunk*> &chunks) {
      for (auto c : chunks) {
        c->clear();
        send->take_back(c);
      }
    }

    int get_send_buffers(std::vector<Chunk*> &c, size_t bytes) {
      return send->get_buffers(c, bytes);
    }

    int get_channel_buffers(std::vector<Chunk*> &chunks, size_t bytes) {
      return channel->get_buffers(chunks, bytes);
    }

    int is_tx_chunk(Chunk* c) { return send->all_chunks.count(c);}
    int is_rx_chunk(Chunk* c) { return channel->all_chunks.count(c);}
    bool enabled_huge_page;
   private:
    Cluster* channel;//RECV
    Cluster* send;// SEND
    CephContext *cct;
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
  CephContext* cct;
  DeviceList device_list;
  void wire_gid_to_gid(const char *wgid, union ibv_gid *gid);
  void gid_to_wire_gid(const union ibv_gid *gid, char wgid[]);

 public:
  NetHandler net;
  explicit Infiniband(CephContext *c, const std::string &device_name, uint8_t p);

  /**
   * Destroy an Infiniband object.
   */
  ~Infiniband() {
    assert(ibv_destroy_srq(srq) == 0);
    delete memory_manager;
    delete pd;
  }

  class CompletionChannel {
     static const uint32_t MAX_ACK_EVENT = 5000;
     Infiniband& infiniband;
     ibv_comp_channel *channel;
     ibv_cq *cq;
     uint32_t cq_events_that_need_ack;

   public:
    CompletionChannel(Infiniband &ib): infiniband(ib), channel(NULL), cq(NULL), cq_events_that_need_ack(0) {}
    ~CompletionChannel();
    int init();
    bool get_cq_event();
    int get_fd() { return channel->fd; }
    ibv_comp_channel* get_channel() { return channel; }
    void bind_cq(ibv_cq *c) { cq = c; }
    void ack_events() {
      ibv_ack_cq_events(cq, cq_events_that_need_ack);
      cq_events_that_need_ack = 0;
    }
  };

  // this class encapsulates the creation, use, and destruction of an RC
  // completion queue.
  //
  // You need to call init and it will create a cq and associate to comp channel
  class CompletionQueue {
   public:
    CompletionQueue(Infiniband &ib, const uint32_t qd, CompletionChannel *cc):infiniband(ib), channel(cc), cq(NULL), queue_depth(qd) {}
    ~CompletionQueue();
    int init();
    int poll_cq(int num_entries, ibv_wc *ret_wc_array);

    ibv_cq* get_cq() const { return cq; }
    int rearm_notify(bool solicited_only=true);
    CompletionChannel* get_cc() const { return channel; }
   private:
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
    QueuePair(Infiniband& infiniband, ibv_qp_type type,int ib_physical_port,  ibv_srq *srq, Infiniband::CompletionQueue* txcq, Infiniband::CompletionQueue* rxcq, uint32_t max_send_wr, uint32_t max_recv_wr, uint32_t q_key = 0);
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
    int get_remote_qp_number(uint32_t *rqp) const {
      ibv_qp_attr qpa;
      ibv_qp_init_attr qpia;

      int r = ibv_query_qp(qp, &qpa, IBV_QP_DEST_QPN, &qpia);
      if (r) {
        lderr(infiniband.cct) << __func__ << " failed to query qp: "
          << cpp_strerror(errno) << dendl;
        return -1;
      }

      if (rqp)
        *rqp = qpa.dest_qp_num;
      return 0;
    }
    /**
     * Get the remote infiniband address for this QueuePair, as set in #plumb().
     * LIDs are "local IDs" in infiniband terminology. They are short, locally
     * routable addresses.
     */
    int get_remote_lid(uint16_t *lid) const {
      ibv_qp_attr qpa;
      ibv_qp_init_attr qpia;

      int r = ibv_query_qp(qp, &qpa, IBV_QP_AV, &qpia);
      if (r) {
        lderr(infiniband.cct) << __func__ << " failed to query qp: "
          << cpp_strerror(errno) << dendl;
        return -1;
      }

      if (lid)
        *lid = qpa.ah_attr.dlid;
      return 0;
    }
    /**
     * Get the state of a QueuePair.
     */
    int get_state() const {
      ibv_qp_attr qpa;
      ibv_qp_init_attr qpia;

      int r = ibv_query_qp(qp, &qpa, IBV_QP_STATE, &qpia);
      if (r) {
        lderr(infiniband.cct) << __func__ << " failed to get state: "
          << cpp_strerror(errno) << dendl;
        return -1;
      }
      return qpa.qp_state;
    }
    /**
     * Return true if the queue pair is in an error state, false otherwise.
     */
    bool is_error() const {
      ibv_qp_attr qpa;
      ibv_qp_init_attr qpia;

      int r = ibv_query_qp(qp, &qpa, -1, &qpia);
      if (r) {
        lderr(infiniband.cct) << __func__ << " failed to get state: "
          << cpp_strerror(errno) << dendl;
        return true;
      }
      return qpa.cur_qp_state == IBV_QPS_ERR;
    }
    ibv_qp* get_qp() const { return qp; }
    Infiniband::CompletionQueue* get_tx_cq() const { return txcq; }
    Infiniband::CompletionQueue* get_rx_cq() const { return rxcq; }
    int to_dead();
    bool is_dead() const { return dead; }

   private:
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
  QueuePair* create_queue_pair(CompletionQueue*, CompletionQueue*, ibv_qp_type type);
  ibv_srq* create_shared_receive_queue(uint32_t max_wr, uint32_t max_sge);
  int post_chunk(Chunk* chunk);
  int post_channel_cluster();
  int get_tx_buffers(std::vector<Chunk*> &c, size_t bytes) {
    return memory_manager->get_send_buffers(c, bytes);
  }
  CompletionChannel *create_comp_channel();
  CompletionQueue *create_comp_queue(CompletionChannel *cc=NULL);
  uint8_t get_ib_physical_port() {
    return ib_physical_port;
  }
  int send_msg(int sd, IBSYNMsg& msg);
  int recv_msg(int sd, IBSYNMsg& msg);
  uint16_t get_lid() { return device->get_lid(); }
  ibv_gid get_gid() { return device->get_gid(); }
  MemoryManager* get_memory_manager() { return memory_manager; }
  Device* get_device() { return device; }
  int get_async_fd() { return device->ctxt->async_fd; }
  int recall_chunk(Chunk* c) {
    if (memory_manager->is_rx_chunk(c)) {
      post_chunk(c);  
      return 1;
    } else if (memory_manager->is_tx_chunk(c)) {
      vector<Chunk*> v;
      v.push_back(c);
      memory_manager->return_tx(v);  
      return 2;
    }
    return -1;
  }
  int is_tx_chunk(Chunk* c) { return memory_manager->is_tx_chunk(c); }
  int is_rx_chunk(Chunk* c) { return memory_manager->is_rx_chunk(c); }
  static const char* wc_status_to_string(int status);
  static const char* qp_state_string(int status);
};

#endif
