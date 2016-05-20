// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_DPDK_DEV_H
#define CEPH_DPDK_DEV_H

#include <memory>
#include <functional>
#include <rte_config.h>
#include <rte_common.h>
#include <rte_ethdev.h>
#include <rte_malloc.h>

#include "include/page.h"
#include "common/Tub.h"
#include "common/perf_counters.h"
#include "msg/async/Event.h"
#include "const.h"
#include "circular_buffer.h"
#include "ethernet.h"
#include "memory.h"
#include "Packet.h"
#include "stream.h"
#include "net.h"
#include "toeplitz.h"


struct free_deleter {
  void operator()(void* p) { ::free(p); }
};


enum {
  l_dpdk_dev_first = 58800,
  l_dpdk_dev_rx_mcast,
  l_dpdk_dev_rx_total_errors,
  l_dpdk_dev_tx_total_errors,
  l_dpdk_dev_rx_badcrc_errors,
  l_dpdk_dev_rx_dropped_errors,
  l_dpdk_dev_rx_nombuf_errors,
  l_dpdk_dev_last
};

enum {
  l_dpdk_qp_first = 58900,
  l_dpdk_qp_rx_packets,
  l_dpdk_qp_tx_packets,
  l_dpdk_qp_rx_bad_checksum_errors,
  l_dpdk_qp_rx_no_memory_errors,
  l_dpdk_qp_rx_bytes,
  l_dpdk_qp_tx_bytes,
  l_dpdk_qp_rx_last_bunch,
  l_dpdk_qp_tx_last_bunch,
  l_dpdk_qp_rx_fragments,
  l_dpdk_qp_tx_fragments,
  l_dpdk_qp_rx_copy_ops,
  l_dpdk_qp_tx_copy_ops,
  l_dpdk_qp_rx_copy_bytes,
  l_dpdk_qp_tx_copy_bytes,
  l_dpdk_qp_rx_linearize_ops,
  l_dpdk_qp_tx_linearize_ops,
  l_dpdk_qp_tx_queue_length,
  l_dpdk_qp_last
};

class DPDKDevice;
class DPDKWorker;

class DPDKQueuePair {
  using packet_provider_type = std::function<Tub<Packet> ()>;
 public:
  void configure_proxies(const std::map<unsigned, float>& cpu_weights);
  // build REdirection TAble for cpu_weights map: target cpu -> weight
  void build_sw_reta(const std::map<unsigned, float>& cpu_weights);
  void proxy_send(Packet p) {
    _proxy_packetq.push_back(std::move(p));
  }
  void register_packet_provider(packet_provider_type func) {
    _pkt_providers.push_back(std::move(func));
  }
  bool poll_tx();
  friend class DPDKDevice;

  class tx_buf_factory;

  class tx_buf {
    friend class DPDKQueuePair;
   public:
    static tx_buf* me(rte_mbuf* mbuf) {
      return reinterpret_cast<tx_buf*>(mbuf);
    }

   private:
    /**
     * Checks if the original packet of a given cluster should be linearized
     * due to HW limitations.
     *
     * @param head head of a cluster to check
     *
     * @return TRUE if a packet should be linearized.
     */
    static bool i40e_should_linearize(rte_mbuf *head);

    /**
     * Sets the offload info in the head buffer of an rte_mbufs cluster.
     *
     * @param p an original packet the cluster is built for
     * @param qp QP handle
     * @param head a head of an rte_mbufs cluster
     */
    static void set_cluster_offload_info(const Packet& p, const DPDKQueuePair& qp, rte_mbuf* head);

    /**
     * Creates a tx_buf cluster representing a given packet in a "zero-copy"
     * way.
     *
     * @param p packet to translate
     * @param qp DPDKQueuePair handle
     *
     * @return the HEAD tx_buf of the cluster or nullptr in case of a
     *         failure
     */
    static tx_buf* from_packet_zc(
            CephContext *cct, Packet&& p, DPDKQueuePair& qp);

    /**
     * Copy the contents of the "packet" into the given cluster of
     * rte_mbuf's.
     *
     * @note Size of the cluster has to be big enough to accommodate all the
     *       contents of the given packet.
     *
     * @param p packet to copy
     * @param head head of the rte_mbuf's cluster
     */
    static void copy_packet_to_cluster(const Packet& p, rte_mbuf* head);

    /**
     * Creates a tx_buf cluster representing a given packet in a "copy" way.
     *
     * @param p packet to translate
     * @param qp DPDKQueuePair handle
     *
     * @return the HEAD tx_buf of the cluster or nullptr in case of a
     *         failure
     */
    static tx_buf* from_packet_copy(Packet&& p, DPDKQueuePair& qp);

    /**
     * Zero-copy handling of a single fragment.
     *
     * @param do_one_buf Functor responsible for a single rte_mbuf
     *                   handling
     * @param qp DPDKQueuePair handle (in)
     * @param frag Fragment to copy (in)
     * @param head Head of the cluster (out)
     * @param last_seg Last segment of the cluster (out)
     * @param nsegs Number of segments in the cluster (out)
     *
     * @return TRUE in case of success
     */
    template <class DoOneBufFunc>
    static bool do_one_frag(DoOneBufFunc do_one_buf, DPDKQueuePair& qp,
                            fragment& frag, rte_mbuf*& head,
                            rte_mbuf*& last_seg, unsigned& nsegs) {
      size_t len, left_to_set = frag.size;
      char* base = frag.base;

      rte_mbuf* m;

      // TODO: assert() in a fast path! Remove me ASAP!
      assert(frag.size);

      // Create a HEAD of mbufs' cluster and set the first bytes into it
      len = do_one_buf(qp, head, base, left_to_set);
      if (!len) {
        return false;
      }

      left_to_set -= len;
      base += len;
      nsegs = 1;

      //
      // Set the rest of the data into the new mbufs and chain them to
      // the cluster.
      //
      rte_mbuf* prev_seg = head;
      while (left_to_set) {
        len = do_one_buf(qp, m, base, left_to_set);
        if (!len) {
          me(head)->recycle();
          return false;
        }

        left_to_set -= len;
        base += len;
        nsegs++;

        prev_seg->next = m;
        prev_seg = m;
      }

      // Return the last mbuf in the cluster
      last_seg = prev_seg;

      return true;
    }

    /**
     * Zero-copy handling of a single fragment.
     *
     * @param qp DPDKQueuePair handle (in)
     * @param frag Fragment to copy (in)
     * @param head Head of the cluster (out)
     * @param last_seg Last segment of the cluster (out)
     * @param nsegs Number of segments in the cluster (out)
     *
     * @return TRUE in case of success
     */
    static bool translate_one_frag(DPDKQueuePair& qp, fragment& frag,
                                   rte_mbuf*& head, rte_mbuf*& last_seg,
                                   unsigned& nsegs) {
      return do_one_frag(set_one_data_buf, qp, frag, head,
                         last_seg, nsegs);
    }

    /**
     * Copies one fragment into the cluster of rte_mbuf's.
     *
     * @param qp DPDKQueuePair handle (in)
     * @param frag Fragment to copy (in)
     * @param head Head of the cluster (out)
     * @param last_seg Last segment of the cluster (out)
     * @param nsegs Number of segments in the cluster (out)
     *
     * We return the "last_seg" to avoid traversing the cluster in order to get
     * it.
     *
     * @return TRUE in case of success
     */
    static bool copy_one_frag(DPDKQueuePair& qp, fragment& frag,
                              rte_mbuf*& head, rte_mbuf*& last_seg,
                              unsigned& nsegs) {
      return do_one_frag(copy_one_data_buf, qp, frag, head,
                         last_seg, nsegs);
    }

    /**
     * Allocates a single rte_mbuf and sets it to point to a given data
     * buffer.
     *
     * @param qp DPDKQueuePair handle (in)
     * @param m New allocated rte_mbuf (out)
     * @param va virtual address of a data buffer (in)
     * @param buf_len length of the data to copy (in)
     *
     * @return The actual number of bytes that has been set in the mbuf
     */
    static size_t set_one_data_buf(
        DPDKQueuePair& qp, rte_mbuf*& m, char* va, size_t buf_len) {
      static constexpr size_t max_frag_len = 15 * 1024; // 15K

      // FIXME: current all tx buf is alloced without rte_malloc
      return copy_one_data_buf(qp, m, va, buf_len);
      //
      // Currently we break a buffer on a 15K boundary because 82599
      // devices have a 15.5K limitation on a maximum single fragment
      // size.
      //
      phys_addr_t pa = rte_malloc_virt2phy(va);
      if (!pa)
        return copy_one_data_buf(qp, m, va, buf_len);

      assert(buf_len);
      tx_buf* buf = qp.get_tx_buf();
      if (!buf) {
        return 0;
      }

      size_t len = std::min(buf_len, max_frag_len);

      buf->set_zc_info(va, pa, len);
      m = buf->rte_mbuf_p();

      return len;
    }

    /**
     *  Allocates a single rte_mbuf and copies a given data into it.
     *
     * @param qp DPDKQueuePair handle (in)
     * @param m New allocated rte_mbuf (out)
     * @param data Data to copy from (in)
     * @param buf_len length of the data to copy (in)
     *
     * @return The actual number of bytes that has been copied
     */
    static size_t copy_one_data_buf(
        DPDKQueuePair& qp, rte_mbuf*& m, char* data, size_t buf_len);

    /**
     * Checks if the first fragment of the given packet satisfies the
     * zero-copy flow requirement: its first 128 bytes should not cross the
     * 4K page boundary. This is required in order to avoid splitting packet
     * headers.
     *
     * @param p packet to check
     *
     * @return TRUE if packet is ok and FALSE otherwise.
     */
    static bool check_frag0(Packet& p)
    {
      //
      // First frag is special - it has headers that should not be split.
      // If the addressing is such that the first fragment has to be
      // split, then send this packet in a (non-zero) copy flow. We'll
      // check if the first 128 bytes of the first fragment reside in the
      // physically contiguous area. If that's the case - we are good to
      // go.
      //
      if (p.frag(0).size < 128)
        return false;

      return true;
    }

   public:
    tx_buf(tx_buf_factory& fc) : _fc(fc) {

      _buf_physaddr = _mbuf.buf_physaddr;
      _data_off     = _mbuf.data_off;
    }

    rte_mbuf* rte_mbuf_p() { return &_mbuf; }

    void set_zc_info(void* va, phys_addr_t pa, size_t len) {
      // mbuf_put()
      _mbuf.data_len           = len;
      _mbuf.pkt_len            = len;

      // Set the mbuf to point to our data
      _mbuf.buf_addr           = va;
      _mbuf.buf_physaddr       = pa;
      _mbuf.data_off           = 0;
      _is_zc                   = true;
    }

    void reset_zc() {

      //
      // If this mbuf was the last in a cluster and contains an
      // original packet object then call the destructor of the
      // original packet object.
      //
      if (_p) {
        //
        // Reset the std::optional. This in particular is going
        // to call the "packet"'s destructor and reset the
        // "optional" state to "nonengaged".
        //
        _p.destroy();

      } else if (!_is_zc) {
        return;
      }

      // Restore the rte_mbuf fields we trashed in set_zc_info()
      _mbuf.buf_physaddr = _buf_physaddr;
      _mbuf.buf_addr     = rte_mbuf_to_baddr(&_mbuf);
      _mbuf.data_off     = _data_off;

      _is_zc             = false;
    }

    void recycle() {
      struct rte_mbuf *m = &_mbuf, *m_next;

      while (m != nullptr) {
        m_next = m->next;
        rte_pktmbuf_reset(m);
        _fc.put(me(m));
        m = m_next;
      }
    }

    void set_packet(Packet&& p) {
      _p = std::move(p);
    }

   private:
    struct rte_mbuf _mbuf;
    MARKER private_start;
    Tub<Packet> _p;
    phys_addr_t _buf_physaddr;
    uint16_t _data_off;
    // TRUE if underlying mbuf has been used in the zero-copy flow
    bool _is_zc = false;
    // buffers' factory the buffer came from
    tx_buf_factory& _fc;
    MARKER private_end;
  };

  class tx_buf_factory {
    //
    // Number of buffers to free in each GC iteration:
    // We want the buffers to be allocated from the mempool as many as
    // possible.
    //
    // On the other hand if there is no Tx for some time we want the
    // completions to be eventually handled. Thus we choose the smallest
    // possible packets count number here.
    //
    static constexpr int gc_count = 1;
   public:
    tx_buf_factory(CephContext *c, DPDKDevice *dev, uint8_t qid);
    ~tx_buf_factory() {
      // put all mbuf back into mempool in order to make the next factory work
      while (gc());
      rte_mempool_put_bulk(_pool, (void**)_ring.data(),
                           _ring.size());
    }


    /**
     * @note Should not be called if there are no free tx_buf's
     *
     * @return a free tx_buf object
     */
    tx_buf* get() {
      // Take completed from the HW first
      tx_buf *pkt = get_one_completed();
      if (pkt) {
        pkt->reset_zc();
        return pkt;
      }

      //
      // If there are no completed at the moment - take from the
      // factory's cache.
      //
      if (_ring.empty()) {
        return nullptr;
      }

      pkt = _ring.back();
      _ring.pop_back();

      return pkt;
    }

    void put(tx_buf* buf) {
      buf->reset_zc();
      _ring.push_back(buf);
    }

    bool gc() {
      for (int cnt = 0; cnt < gc_count; ++cnt) {
        auto tx_buf_p = get_one_completed();
        if (!tx_buf_p) {
          return false;
        }

        put(tx_buf_p);
      }

      return true;
    }
   private:
    /**
     * Fill the mbufs circular buffer: after this the _pool will become
     * empty. We will use it to catch the completed buffers:
     *
     * - Underlying PMD drivers will "free" the mbufs once they are
     *   completed.
     * - We will poll the _pktmbuf_pool_tx till it's empty and release
     *   all the buffers from the freed mbufs.
     */
    void init_factory() {
      while (rte_mbuf* mbuf = rte_pktmbuf_alloc(_pool)) {
        _ring.push_back(new(tx_buf::me(mbuf)) tx_buf{*this});
      }
    }

    /**
     * PMD puts the completed buffers back into the mempool they have
     * originally come from.
     *
     * @note rte_pktmbuf_alloc() resets the mbuf so there is no need to call
     *       rte_pktmbuf_reset() here again.
     *
     * @return a single tx_buf that has been completed by HW.
     */
    tx_buf* get_one_completed() {
      return tx_buf::me(rte_pktmbuf_alloc(_pool));
    }

   private:
    CephContext *cct;
    std::vector<tx_buf*> _ring;
    rte_mempool* _pool = nullptr;
  };

 public:
  explicit DPDKQueuePair(CephContext *c, EventCenter *cen, DPDKDevice* dev, uint8_t qid);
  ~DPDKQueuePair() {
    if (device_stat_time_fd) {
      center->delete_time_event(device_stat_time_fd);
    }
    rx_gc(true);
  }

  void rx_start() {
    _rx_poller.construct(this);
  }

  uint32_t send(circular_buffer<Packet>& pb) {
    // Zero-copy send
    return _send(pb, [&] (Packet&& p) {
      return tx_buf::from_packet_zc(cct, std::move(p), *this);
    });
  }

  DPDKDevice& port() const { return *_dev; }
  tx_buf* get_tx_buf() { return _tx_buf_factory.get(); }

  void handle_stats();

 private:
  template <class Func>
  uint32_t _send(circular_buffer<Packet>& pb, Func &&packet_to_tx_buf_p) {
    if (_tx_burst.size() == 0) {
      for (auto&& p : pb) {
        // TODO: assert() in a fast path! Remove me ASAP!
        assert(p.len());

        tx_buf* buf = packet_to_tx_buf_p(std::move(p));
        if (!buf) {
          break;
        }

        _tx_burst.push_back(buf->rte_mbuf_p());
      }
    }

    uint16_t sent = rte_eth_tx_burst(_dev_port_idx, _qid,
                                     _tx_burst.data() + _tx_burst_idx,
                                     _tx_burst.size() - _tx_burst_idx);

    uint64_t nr_frags = 0, bytes = 0;

    for (int i = 0; i < sent; i++) {
      rte_mbuf* m = _tx_burst[_tx_burst_idx + i];
      bytes    += m->pkt_len;
      nr_frags += m->nb_segs;
      pb.pop_front();
    }

    perf_logger->inc(l_dpdk_qp_tx_fragments, nr_frags);
    perf_logger->inc(l_dpdk_qp_tx_bytes, bytes);

    _tx_burst_idx += sent;

    if (_tx_burst_idx == _tx_burst.size()) {
      _tx_burst_idx = 0;
      _tx_burst.clear();
    }

    return sent;
  }

  /**
   * Allocate a new data buffer and set the mbuf to point to it.
   *
   * Do some DPDK hacks to work on PMD: it assumes that the buf_addr
   * points to the private data of RTE_PKTMBUF_HEADROOM before the actual
   * data buffer.
   *
   * @param m mbuf to update
   */
  static bool refill_rx_mbuf(rte_mbuf* m, size_t size,
                             std::vector<void*> &datas) {
    if (datas.empty())
      return false;
    void *data = datas.back();
    datas.pop_back();

    //
    // Set the mbuf to point to our data.
    //
    // Do some DPDK hacks to work on PMD: it assumes that the buf_addr
    // points to the private data of RTE_PKTMBUF_HEADROOM before the
    // actual data buffer.
    //
    m->buf_addr      = (char*)data - RTE_PKTMBUF_HEADROOM;
    m->buf_physaddr  = rte_malloc_virt2phy(data) - RTE_PKTMBUF_HEADROOM;
    return true;
  }

  static bool init_noninline_rx_mbuf(rte_mbuf* m, size_t size,
                                     std::vector<void*> &datas) {
    if (!refill_rx_mbuf(m, size, datas)) {
      return false;
    }
    // The below fields stay constant during the execution.
    m->buf_len       = size + RTE_PKTMBUF_HEADROOM;
    m->data_off      = RTE_PKTMBUF_HEADROOM;
    return true;
  }

  bool init_rx_mbuf_pool();
  bool rx_gc(bool force=false);
  bool refill_one_cluster(rte_mbuf* head);

  /**
   * Polls for a burst of incoming packets. This function will not block and
   * will immediately return after processing all available packets.
   *
   */
  bool poll_rx_once();

  /**
   * Translates an rte_mbuf's into packet and feeds them to _rx_stream.
   *
   * @param bufs An array of received rte_mbuf's
   * @param count Number of buffers in the bufs[]
   */
  void process_packets(struct rte_mbuf **bufs, uint16_t count);

  /**
   * Translate rte_mbuf into the "packet".
   * @param m mbuf to translate
   *
   * @return a "optional" object representing the newly received data if in an
   *         "engaged" state or an error if in a "disengaged" state.
   */
  Tub<Packet> from_mbuf(rte_mbuf* m);

  /**
   * Transform an LRO rte_mbuf cluster into the "packet" object.
   * @param m HEAD of the mbufs' cluster to transform
   *
   * @return a "optional" object representing the newly received LRO packet if
   *         in an "engaged" state or an error if in a "disengaged" state.
   */
  Tub<Packet> from_mbuf_lro(rte_mbuf* m);

 private:
  CephContext *cct;
  std::vector<packet_provider_type> _pkt_providers;
  Tub<std::array<uint8_t, 128>> _sw_reta;
  circular_buffer<Packet> _proxy_packetq;
  stream<Packet> _rx_stream;
  circular_buffer<Packet> _tx_packetq;
  std::vector<void*> _alloc_bufs;

  PerfCounters *perf_logger;
  DPDKDevice* _dev;
  uint8_t _dev_port_idx;
  EventCenter *center;
  uint8_t _qid;
  rte_mempool *_pktmbuf_pool_rx;
  std::vector<rte_mbuf*> _rx_free_pkts;
  std::vector<rte_mbuf*> _rx_free_bufs;
  std::vector<fragment> _frags;
  std::vector<char*> _bufs;
  size_t _num_rx_free_segs = 0;
  uint64_t device_stat_time_fd = 0;

#ifdef CEPH_PERF_DEV
  uint64_t rx_cycles = 0;
  uint64_t rx_count = 0;
  uint64_t tx_cycles = 0;
  uint64_t tx_count = 0;
#endif

  class DPDKTXPoller : public EventCenter::Poller {
    DPDKQueuePair *qp;

   public:
    explicit DPDKTXPoller(DPDKQueuePair *qp)
        : EventCenter::Poller(qp->center, "DPDK::DPDKTXPoller"), qp(qp) {}

    virtual int poll() {
      return qp->poll_tx();
    }
  } _tx_poller;

  class DPDKRXGCPoller : public EventCenter::Poller {
    DPDKQueuePair *qp;

   public:
    explicit DPDKRXGCPoller(DPDKQueuePair *qp)
        : EventCenter::Poller(qp->center, "DPDK::DPDKRXGCPoller"), qp(qp) {}

    virtual int poll() {
      return qp->rx_gc();
    }
  } _rx_gc_poller;
  tx_buf_factory _tx_buf_factory;
  class DPDKRXPoller : public EventCenter::Poller {
    DPDKQueuePair *qp;

   public:
    explicit DPDKRXPoller(DPDKQueuePair *qp)
        : EventCenter::Poller(qp->center, "DPDK::DPDKRXPoller"), qp(qp) {}

    virtual int poll() {
      return qp->poll_rx_once();
    }
  };
  Tub<DPDKRXPoller> _rx_poller;
  class DPDKTXGCPoller : public EventCenter::Poller {
    DPDKQueuePair *qp;

   public:
    explicit DPDKTXGCPoller(DPDKQueuePair *qp)
        : EventCenter::Poller(qp->center, "DPDK::DPDKTXGCPoller"), qp(qp) {}

    virtual int poll() {
      return qp->_tx_buf_factory.gc();
    }
  } _tx_gc_poller;
  std::vector<rte_mbuf*> _tx_burst;
  uint16_t _tx_burst_idx = 0;
};

class DPDKDevice {
 public:
  CephContext *cct;
  PerfCounters *perf_logger;
  std::vector<std::unique_ptr<DPDKQueuePair>> _queues;
  std::vector<DPDKWorker*> workers;
  size_t _rss_table_bits = 0;
  uint8_t _port_idx;
  uint16_t _num_queues;
  unsigned cores;
  hw_features _hw_features;
  uint8_t _queues_ready = 0;
  unsigned _home_cpu;
  bool _use_lro;
  bool _enable_fc;
  std::vector<uint8_t> _redir_table;
  rss_key_type _rss_key;
  bool _is_i40e_device = false;
  bool _is_vmxnet3_device = false;

 public:
  rte_eth_dev_info _dev_info = {};

  /**
   * The final stage of a port initialization.
   * @note Must be called *after* all queues from stage (2) have been
   *       initialized.
   */
  int init_port_fini();

 private:
  /**
   * Port initialization consists of 3 main stages:
   * 1) General port initialization which ends with a call to
   *    rte_eth_dev_configure() where we request the needed number of Rx and
   *    Tx queues.
   * 2) Individual queues initialization. This is done in the constructor of
   *    DPDKQueuePair class. In particular the memory pools for queues are allocated
   *    in this stage.
   * 3) The final stage of the initialization which starts with the call of
   *    rte_eth_dev_start() after which the port becomes fully functional. We
   *    will also wait for a link to get up in this stage.
   */


  /**
   * First stage of the port initialization.
   *
   * @return 0 in case of success and an appropriate error code in case of an
   *         error.
   */
  int init_port_start();

  /**
   * Check the link status of out port in up to 9s, and print them finally.
   */
  int check_port_link_status();

  /**
   * Configures the HW Flow Control
   */
  void set_hw_flow_control();

 public:
  DPDKDevice(CephContext *c, uint8_t port_idx, uint16_t num_queues, bool use_lro, bool enable_fc):
      cct(c), _port_idx(port_idx), _num_queues(num_queues),
      _home_cpu(0), _use_lro(use_lro),
      _enable_fc(enable_fc) {
    _queues = std::vector<std::unique_ptr<DPDKQueuePair>>(_num_queues);
    /* now initialise the port we will use */
    int ret = init_port_start();
    if (ret != 0) {
      rte_exit(EXIT_FAILURE, "Cannot initialise port %u\n", _port_idx);
    }
    string name(std::string("port") + std::to_string(port_idx));
    PerfCountersBuilder plb(cct, name, l_dpdk_dev_first, l_dpdk_dev_last);

    plb.add_u64_counter(l_dpdk_dev_rx_mcast, "dpdk_device_receive_multicast_packets", "DPDK received multicast packets");
    plb.add_u64_counter(l_dpdk_dev_rx_total_errors, "dpdk_device_receive_total_errors", "DPDK received total_errors");
    plb.add_u64_counter(l_dpdk_dev_tx_total_errors, "dpdk_device_send_total_errors", "DPDK sendd total_errors");
    plb.add_u64_counter(l_dpdk_dev_rx_badcrc_errors, "dpdk_device_receive_badcrc_errors", "DPDK received bad crc errors");
    plb.add_u64_counter(l_dpdk_dev_rx_dropped_errors, "dpdk_device_receive_dropped_errors", "DPDK received dropped errors");
    plb.add_u64_counter(l_dpdk_dev_rx_nombuf_errors, "dpdk_device_receive_nombuf_errors", "DPDK received RX mbuf allocation errors");

    perf_logger = plb.create_perf_counters();
    cct->get_perfcounters_collection()->add(perf_logger);
  }

  ~DPDKDevice() {
    rte_eth_dev_stop(_port_idx);
  }

  DPDKQueuePair& queue_for_cpu(unsigned cpu) { return *_queues[cpu]; }
  void l2receive(int qid, Packet p) {
    _queues[qid]->_rx_stream.produce(std::move(p));
  }
  subscription<Packet> receive(unsigned cpuid, std::function<int (Packet)> next_packet) {
    auto sub = _queues[cpuid]->_rx_stream.listen(std::move(next_packet));
    _queues[cpuid]->rx_start();
    return std::move(sub);
  }
  ethernet_address hw_address() {
    struct ether_addr mac;
    rte_eth_macaddr_get(_port_idx, &mac);

    return mac.addr_bytes;
  }
  hw_features get_hw_features() {
    return _hw_features;
  }
  const rss_key_type& rss_key() const { return _rss_key; }
  uint16_t hw_queues_count() { return _num_queues; }
  std::unique_ptr<DPDKQueuePair> init_local_queue(CephContext *c, EventCenter *center, string hugepages, uint16_t qid) {
    std::unique_ptr<DPDKQueuePair> qp;
    qp = std::unique_ptr<DPDKQueuePair>(new DPDKQueuePair(c, center, this, qid));
    return std::move(qp);
  }
  unsigned hash2qid(uint32_t hash) {
    // return hash % hw_queues_count();
    return _redir_table[hash & (_redir_table.size() - 1)];
  }
  void set_local_queue(unsigned i, std::unique_ptr<DPDKQueuePair> qp) {
    assert(!_queues[i]);
    _queues[i] = std::move(qp);
  }
  void unset_local_queue(unsigned i) {
    assert(_queues[i]);
    _queues[i].reset();
  }
  template <typename Func>
  unsigned forward_dst(unsigned src_cpuid, Func&& hashfn) {
    auto& qp = queue_for_cpu(src_cpuid);
    if (!qp._sw_reta)
      return src_cpuid;

    assert(!qp._sw_reta);
    auto hash = hashfn() >> _rss_table_bits;
    auto& reta = *qp._sw_reta;
    return reta[hash % reta.size()];
  }
  unsigned hash2cpu(uint32_t hash) {
    // there is an assumption here that qid == get_id() which will
    // not necessary be true in the future
    return forward_dst(hash2qid(hash), [hash] { return hash; });
  }

  hw_features& hw_features_ref() { return _hw_features; }

  const rte_eth_rxconf* def_rx_conf() const {
    return &_dev_info.default_rxconf;
  }

  const rte_eth_txconf* def_tx_conf() const {
    return &_dev_info.default_txconf;
  }

  /**
   *  Set the RSS table in the device and store it in the internal vector.
   */
  void set_rss_table();

  uint8_t port_idx() { return _port_idx; }
  bool is_i40e_device() const {
    return _is_i40e_device;
  }
  bool is_vmxnet3_device() const {
    return _is_vmxnet3_device;
  }
};


std::unique_ptr<DPDKDevice> create_dpdk_net_device(
    CephContext *c, unsigned cores, uint8_t port_idx = 0,
    bool use_lro = true, bool enable_fc = true);


/**
 * @return Number of bytes needed for mempool objects of each QP.
 */
uint32_t qp_mempool_obj_size();

#endif // CEPH_DPDK_DEV_H
