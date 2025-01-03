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

#include <atomic>
#include <vector>
#include <queue>

#include <rte_config.h>
#include <rte_common.h>
#include <rte_eal.h>
#include <rte_pci.h>
#include <rte_ethdev.h>
#include <rte_ether.h>
#include <rte_cycles.h>
#include <rte_memzone.h>

#include "include/page.h"
#include "align.h"
#include "IP.h"
#include "const.h"
#include "dpdk_rte.h"
#include "DPDK.h"
#include "toeplitz.h"

#include "common/Cycles.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/ceph_assert.h"

#define dout_subsys ceph_subsys_dpdk
#undef dout_prefix
#define dout_prefix *_dout << "dpdk "


void* as_cookie(struct rte_pktmbuf_pool_private& p) {
  return &p;
};

/******************* Net device related constatns *****************************/
static constexpr uint16_t default_ring_size      = 512;

//
// We need 2 times the ring size of buffers because of the way PMDs
// refill the ring.
//
static constexpr uint16_t mbufs_per_queue_rx     = 2 * default_ring_size;
static constexpr uint16_t rx_gc_thresh           = 64;

//
// No need to keep more descriptors in the air than can be sent in a single
// rte_eth_tx_burst() call.
//
static constexpr uint16_t mbufs_per_queue_tx     = 2 * default_ring_size;

static constexpr uint16_t mbuf_cache_size        = 512;
//
// Size of the data buffer in the non-inline case.
//
// We may want to change (increase) this value in future, while the
// inline_mbuf_data_size value will unlikely change due to reasons described
// above.
//
static constexpr size_t mbuf_data_size = 4096;

static constexpr uint16_t mbuf_overhead          =
                          sizeof(struct rte_mbuf) + RTE_PKTMBUF_HEADROOM;
//
// We'll allocate 2K data buffers for an inline case because this would require
// a single page per mbuf. If we used 4K data buffers here it would require 2
// pages for a single buffer (due to "mbuf_overhead") and this is a much more
// demanding memory constraint.
//
static constexpr size_t inline_mbuf_data_size = 2048;


// (INLINE_MBUF_DATA_SIZE(2K)*32 = 64K = Max TSO/LRO size) + 1 mbuf for headers
static constexpr uint8_t max_frags = 32 + 1;

//
// Intel's 40G NIC HW limit for a number of fragments in an xmit segment.
//
// See Chapter 8.4.1 "Transmit Packet in System Memory" of the xl710 devices
// spec. for more details.
//
static constexpr uint8_t i40e_max_xmit_segment_frags = 8;

//
// VMWare's virtual NIC limit for a number of fragments in an xmit segment.
//
// see drivers/net/vmxnet3/base/vmxnet3_defs.h VMXNET3_MAX_TXD_PER_PKT
//
static constexpr uint8_t vmxnet3_max_xmit_segment_frags = 16;

static constexpr uint16_t inline_mbuf_size = inline_mbuf_data_size + mbuf_overhead;

static size_t huge_page_size = 512 * CEPH_PAGE_SIZE;

uint32_t qp_mempool_obj_size()
{
  uint32_t mp_size = 0;
  struct rte_mempool_objsz mp_obj_sz = {};

  //
  // We will align each size to huge page size because DPDK allocates
  // physically contiguous memory region for each pool object.
  //

  // Rx
  mp_size += align_up(rte_mempool_calc_obj_size(mbuf_overhead, 0, &mp_obj_sz)+
                      sizeof(struct rte_pktmbuf_pool_private),
                      huge_page_size);

  //Tx
  std::memset(&mp_obj_sz, 0, sizeof(mp_obj_sz));
  mp_size += align_up(rte_mempool_calc_obj_size(inline_mbuf_size, 0,
                                                &mp_obj_sz)+
                      sizeof(struct rte_pktmbuf_pool_private),
                      huge_page_size);
  return mp_size;
}

static constexpr const char* pktmbuf_pool_name   = "dpdk_net_pktmbuf_pool";

/*
 * When doing reads from the NIC queues, use this batch size
 */
static constexpr uint8_t packet_read_size        = 32;
/******************************************************************************/

int DPDKDevice::init_port_start()
{
  ceph_assert(_port_idx < rte_eth_dev_count_avail());

  rte_eth_dev_info_get(_port_idx, &_dev_info);

  //
  // This is a workaround for a missing handling of a HW limitation in the
  // DPDK i40e driver. This and all related to _is_i40e_device code should be
  // removed once this handling is added.
  //
  if (std::string("rte_i40evf_pmd") == _dev_info.driver_name ||
      std::string("rte_i40e_pmd") == _dev_info.driver_name) {
    ldout(cct, 1) << __func__ << " Device is an Intel's 40G NIC. Enabling 8 fragments hack!" << dendl;
    _is_i40e_device = true;
  }

  if (std::string("rte_vmxnet3_pmd") == _dev_info.driver_name) {
    ldout(cct, 1) << __func__ << " Device is a VMWare Virtual NIC. Enabling 16 fragments hack!" << dendl;
    _is_vmxnet3_device = true;
  }

  //
  // Another workaround: this time for a lack of number of RSS bits.
  // ixgbe PF NICs support up to 16 RSS queues.
  // ixgbe VF NICs support up to 4 RSS queues.
  // i40e PF NICs support up to 64 RSS queues.
  // i40e VF NICs support up to 16 RSS queues.
  //
  if (std::string("rte_ixgbe_pmd") == _dev_info.driver_name) {
    _dev_info.max_rx_queues = std::min(_dev_info.max_rx_queues, (uint16_t)16);
  } else if (std::string("rte_ixgbevf_pmd") == _dev_info.driver_name) {
    _dev_info.max_rx_queues = std::min(_dev_info.max_rx_queues, (uint16_t)4);
  } else if (std::string("rte_i40e_pmd") == _dev_info.driver_name) {
    _dev_info.max_rx_queues = std::min(_dev_info.max_rx_queues, (uint16_t)64);
  } else if (std::string("rte_i40evf_pmd") == _dev_info.driver_name) {
    _dev_info.max_rx_queues = std::min(_dev_info.max_rx_queues, (uint16_t)16);
  }

  // Hardware offload capabilities
  // https://github.com/DPDK/dpdk/blob/v19.05/lib/librte_ethdev/rte_ethdev.h#L993-L1074
  // We want to support all available offload features
  // TODO: below features are implemented in 17.05, should support new ones
  const uint64_t tx_offloads_wanted =
    DEV_TX_OFFLOAD_VLAN_INSERT      |
    DEV_TX_OFFLOAD_IPV4_CKSUM       |
    DEV_TX_OFFLOAD_UDP_CKSUM        |
    DEV_TX_OFFLOAD_TCP_CKSUM        |
    DEV_TX_OFFLOAD_SCTP_CKSUM       |
    DEV_TX_OFFLOAD_TCP_TSO          |
    DEV_TX_OFFLOAD_UDP_TSO          |
    DEV_TX_OFFLOAD_OUTER_IPV4_CKSUM |
    DEV_TX_OFFLOAD_QINQ_INSERT      |
    DEV_TX_OFFLOAD_VXLAN_TNL_TSO    |
    DEV_TX_OFFLOAD_GRE_TNL_TSO      |
    DEV_TX_OFFLOAD_IPIP_TNL_TSO     |
    DEV_TX_OFFLOAD_GENEVE_TNL_TSO   |
    DEV_TX_OFFLOAD_MACSEC_INSERT;

  _dev_info.default_txconf.offloads =
    _dev_info.tx_offload_capa & tx_offloads_wanted;

  /* for port configuration all features are off by default */
  rte_eth_conf port_conf = { 0 };

  /* setting tx offloads for port */
  port_conf.txmode.offloads = _dev_info.default_txconf.offloads;

  ldout(cct, 5) << __func__ << " Port " << int(_port_idx) << ": max_rx_queues "
                << _dev_info.max_rx_queues << "  max_tx_queues "
                << _dev_info.max_tx_queues << dendl;

  _num_queues = std::min({_num_queues, _dev_info.max_rx_queues, _dev_info.max_tx_queues});

  ldout(cct, 5) << __func__ << " Port " << int(_port_idx) << ": using "
                << _num_queues << " queues" << dendl;

  // Set RSS mode: enable RSS if seastar is configured with more than 1 CPU.
  // Even if port has a single queue we still want the RSS feature to be
  // available in order to make HW calculate RSS hash for us.
  if (_num_queues > 1) {
    if (_dev_info.hash_key_size == 40) {
      _rss_key = default_rsskey_40bytes;
    } else if (_dev_info.hash_key_size == 52) {
      _rss_key = default_rsskey_52bytes;
    } else if (_dev_info.hash_key_size != 0) {
      lderr(cct) << "Port " << int(_port_idx)
	         << ": We support only 40 or 52 bytes RSS hash keys, "
	         << int(_dev_info.hash_key_size) << " bytes key requested"
	         << dendl;
      return -EINVAL;
    } else {
      _rss_key = default_rsskey_40bytes;
      _dev_info.hash_key_size = 40;
    }

    port_conf.rxmode.mq_mode = ETH_MQ_RX_RSS;
    /* enable all supported rss offloads */
    port_conf.rx_adv_conf.rss_conf.rss_hf = _dev_info.flow_type_rss_offloads;
    if (_dev_info.hash_key_size) {
      port_conf.rx_adv_conf.rss_conf.rss_key = const_cast<uint8_t *>(_rss_key.data());
      port_conf.rx_adv_conf.rss_conf.rss_key_len = _dev_info.hash_key_size;
    }
  } else {
    port_conf.rxmode.mq_mode = ETH_MQ_RX_NONE;
  }

  if (_num_queues > 1) {
    if (_dev_info.reta_size) {
      // RETA size should be a power of 2
      ceph_assert((_dev_info.reta_size & (_dev_info.reta_size - 1)) == 0);

      // Set the RSS table to the correct size
      _redir_table.resize(_dev_info.reta_size);
      _rss_table_bits = std::lround(std::log2(_dev_info.reta_size));
      ldout(cct, 5) << __func__ << " Port " << int(_port_idx)
                    << ": RSS table size is " << _dev_info.reta_size << dendl;
    } else {
      // FIXME: same with sw_reta
      _redir_table.resize(128);
      _rss_table_bits = std::lround(std::log2(128));
    }
  } else {
    _redir_table.push_back(0);
  }

  // Set Rx VLAN stripping
  if (_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_VLAN_STRIP) {
    port_conf.rxmode.offloads |= DEV_RX_OFFLOAD_VLAN_STRIP;
  }

#ifdef RTE_ETHDEV_HAS_LRO_SUPPORT
  // Enable LRO
  if (_use_lro && (_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_TCP_LRO)) {
    ldout(cct, 1) << __func__ << " LRO is on" << dendl;
    port_conf.rxmode.offloads |= DEV_RX_OFFLOAD_TCP_LRO;
    _hw_features.rx_lro = true;
  } else
#endif
    ldout(cct, 1) << __func__ << " LRO is off" << dendl;

  // Check that all CSUM features are either all set all together or not set
  // all together. If this assumption breaks we need to rework the below logic
  // by splitting the csum offload feature bit into separate bits for IPv4,
  // TCP.
  ceph_assert(((_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_IPV4_CKSUM) &&
          (_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_TCP_CKSUM)) ||
         (!(_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_IPV4_CKSUM) &&
          !(_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_TCP_CKSUM)));

  // Set Rx checksum checking
  if ((_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_IPV4_CKSUM) &&
      (_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_TCP_CKSUM)) {
    ldout(cct, 1) << __func__ << " RX checksum offload supported" << dendl;
    port_conf.rxmode.offloads |= DEV_RX_OFFLOAD_CHECKSUM;
    _hw_features.rx_csum_offload = 1;
  }

  if ((_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_IPV4_CKSUM)) {
    ldout(cct, 1) << __func__ << " TX ip checksum offload supported" << dendl;
    _hw_features.tx_csum_ip_offload = 1;
  }

  // TSO is supported starting from DPDK v1.8
  // TSO is abnormal in some DPDK versions (eg.dpdk-20.11-3.e18.aarch64), try
  // disable TSO by ms_dpdk_enable_tso=false
  if ((_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_TCP_TSO) &&
       cct->_conf.get_val<bool>("ms_dpdk_enable_tso")) {
    ldout(cct, 1) << __func__ << " TSO is supported" << dendl;
    _hw_features.tx_tso = 1;
  }

  // Check that Tx TCP CSUM features are either all set all together
  // or not set all together. If this assumption breaks we need to rework the
  // below logic by splitting the csum offload feature bit into separate bits
  // for TCP.
  ceph_assert((_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_TCP_CKSUM) ||
          !(_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_TCP_CKSUM));

  if (_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_TCP_CKSUM) {
    ldout(cct, 1) << __func__ << " TX TCP checksum offload supported" << dendl;
    _hw_features.tx_csum_l4_offload = 1;
  }

  int retval;

  ldout(cct, 1) << __func__ << " Port " << int(_port_idx) << " init ... " << dendl;

  /*
   * Standard DPDK port initialisation - config port, then set up
   * rx and tx rings.
   */
  if ((retval = rte_eth_dev_configure(_port_idx, _num_queues, _num_queues,
                                      &port_conf)) != 0) {
    lderr(cct) << __func__ << " failed to configure port " << (int)_port_idx
               << " rx/tx queues " << _num_queues << " error " << cpp_strerror(retval) << dendl;
    return retval;
  }

  //rte_eth_promiscuous_enable(port_num);
  ldout(cct, 1) << __func__ << " done." << dendl;

  return 0;
}

void DPDKDevice::set_hw_flow_control()
{
  // Read the port's current/default flow control settings
  struct rte_eth_fc_conf fc_conf;
  auto ret = rte_eth_dev_flow_ctrl_get(_port_idx, &fc_conf);

  if (ret == -ENOTSUP) {
    ldout(cct, 1) << __func__ << " port " << int(_port_idx)
                  << ": not support to get hardware flow control settings: " << ret << dendl;
    goto not_supported;
  }

  if (ret < 0) {
    lderr(cct) << __func__ << " port " << int(_port_idx)
               << ": failed to get hardware flow control settings: " << ret << dendl;
    ceph_abort();
  }

  if (_enable_fc) {
    fc_conf.mode = RTE_FC_FULL;
  } else {
    fc_conf.mode = RTE_FC_NONE;
  }

  ret = rte_eth_dev_flow_ctrl_set(_port_idx, &fc_conf);
  if (ret == -ENOTSUP) {
    ldout(cct, 1) << __func__ << " port " << int(_port_idx)
                  << ": not support to set hardware flow control settings: " << ret << dendl;
    goto not_supported;
  }

  if (ret < 0) {
    lderr(cct) << __func__ << " port " << int(_port_idx)
               << ": failed to set hardware flow control settings: " << ret << dendl;
    ceph_abort();
  }

  ldout(cct, 1) << __func__ << " port " << int(_port_idx) << ":  HW FC " << _enable_fc << dendl;
  return;

not_supported:
  ldout(cct, 1) << __func__ << " port " << int(_port_idx) << ": changing HW FC settings is not supported" << dendl;
}

class XstatSocketHook : public AdminSocketHook {
  DPDKDevice *dev;
 public:
  explicit XstatSocketHook(DPDKDevice *dev) : dev(dev) {}
  int call(std::string_view prefix, const cmdmap_t& cmdmap,
           Formatter *f,
           std::ostream& ss,
           bufferlist& out) override {
    if (prefix == "show_pmd_stats") {
      dev->nic_stats_dump(f);
    } else if (prefix == "show_pmd_xstats") {
      dev->nic_xstats_dump(f);
    }
    return 0;
  }
};

int DPDKDevice::init_port_fini()
{
  // Changing FC requires HW reset, so set it before the port is initialized.
  set_hw_flow_control();

  if (rte_eth_dev_start(_port_idx) != 0) {
    lderr(cct) << __func__ << " can't start port " << _port_idx << dendl;
    return -1;
  }

  if (_num_queues > 1)
    set_rss_table();

  // Wait for a link
  if (check_port_link_status() < 0) {
    lderr(cct) << __func__ << " port link up failed " << _port_idx << dendl;
    return -1;
  }

  ldout(cct, 5) << __func__ << " created DPDK device" << dendl;
  AdminSocket *admin_socket = cct->get_admin_socket();
  dfx_hook = std::make_unique<XstatSocketHook>(this);
  int r = admin_socket->register_command("show_pmd_stats", dfx_hook.get(),
                                         "show pmd stats statistics");
  ceph_assert(r == 0);
  r = admin_socket->register_command("show_pmd_xstats", dfx_hook.get(),
                                   "show pmd xstats statistics");
  ceph_assert(r == 0);
  return 0;
}

void DPDKDevice::set_rss_table()
{
  struct rte_flow_attr attr;
  struct rte_flow_item pattern[1];
  struct rte_flow_action action[2];
  struct rte_flow_action_rss rss_conf;

  /*
   * set the rule attribute.
   * in this case only ingress packets will be checked.
   */
  memset(&attr, 0, sizeof(struct rte_flow_attr));
  attr.ingress = 1;

  /* the final level must be always type end */
  pattern[0].type = RTE_FLOW_ITEM_TYPE_END;

  /*
   * create the action sequence.
   * one action only,  set rss hash func to toeplitz.
   */
  uint16_t i = 0;
  for (auto& r : _redir_table) {
    r = i++ % _num_queues;
  }
  rss_conf.func = RTE_ETH_HASH_FUNCTION_TOEPLITZ;
  rss_conf.types = ETH_RSS_FRAG_IPV4 | ETH_RSS_NONFRAG_IPV4_TCP;
  rss_conf.queue_num = _num_queues;
  rss_conf.queue = const_cast<uint16_t *>(_redir_table.data());
  rss_conf.key_len = _dev_info.hash_key_size;
  rss_conf.key = const_cast<uint8_t *>(_rss_key.data());
  rss_conf.level = 0;
  action[0].type = RTE_FLOW_ACTION_TYPE_RSS;
  action[0].conf = &rss_conf;
  action[1].type = RTE_FLOW_ACTION_TYPE_END;

  if (rte_flow_validate(_port_idx, &attr, pattern, action, nullptr) == 0)
    _flow = rte_flow_create(_port_idx, &attr, pattern, action, nullptr);
  else
    ldout(cct, 0) << __func__ << " Port " << _port_idx
                  << ": flow rss func configuration is unsupported"
                  << dendl;
}

void DPDKQueuePair::configure_proxies(const std::map<unsigned, float>& cpu_weights) {
  ceph_assert(!cpu_weights.empty());
  if (cpu_weights.size() == 1 && cpu_weights.begin()->first == _qid) {
    // special case queue sending to self only, to avoid requiring a hash value
    return;
  }
  register_packet_provider([this] {
    std::optional<Packet> p;
    if (!_proxy_packetq.empty()) {
      p = std::move(_proxy_packetq.front());
      _proxy_packetq.pop_front();
    }
    return p;
  });
  build_sw_reta(cpu_weights);
}

void DPDKQueuePair::build_sw_reta(const std::map<unsigned, float>& cpu_weights) {
  float total_weight = 0;
  for (auto&& x : cpu_weights) {
    total_weight += x.second;
  }
  float accum = 0;
  unsigned idx = 0;
  std::array<uint8_t, 128> reta;
  for (auto&& entry : cpu_weights) {
    auto cpu = entry.first;
    auto weight = entry.second;
    accum += weight;
    while (idx < (accum / total_weight * reta.size() - 0.5)) {
      reta[idx++] = cpu;
    }
  }
  _sw_reta = reta;
}


bool DPDKQueuePair::init_rx_mbuf_pool()
{
  std::string name = std::string(pktmbuf_pool_name) + std::to_string(_qid) + "_rx";

  // reserve the memory for Rx buffers containers
  _rx_free_pkts.reserve(mbufs_per_queue_rx);
  _rx_free_bufs.reserve(mbufs_per_queue_rx);

  _pktmbuf_pool_rx = rte_mempool_lookup(name.c_str());
  if (!_pktmbuf_pool_rx) {
    ldout(cct, 1) << __func__ << " Creating Rx mbuf pool '" << name.c_str()
                  << "' [" << mbufs_per_queue_rx << " mbufs] ..."<< dendl;

    //
    // Don't pass single-producer/single-consumer flags to mbuf create as it
    // seems faster to use a cache instead.
    //
    struct rte_pktmbuf_pool_private roomsz = {};
    roomsz.mbuf_data_room_size = mbuf_data_size + RTE_PKTMBUF_HEADROOM;
    _pktmbuf_pool_rx = rte_mempool_create(
        name.c_str(),
        mbufs_per_queue_rx, mbuf_overhead + mbuf_data_size,
        mbuf_cache_size,
        sizeof(struct rte_pktmbuf_pool_private),
        rte_pktmbuf_pool_init, as_cookie(roomsz),
        rte_pktmbuf_init, nullptr,
        rte_socket_id(), 0);
    if (!_pktmbuf_pool_rx) {
      lderr(cct) << __func__ << " Failed to create mempool for rx" << dendl;
      return false;
    }

    //
    // allocate more data buffer
    int bufs_count =  cct->_conf->ms_dpdk_rx_buffer_count_per_core - mbufs_per_queue_rx;
    int mz_flags = RTE_MEMZONE_1GB|RTE_MEMZONE_SIZE_HINT_ONLY;
    std::string mz_name = "rx_buffer_data" + std::to_string(_qid);
    const struct rte_memzone *mz = rte_memzone_reserve_aligned(mz_name.c_str(),
          mbuf_data_size*bufs_count, _pktmbuf_pool_rx->socket_id, mz_flags, mbuf_data_size);
    ceph_assert(mz);
    void* m = mz->addr;
    for (int i = 0; i < bufs_count; i++) {
      ceph_assert(m);
      _alloc_bufs.push_back(m);
      m += mbuf_data_size;
    }

    if (rte_eth_rx_queue_setup(_dev_port_idx, _qid, default_ring_size,
                               rte_eth_dev_socket_id(_dev_port_idx),
                               _dev->def_rx_conf(), _pktmbuf_pool_rx) < 0) {
      lderr(cct) << __func__ << " cannot initialize rx queue" << dendl;
      return false;
    }
  }

  return _pktmbuf_pool_rx != nullptr;
}

int DPDKDevice::check_port_link_status()
{
  int count = 0;

  ldout(cct, 20) << __func__ << dendl;
  const int sleep_time = 100 * 1000;
  const int max_check_time = 90;  /* 9s (90 * 100ms) in total */
  while (true) {
    struct rte_eth_link link;
    memset(&link, 0, sizeof(link));
    rte_eth_link_get_nowait(_port_idx, &link);

    if (true) {
      if (link.link_status) {
        ldout(cct, 5) << __func__ << " done port "
                      << static_cast<unsigned>(_port_idx)
                      << " link Up - speed " << link.link_speed
                      << " Mbps - "
                      << ((link.link_duplex == ETH_LINK_FULL_DUPLEX) ? ("full-duplex") : ("half-duplex\n"))
                      << dendl;
        break;
      } else if (count++ < max_check_time) {
        ldout(cct, 20) << __func__ << " not ready, continue to wait." << dendl;
        usleep(sleep_time);
      } else {
        lderr(cct) << __func__ << " done port " << _port_idx << " link down" << dendl;
        return -1;
      }
    }
  }
  return 0;
}

class C_handle_dev_stats : public EventCallback {
  DPDKQueuePair *_qp;
 public:
  C_handle_dev_stats(DPDKQueuePair *qp): _qp(qp) { }
  void do_request(uint64_t id) {
    _qp->handle_stats();
  }
};

DPDKQueuePair::DPDKQueuePair(CephContext *c, EventCenter *cen, DPDKDevice* dev, uint8_t qid)
  : cct(c), _dev(dev), _dev_port_idx(dev->port_idx()), center(cen), _qid(qid),
    _tx_poller(this), _rx_gc_poller(this), _tx_buf_factory(c, dev, qid),
    _tx_gc_poller(this)
{
  if (!init_rx_mbuf_pool()) {
    lderr(cct) << __func__ << " cannot initialize mbuf pools" << dendl;
    ceph_abort();
  }

  static_assert(offsetof(tx_buf, private_end) -
                offsetof(tx_buf, private_start) <= RTE_PKTMBUF_HEADROOM,
                "RTE_PKTMBUF_HEADROOM is less than DPDKQueuePair::tx_buf size! "
                "Increase the headroom size in the DPDK configuration");
  static_assert(offsetof(tx_buf, _mbuf) == 0,
                "There is a pad at the beginning of the tx_buf before _mbuf "
                "field!");
  static_assert((inline_mbuf_data_size & (inline_mbuf_data_size - 1)) == 0,
                "inline_mbuf_data_size has to be a power of two!");

  std::string name(std::string("queue") + std::to_string(qid));
  PerfCountersBuilder plb(cct, name, l_dpdk_qp_first, l_dpdk_qp_last);

  plb.add_u64_counter(l_dpdk_qp_rx_packets, "dpdk_receive_packets", "DPDK received packets");
  plb.add_u64_counter(l_dpdk_qp_tx_packets, "dpdk_send_packets", "DPDK sendd packets");
  plb.add_u64_counter(l_dpdk_qp_rx_bad_checksum_errors, "dpdk_receive_bad_checksum_errors", "DPDK received bad checksum packets");
  plb.add_u64_counter(l_dpdk_qp_rx_no_memory_errors, "dpdk_receive_no_memory_errors", "DPDK received no memory packets");
  plb.add_u64_counter(l_dpdk_qp_rx_bytes, "dpdk_receive_bytes", "DPDK received bytes", NULL, 0, unit_t(UNIT_BYTES));
  plb.add_u64_counter(l_dpdk_qp_tx_bytes, "dpdk_send_bytes", "DPDK sendd bytes", NULL, 0, unit_t(UNIT_BYTES));
  plb.add_u64_counter(l_dpdk_qp_rx_last_bunch, "dpdk_receive_last_bunch", "DPDK last received bunch");
  plb.add_u64_counter(l_dpdk_qp_tx_last_bunch, "dpdk_send_last_bunch", "DPDK last send bunch");
  plb.add_u64_counter(l_dpdk_qp_rx_fragments, "dpdk_receive_fragments", "DPDK received total fragments");
  plb.add_u64_counter(l_dpdk_qp_tx_fragments, "dpdk_send_fragments", "DPDK sendd total fragments");
  plb.add_u64_counter(l_dpdk_qp_rx_copy_ops, "dpdk_receive_copy_ops", "DPDK received copy operations");
  plb.add_u64_counter(l_dpdk_qp_tx_copy_ops, "dpdk_send_copy_ops", "DPDK sendd copy operations");
  plb.add_u64_counter(l_dpdk_qp_rx_copy_bytes, "dpdk_receive_copy_bytes", "DPDK received copy bytes", NULL, 0, unit_t(UNIT_BYTES));
  plb.add_u64_counter(l_dpdk_qp_tx_copy_bytes, "dpdk_send_copy_bytes", "DPDK send copy bytes", NULL, 0, unit_t(UNIT_BYTES));
  plb.add_u64_counter(l_dpdk_qp_rx_linearize_ops, "dpdk_receive_linearize_ops", "DPDK received linearize operations");
  plb.add_u64_counter(l_dpdk_qp_tx_linearize_ops, "dpdk_send_linearize_ops", "DPDK send linearize operations");
  plb.add_u64_counter(l_dpdk_qp_tx_queue_length, "dpdk_send_queue_length", "DPDK send queue length");

  perf_logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(perf_logger);

  if (!_qid)
    device_stat_time_fd = center->create_time_event(1000*1000, new C_handle_dev_stats(this));
}

void DPDKDevice::nic_stats_dump(Formatter *f)
{
  static uint64_t prev_pkts_rx[RTE_MAX_ETHPORTS];
  static uint64_t prev_pkts_tx[RTE_MAX_ETHPORTS];
  static uint64_t prev_cycles[RTE_MAX_ETHPORTS];
  size_t tx_fragments = 0;
  size_t rx_fragments = 0;
  size_t tx_free_cnt = 0;
  size_t rx_free_cnt = 0;

  for (auto &qp: _queues) {
    tx_fragments += qp->perf_logger->get(l_dpdk_qp_tx_fragments);
    rx_fragments += qp->perf_logger->get(l_dpdk_qp_rx_fragments);
    tx_free_cnt += qp->_tx_buf_factory.ring_size();
    rx_free_cnt += rte_mempool_avail_count(qp->_pktmbuf_pool_rx);
  }
  struct rte_eth_stats stats;
  rte_eth_stats_get(_port_idx, &stats);
  f->open_object_section("RX");
  f->dump_unsigned("in_packets", stats.ipackets);
  f->dump_unsigned("recv_packets", rx_fragments);
  f->dump_unsigned("in_bytes", stats.ibytes);
  f->dump_unsigned("missed", stats.imissed);
  f->dump_unsigned("errors", stats.ierrors);
  f->close_section();

  f->open_object_section("TX");
  f->dump_unsigned("out_packets", stats.opackets);
  f->dump_unsigned("send_packets", tx_fragments);
  f->dump_unsigned("out_bytes", stats.obytes);
  f->dump_unsigned("errors", stats.oerrors);
  f->close_section();

  f->open_object_section("stats");
  f->dump_unsigned("RX_nombuf", stats.rx_nombuf);
  f->dump_unsigned("RX_avail_mbufs", rx_free_cnt);
  f->dump_unsigned("TX_avail_mbufs", tx_free_cnt);

  uint64_t diff_cycles = prev_cycles[_port_idx];
  prev_cycles[_port_idx] = rte_rdtsc();
  if (diff_cycles > 0) {
    diff_cycles = prev_cycles[_port_idx] - diff_cycles;
  }

  uint64_t diff_pkts_rx = (stats.ipackets > prev_pkts_rx[_port_idx]) ?
	         (stats.ipackets - prev_pkts_rx[_port_idx]) : 0;
  uint64_t diff_pkts_tx = (stats.opackets > prev_pkts_tx[_port_idx]) ?
	         (stats.opackets - prev_pkts_tx[_port_idx]) : 0;
  prev_pkts_rx[_port_idx] = stats.ipackets;
  prev_pkts_tx[_port_idx] = stats.opackets;
  uint64_t mpps_rx = diff_cycles > 0 ? diff_pkts_rx * rte_get_tsc_hz() / diff_cycles : 0;
  uint64_t mpps_tx = diff_cycles > 0 ? diff_pkts_tx * rte_get_tsc_hz() / diff_cycles : 0;
  f->dump_unsigned("Rx_pps", mpps_rx);
  f->dump_unsigned("Tx_pps", mpps_tx);
  f->close_section();
}

void DPDKDevice::nic_xstats_dump(Formatter *f)
{
  // Get count
  int cnt_xstats = rte_eth_xstats_get_names(_port_idx, NULL, 0);
  if (cnt_xstats < 0) {
    ldout(cct, 1) << "Error: Cannot get count of xstats" << dendl;
    return;
  }
 
  // Get id-name lookup table
  std::vector<struct rte_eth_xstat_name> xstats_names(cnt_xstats);
  if (cnt_xstats != rte_eth_xstats_get_names(_port_idx, xstats_names.data(), cnt_xstats)) {
    ldout(cct, 1) << "Error: Cannot get xstats lookup" << dendl;
    return;
  }

  // Get stats themselves
  std::vector<struct rte_eth_xstat> xstats(cnt_xstats);
  if (cnt_xstats != rte_eth_xstats_get(_port_idx, xstats.data(), cnt_xstats)) {
    ldout(cct, 1) << "Error: Unable to get xstats" << dendl;
    return;
  }
  f->open_object_section("xstats");
  for (int i = 0; i < cnt_xstats; i++){
    f->dump_unsigned(xstats_names[i].name, xstats[i].value);
  }
  f->close_section();
}

void DPDKQueuePair::handle_stats()
{
  ldout(cct, 20) << __func__ << " started." << dendl;
  rte_eth_stats rte_stats = {};
  int rc = rte_eth_stats_get(_dev_port_idx, &rte_stats);

  if (rc) {
    ldout(cct, 0) << __func__ << " failed to get port statistics: " << cpp_strerror(rc) << dendl;
    return ;
  }

#if RTE_VERSION < RTE_VERSION_NUM(16,7,0,0)
  _dev->perf_logger->set(l_dpdk_dev_rx_mcast, rte_stats.imcasts);
  _dev->perf_logger->set(l_dpdk_dev_rx_badcrc_errors, rte_stats.ibadcrc);
#endif
  _dev->perf_logger->set(l_dpdk_dev_rx_dropped_errors, rte_stats.imissed);
  _dev->perf_logger->set(l_dpdk_dev_rx_nombuf_errors, rte_stats.rx_nombuf);

  _dev->perf_logger->set(l_dpdk_dev_rx_total_errors, rte_stats.ierrors);
  _dev->perf_logger->set(l_dpdk_dev_tx_total_errors, rte_stats.oerrors);
  device_stat_time_fd = center->create_time_event(1000*1000, new C_handle_dev_stats(this));
}

bool DPDKQueuePair::poll_tx() {
  bool nonloopback = !cct->_conf->ms_dpdk_debug_allow_loopback;
#ifdef CEPH_PERF_DEV
  uint64_t start = Cycles::rdtsc();
#endif
  uint32_t total_work = 0;
  if (_tx_packetq.size() < 16) {
    // refill send queue from upper layers
    uint32_t work;
    do {
      work = 0;
      for (auto&& pr : _pkt_providers) {
        auto p = pr();
        if (p) {
          work++;
          if (likely(nonloopback)) {
            // ldout(cct, 0) << __func__ << " len: " << p->len() << " frags: " << p->nr_frags() << dendl;
            _tx_packetq.push_back(std::move(*p));
          } else {
            auto th = p->get_header<eth_hdr>(0);
            if (th->dst_mac == th->src_mac) {
              _dev->l2receive(_qid, std::move(*p));
            } else {
              _tx_packetq.push_back(std::move(*p));
            }
          }
          if (_tx_packetq.size() == 128) {
            break;
          }
        }
      }
      total_work += work;
    } while (work && total_work < 256 && _tx_packetq.size() < 128);
  }
  if (!_tx_packetq.empty()) {
    uint64_t c = send(_tx_packetq);
    perf_logger->inc(l_dpdk_qp_tx_packets, c);
    perf_logger->set(l_dpdk_qp_tx_last_bunch, c);
#ifdef CEPH_PERF_DEV
    tx_count += total_work;
    tx_cycles += Cycles::rdtsc() - start;
#endif
    return true;
  }

  return false;
}

inline std::optional<Packet> DPDKQueuePair::from_mbuf_lro(rte_mbuf* m)
{
  _frags.clear();
  _bufs.clear();

  for (; m != nullptr; m = m->next) {
    char* data = rte_pktmbuf_mtod(m, char*);

    _frags.emplace_back(fragment{data, rte_pktmbuf_data_len(m)});
    _bufs.push_back(data);
  }

  auto del = std::bind(
          [this](std::vector<char*> &bufs) {
            for (auto&& b : bufs) { _alloc_bufs.push_back(b); }
          }, std::move(_bufs));
  return Packet(
      _frags.begin(), _frags.end(), make_deleter(std::move(del)));
}

inline std::optional<Packet> DPDKQueuePair::from_mbuf(rte_mbuf* m)
{
  _rx_free_pkts.push_back(m);
  _num_rx_free_segs += m->nb_segs;

  if (!_dev->hw_features_ref().rx_lro || rte_pktmbuf_is_contiguous(m)) {
    char* data = rte_pktmbuf_mtod(m, char*);

    return Packet(fragment{data, rte_pktmbuf_data_len(m)},
                  make_deleter([this, data] { _alloc_bufs.push_back(data); }));
  } else {
    return from_mbuf_lro(m);
  }
}

inline bool DPDKQueuePair::refill_one_cluster(rte_mbuf* head)
{
  for (; head != nullptr; head = head->next) {
    if (!refill_rx_mbuf(head, mbuf_data_size, _alloc_bufs)) {
      //
      // If we failed to allocate a new buffer - push the rest of the
      // cluster back to the free_packets list for a later retry.
      //
      _rx_free_pkts.push_back(head);
      return false;
    }
    _rx_free_bufs.push_back(head);
  }

  return true;
}

bool DPDKQueuePair::rx_gc(bool force)
{
  if (_num_rx_free_segs >= rx_gc_thresh || force) {
    ldout(cct, 10) << __func__ << " free segs " << _num_rx_free_segs
                   << " thresh " << rx_gc_thresh
                   << " free pkts " << _rx_free_pkts.size()
                   << dendl;

    while (!_rx_free_pkts.empty()) {
      //
      // Use back() + pop_back() semantics to avoid an extra
      // _rx_free_pkts.clear() at the end of the function - clear() has a
      // linear complexity.
      //
      auto m = _rx_free_pkts.back();
      _rx_free_pkts.pop_back();

      if (!refill_one_cluster(m)) {
        ldout(cct, 1) << __func__ << " get new mbuf failed " << dendl;
        break;
      }
    }
    for (auto&& m : _rx_free_bufs) {
      rte_pktmbuf_prefree_seg(m);
    }

    if (_rx_free_bufs.size()) {
      rte_mempool_put_bulk(_pktmbuf_pool_rx,
                           (void **)_rx_free_bufs.data(),
                           _rx_free_bufs.size());

      // TODO: ceph_assert() in a fast path! Remove me ASAP!
      ceph_assert(_num_rx_free_segs >= _rx_free_bufs.size());

      _num_rx_free_segs -= _rx_free_bufs.size();
      _rx_free_bufs.clear();

      // TODO: ceph_assert() in a fast path! Remove me ASAP!
      ceph_assert((_rx_free_pkts.empty() && !_num_rx_free_segs) ||
             (!_rx_free_pkts.empty() && _num_rx_free_segs));
    }
  }

  return _num_rx_free_segs >= rx_gc_thresh;
}


void DPDKQueuePair::process_packets(
    struct rte_mbuf **bufs, uint16_t count)
{
  uint64_t nr_frags = 0, bytes = 0;

  for (uint16_t i = 0; i < count; i++) {
    struct rte_mbuf *m = bufs[i];
    offload_info oi;

    std::optional<Packet> p = from_mbuf(m);

    // Drop the packet if translation above has failed
    if (!p) {
      perf_logger->inc(l_dpdk_qp_rx_no_memory_errors);
      continue;
    }
    // ldout(cct, 0) << __func__ << " len " << p->len() << " " << dendl;

    nr_frags += m->nb_segs;
    bytes    += m->pkt_len;

    // Set stipped VLAN value if available
    if ((_dev->_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_VLAN_STRIP) &&
        (m->ol_flags & PKT_RX_VLAN_STRIPPED)) {
      oi.vlan_tci = m->vlan_tci;
    }

    if (_dev->get_hw_features().rx_csum_offload) {
      if (m->ol_flags & (PKT_RX_IP_CKSUM_BAD | PKT_RX_L4_CKSUM_BAD)) {
        // Packet with bad checksum, just drop it.
        perf_logger->inc(l_dpdk_qp_rx_bad_checksum_errors);
        continue;
      }
      // Note that when _hw_features.rx_csum_offload is on, the receive
      // code for ip, tcp and udp will assume they don't need to check
      // the checksum again, because we did this here.
    }

    p->set_offload_info(oi);
    if (m->ol_flags & PKT_RX_RSS_HASH) {
      p->set_rss_hash(m->hash.rss);
    }

    _dev->l2receive(_qid, std::move(*p));
  }

  perf_logger->inc(l_dpdk_qp_rx_packets, count);
  perf_logger->set(l_dpdk_qp_rx_last_bunch, count);
  perf_logger->inc(l_dpdk_qp_rx_fragments, nr_frags);
  perf_logger->inc(l_dpdk_qp_rx_bytes, bytes);
}

bool DPDKQueuePair::poll_rx_once()
{
  struct rte_mbuf *buf[packet_read_size];

  /* read a port */
#ifdef CEPH_PERF_DEV
  uint64_t start = Cycles::rdtsc();
#endif
  uint16_t count = rte_eth_rx_burst(_dev_port_idx, _qid,
                                       buf, packet_read_size);

  /* Now process the NIC packets read */
  if (likely(count > 0)) {
    process_packets(buf, count);
#ifdef CEPH_PERF_DEV
    rx_cycles = Cycles::rdtsc() - start;
    rx_count += count;
#endif
  }
#ifdef CEPH_PERF_DEV
  else {
    if (rx_count > 10000 && tx_count) {
      ldout(cct, 0) << __func__ << " rx count=" << rx_count << " avg rx=" << Cycles::to_nanoseconds(rx_cycles)/rx_count << "ns "
                    << " tx count=" << tx_count << " avg tx=" << Cycles::to_nanoseconds(tx_cycles)/tx_count << "ns"
                    << dendl;
      rx_count = rx_cycles = tx_count = tx_cycles = 0;
    }
  }
#endif

  return count;
}

DPDKQueuePair::tx_buf_factory::tx_buf_factory(CephContext *c,
        DPDKDevice *dev, uint8_t qid): cct(c)
{
  std::string name = std::string(pktmbuf_pool_name) + std::to_string(qid) + "_tx";

  _pool = rte_mempool_lookup(name.c_str());
  if (!_pool) {
    ldout(cct, 0) << __func__ << " Creating Tx mbuf pool '" << name.c_str()
                  << "' [" << mbufs_per_queue_tx << " mbufs] ..." << dendl;
    //
    // We are going to push the buffers from the mempool into
    // the circular_buffer and then poll them from there anyway, so
    // we prefer to make a mempool non-atomic in this case.
    //
    _pool = rte_mempool_create(name.c_str(),
                               mbufs_per_queue_tx, inline_mbuf_size,
                               mbuf_cache_size,
                               sizeof(struct rte_pktmbuf_pool_private),
                               rte_pktmbuf_pool_init, nullptr,
                               rte_pktmbuf_init, nullptr,
                               rte_socket_id(), 0);

    if (!_pool) {
      lderr(cct) << __func__ << " Failed to create mempool for Tx" << dendl;
      ceph_abort();
    }
    if (rte_eth_tx_queue_setup(dev->port_idx(), qid, default_ring_size,
                               rte_eth_dev_socket_id(dev->port_idx()),
                               dev->def_tx_conf()) < 0) {
      lderr(cct) << __func__ << " cannot initialize tx queue" << dendl;
      ceph_abort();
    }
  }

  //
  // Fill the factory with the buffers from the mempool allocated
  // above.
  //
  init_factory();
}

bool DPDKQueuePair::tx_buf::i40e_should_linearize(rte_mbuf *head)
{
  bool is_tso = head->ol_flags & PKT_TX_TCP_SEG;

  // For a non-TSO case: number of fragments should not exceed 8
  if (!is_tso){
    return head->nb_segs > i40e_max_xmit_segment_frags;
  }

  //
  // For a TSO case each MSS window should not include more than 8
  // fragments including headers.
  //

  // Calculate the number of frags containing headers.
  //
  // Note: we support neither VLAN nor tunneling thus headers size
  // accounting is super simple.
  //
  size_t headers_size = head->l2_len + head->l3_len + head->l4_len;
  unsigned hdr_frags = 0;
  size_t cur_payload_len = 0;
  rte_mbuf *cur_seg = head;

  while (cur_seg && cur_payload_len < headers_size) {
    cur_payload_len += cur_seg->data_len;
    cur_seg = cur_seg->next;
    hdr_frags++;
  }

  //
  // Header fragments will be used for each TSO segment, thus the
  // maximum number of data segments will be 8 minus the number of
  // header fragments.
  //
  // It's unclear from the spec how the first TSO segment is treated
  // if the last fragment with headers contains some data bytes:
  // whether this fragment will be accounted as a single fragment or
  // as two separate fragments. We prefer to play it safe and assume
  // that this fragment will be accounted as two separate fragments.
  //
  size_t max_win_size = i40e_max_xmit_segment_frags - hdr_frags;

  if (head->nb_segs <= max_win_size) {
    return false;
  }

  // Get the data (without headers) part of the first data fragment
  size_t prev_frag_data = cur_payload_len - headers_size;
  auto mss = head->tso_segsz;

  while (cur_seg) {
    unsigned frags_in_seg = 0;
    size_t cur_seg_size = 0;

    if (prev_frag_data) {
      cur_seg_size = prev_frag_data;
      frags_in_seg++;
      prev_frag_data = 0;
    }

    while (cur_seg_size < mss && cur_seg) {
      cur_seg_size += cur_seg->data_len;
      cur_seg = cur_seg->next;
      frags_in_seg++;

      if (frags_in_seg > max_win_size) {
        return true;
      }
    }

    if (cur_seg_size > mss) {
      prev_frag_data = cur_seg_size - mss;
    }
  }

  return false;
}

void DPDKQueuePair::tx_buf::set_cluster_offload_info(const Packet& p, const DPDKQueuePair& qp, rte_mbuf* head)
{
  // Handle TCP checksum offload
  auto oi = p.offload_info();
  if (oi.needs_ip_csum) {
    head->ol_flags |= PKT_TX_IP_CKSUM;
    // TODO: Take a VLAN header into an account here
    head->l2_len = sizeof(struct rte_ether_hdr);
    head->l3_len = oi.ip_hdr_len;
  }
  if (qp.port().get_hw_features().tx_csum_l4_offload) {
    if (oi.protocol == ip_protocol_num::tcp) {
      head->ol_flags |= PKT_TX_TCP_CKSUM;
      // TODO: Take a VLAN header into an account here
      head->l2_len = sizeof(struct rte_ether_hdr);
      head->l3_len = oi.ip_hdr_len;

      if (oi.tso_seg_size) {
        ceph_assert(oi.needs_ip_csum);
        head->ol_flags |= PKT_TX_TCP_SEG;
        head->l4_len = oi.tcp_hdr_len;
        head->tso_segsz = oi.tso_seg_size;
      }
    }
  }
}

DPDKQueuePair::tx_buf* DPDKQueuePair::tx_buf::from_packet_zc(
        CephContext *cct, Packet&& p, DPDKQueuePair& qp)
{
  // Too fragmented - linearize
  if (p.nr_frags() > max_frags) {
    p.linearize();
    qp.perf_logger->inc(l_dpdk_qp_tx_linearize_ops);
  }

 build_mbuf_cluster:
  rte_mbuf *head = nullptr, *last_seg = nullptr;
  unsigned nsegs = 0;

  //
  // Create a HEAD of the fragmented packet: check if frag0 has to be
  // copied and if yes - send it in a copy way
  //
  if (!check_frag0(p)) {
    if (!copy_one_frag(qp, p.frag(0), head, last_seg, nsegs)) {
      ldout(cct, 1) << __func__ << " no available mbuf for " << p.frag(0).size << dendl;
      return nullptr;
    }
  } else if (!translate_one_frag(qp, p.frag(0), head, last_seg, nsegs)) {
    ldout(cct, 1) << __func__ << " no available mbuf for " << p.frag(0).size << dendl;
    return nullptr;
  }

  unsigned total_nsegs = nsegs;

  for (unsigned i = 1; i < p.nr_frags(); i++) {
    rte_mbuf *h = nullptr, *new_last_seg = nullptr;
    if (!translate_one_frag(qp, p.frag(i), h, new_last_seg, nsegs)) {
      ldout(cct, 1) << __func__ << " no available mbuf for " << p.frag(i).size << dendl;
      me(head)->recycle();
      return nullptr;
    }

    total_nsegs += nsegs;

    // Attach a new buffers' chain to the packet chain
    last_seg->next = h;
    last_seg = new_last_seg;
  }

  // Update the HEAD buffer with the packet info
  head->pkt_len = p.len();
  head->nb_segs = total_nsegs;
  // tx_pkt_burst loops until the next pointer is null, so last_seg->next must
  // be null.
  last_seg->next = nullptr;

  set_cluster_offload_info(p, qp, head);

  //
  // If a packet hasn't been linearized already and the resulting
  // cluster requires the linearisation due to HW limitation:
  //
  //    - Recycle the cluster.
  //    - Linearize the packet.
  //    - Build the cluster once again
  //
  if (head->nb_segs > max_frags ||
      (p.nr_frags() > 1 && qp.port().is_i40e_device() && i40e_should_linearize(head)) ||
      (p.nr_frags() > vmxnet3_max_xmit_segment_frags && qp.port().is_vmxnet3_device())) {
    me(head)->recycle();
    p.linearize();
    qp.perf_logger->inc(l_dpdk_qp_tx_linearize_ops);

    goto build_mbuf_cluster;
  }

  me(last_seg)->set_packet(std::move(p));

  return me(head);
}

void DPDKQueuePair::tx_buf::copy_packet_to_cluster(const Packet& p, rte_mbuf* head)
{
  rte_mbuf* cur_seg = head;
  size_t cur_seg_offset = 0;
  unsigned cur_frag_idx = 0;
  size_t cur_frag_offset = 0;

  while (true) {
    size_t to_copy = std::min(p.frag(cur_frag_idx).size - cur_frag_offset,
                              inline_mbuf_data_size - cur_seg_offset);

    memcpy(rte_pktmbuf_mtod_offset(cur_seg, void*, cur_seg_offset),
           p.frag(cur_frag_idx).base + cur_frag_offset, to_copy);

    cur_frag_offset += to_copy;
    cur_seg_offset += to_copy;

    if (cur_frag_offset >= p.frag(cur_frag_idx).size) {
      ++cur_frag_idx;
      if (cur_frag_idx >= p.nr_frags()) {
        //
        // We are done - set the data size of the last segment
        // of the cluster.
        //
        cur_seg->data_len = cur_seg_offset;
        break;
      }

      cur_frag_offset = 0;
    }

    if (cur_seg_offset >= inline_mbuf_data_size) {
      cur_seg->data_len = inline_mbuf_data_size;
      cur_seg = cur_seg->next;
      cur_seg_offset = 0;

      // FIXME: assert in a fast-path - remove!!!
      ceph_assert(cur_seg);
    }
  }
}

DPDKQueuePair::tx_buf* DPDKQueuePair::tx_buf::from_packet_copy(Packet&& p, DPDKQueuePair& qp)
{
  // sanity
  if (!p.len()) {
    return nullptr;
  }

  /*
   * Here we are going to use the fact that the inline data size is a
   * power of two.
   *
   * We will first try to allocate the cluster and only if we are
   * successful - we will go and copy the data.
   */
  auto aligned_len = align_up((size_t)p.len(), inline_mbuf_data_size);
  unsigned nsegs = aligned_len / inline_mbuf_data_size;
  rte_mbuf *head = nullptr, *last_seg = nullptr;

  tx_buf* buf = qp.get_tx_buf();
  if (!buf) {
    return nullptr;
  }

  head = buf->rte_mbuf_p();
  last_seg = head;
  for (unsigned i = 1; i < nsegs; i++) {
    buf = qp.get_tx_buf();
    if (!buf) {
      me(head)->recycle();
      return nullptr;
    }

    last_seg->next = buf->rte_mbuf_p();
    last_seg = last_seg->next;
  }

  //
  // If we've got here means that we have succeeded already!
  // We only need to copy the data and set the head buffer with the
  // relevant info.
  //
  head->pkt_len = p.len();
  head->nb_segs = nsegs;
  // tx_pkt_burst loops until the next pointer is null, so last_seg->next must
  // be null.
  last_seg->next = nullptr;

  copy_packet_to_cluster(p, head);
  set_cluster_offload_info(p, qp, head);

  return me(head);
}

size_t DPDKQueuePair::tx_buf::copy_one_data_buf(
    DPDKQueuePair& qp, rte_mbuf*& m, char* data, size_t buf_len)
{
  tx_buf* buf = qp.get_tx_buf();
  if (!buf) {
    return 0;
  }

  size_t len = std::min(buf_len, inline_mbuf_data_size);

  m = buf->rte_mbuf_p();

  // mbuf_put()
  m->data_len = len;
  m->pkt_len  = len;

  qp.perf_logger->inc(l_dpdk_qp_tx_copy_ops);
  qp.perf_logger->inc(l_dpdk_qp_tx_copy_bytes, len);

  memcpy(rte_pktmbuf_mtod(m, void*), data, len);

  return len;
}

/******************************** Interface functions *************************/

std::unique_ptr<DPDKDevice> create_dpdk_net_device(
    CephContext *cct,
    unsigned cores,
    uint8_t port_idx,
    bool use_lro,
    bool enable_fc)
{
  // Check that we have at least one DPDK-able port
  if (rte_eth_dev_count_avail() == 0) {
    ceph_assert(false && "No Ethernet ports - bye\n");
  } else {
    ldout(cct, 10) << __func__ << " ports number: " << int(rte_eth_dev_count_avail()) << dendl;
  }

  return std::unique_ptr<DPDKDevice>(
      new DPDKDevice(cct, port_idx, cores, use_lro, enable_fc));
}
