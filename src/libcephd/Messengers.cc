// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#define dout_context cct
#include "Messengers.h"

#include "msg/Messenger.h"
#include "include/msgr.h"

#include "osd/OSD.h"
#include "common/pick_address.h"
#include "common/debug.h"
#include "include/color.h"

#include "messages/MPing.h"
#include "messages/MOSDMarkMeDown.h"
#include "messages/MOSDPing.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDMap.h"
#include "messages/MGenericMessage.h"
#include "messages/MOSDOpReply.h"

#define dout_subsys ceph_subsys_osd

using namespace ceph::osd;


Messengers::Messengers()
  : cluster(NULL),
    client(NULL),
    client_hb(NULL),
    front_hb(NULL),
    back_hb(NULL),
    byte_throttler(NULL),
    msg_throttler(NULL)
{
}

Messengers::~Messengers()
{
  delete cluster;
  delete client;
  delete client_hb;
  delete front_hb;
  delete back_hb;
  delete byte_throttler;
  delete msg_throttler;
}

Messenger* create_messenger(CephContext *cct, const string &type,
			    const entity_name_t &me,
			    const char *name, pid_t pid)
{
  Messenger *ms = Messenger::create(cct, type,
				    me, name, pid, 0);
  ms->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  return ms;
}

int Messengers::create(CephContext *cct, md_config_t *conf,
		       const entity_name_t &name, pid_t pid)
{
  // create messengers
  cluster = create_messenger(cct, "simple", name, "cluster", pid);
  client = create_messenger(cct, "simple", name, "client", pid);
  client_hb = create_messenger(cct, "simple", name, "hbclient", pid);
  front_hb = create_messenger(cct,"simple",  name, "hb_front", pid);
  back_hb = create_messenger(cct, "simple", name, "hb_back", pid);

  // set up policies
  byte_throttler = new Throttle(cct, "direct", conf->osd_client_message_size_cap);
  msg_throttler = new Throttle(cct, "direct", conf->osd_client_message_cap);

  uint64_t supported =
    CEPH_FEATURE_UID |
    CEPH_FEATURE_NOSRCADDR |
    CEPH_FEATURE_MSG_AUTH;

  if (client) {
    client->set_default_policy(
	Messenger::Policy::stateless_server(supported, 0));
    client->set_policy_throttlers(entity_name_t::TYPE_CLIENT,
	byte_throttler, msg_throttler);
    client->set_policy(entity_name_t::TYPE_MON,
	Messenger::Policy::lossy_client(supported,
	  CEPH_FEATURE_UID | CEPH_FEATURE_OSDENC));
    client->set_policy(entity_name_t::TYPE_OSD,
	Messenger::Policy::stateless_server(0,0));
  }

  cluster->set_default_policy(
      Messenger::Policy::stateless_server(0, 0));
  cluster->set_policy(entity_name_t::TYPE_MON,
      Messenger::Policy::lossy_client(0,0));
  cluster->set_policy(entity_name_t::TYPE_OSD,
      Messenger::Policy::lossless_peer(supported,
	CEPH_FEATURE_UID | CEPH_FEATURE_OSDENC));
  cluster->set_policy(entity_name_t::TYPE_CLIENT,
      Messenger::Policy::stateless_server(0, 0));

  client_hb->set_policy(entity_name_t::TYPE_OSD,
			   Messenger::Policy::lossy_client(0, 0));
  front_hb->set_policy(entity_name_t::TYPE_OSD,
			  Messenger::Policy::stateless_server(0, 0));
  back_hb->set_policy(entity_name_t::TYPE_OSD,
			 Messenger::Policy::stateless_server(0, 0));

  return 0;
}

int Messengers::bind(CephContext *cct, md_config_t *conf)
{
  // bind messengers
  pick_addresses(cct, CEPH_PICK_ADDRESS_PUBLIC|CEPH_PICK_ADDRESS_CLUSTER);
  dout(10) << __FUNCTION__ << ": client " << conf->public_addr
	   << ", cluster " << conf->cluster_addr  << dendl;

  if (conf->public_addr.is_blank_ip() &&
      !conf->cluster_addr.is_blank_ip()) {
    derr << TEXT_YELLOW
      << " ** WARNING: specified cluster addr but not client addr; we **\n"
      << " **          recommend you specify neither or both.         **"
      << TEXT_NORMAL << dendl;
  }

  int r = cluster->bind(conf->cluster_addr);
  if (r < 0)
    return r;
  dout(10) << "bound cluster: " << cluster->get_myaddr() << dendl;

  entity_addr_t public_addr(conf->public_addr);

  r = client->bind(public_addr);
  if (r < 0)
    return r;
  dout(10) << "bound client: " << client->get_myaddr() << dendl;
  public_addr = client->get_myaddr();

  // hb front should bind to same ip as public_addr
  entity_addr_t hb_front_addr(conf->public_addr);
  if (hb_front_addr.is_ip())
    hb_front_addr.set_port(0);
  r = front_hb->bind(hb_front_addr);
  if (r < 0)
    return r;
  dout(10) << "bound front_hb: " << front_hb->get_myaddr() << dendl;

  // hb back should bind to same ip as cluster_addr (if specified)
  entity_addr_t hb_back_addr(conf->osd_heartbeat_addr);
  if (hb_back_addr.is_blank_ip()) {
    hb_back_addr = conf->cluster_addr;
    if (hb_back_addr.is_ip())
      hb_back_addr.set_port(0);
  }
  r = back_hb->bind(hb_back_addr);
  if (r < 0)
    return r;
  dout(10) << "bound back_hb: " << back_hb->get_myaddr() << dendl;

  return 0;
}

void Messengers::start()
{
  cluster->start();
  client->start();
  client_hb->start();
  front_hb->start();
  back_hb->start();
}

void Messengers::wait()
{
  // XXX: assert(started);
  // close/wait on messengers
  cluster->wait();
  client->wait();
  client_hb->wait();
  front_hb->wait();
  back_hb->wait();
}

