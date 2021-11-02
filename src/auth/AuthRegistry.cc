// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "AuthRegistry.h"

#include "cephx/CephxAuthorizeHandler.h"
#ifdef HAVE_GSSAPI
#include "krb/KrbAuthorizeHandler.hpp"
#endif
#include "none/AuthNoneAuthorizeHandler.h"
#include "common/ceph_context.h"
#include "common/debug.h"
#include "auth/KeyRing.h"

#define dout_subsys ceph_subsys_auth
#undef dout_prefix
#define dout_prefix *_dout << "AuthRegistry(" << this << ") "

using std::string;

AuthRegistry::AuthRegistry(CephContext *cct)
  : cct(cct)
{
  cct->_conf.add_observer(this);
}

AuthRegistry::~AuthRegistry()
{
  cct->_conf.remove_observer(this);
  for (auto i : authorize_handlers) {
    delete i.second;
  }
}

const char** AuthRegistry::get_tracked_conf_keys() const
{
  static const char *keys[] = {
    "auth_supported",
    "auth_client_required",
    "auth_cluster_required",
    "auth_service_required",
    "ms_mon_cluster_mode",
    "ms_mon_service_mode",
    "ms_mon_client_mode",
    "ms_cluster_mode",
    "ms_service_mode",
    "ms_client_mode",
    "keyring",
    NULL
  };
  return keys;
}

void AuthRegistry::handle_conf_change(
  const ConfigProxy& conf,
  const std::set<std::string>& changed)
{
  std::scoped_lock l(lock);
  _refresh_config();
}


void AuthRegistry::_parse_method_list(const string& s,
				      std::vector<uint32_t> *v)
{
  std::list<std::string> sup_list;
  get_str_list(s, sup_list);
  if (sup_list.empty()) {
    lderr(cct) << "WARNING: empty auth protocol list" << dendl;
  }
  v->clear();
  for (auto& i : sup_list) {
    ldout(cct, 5) << "adding auth protocol: " << i << dendl;
    if (i == "cephx") {
      v->push_back(CEPH_AUTH_CEPHX);
    } else if (i == "none") {
      v->push_back(CEPH_AUTH_NONE);
    } else if (i == "gss") {
      v->push_back(CEPH_AUTH_GSS);
    } else {
      lderr(cct) << "WARNING: unknown auth protocol defined: " << i << dendl;
    }
  }
  if (v->empty()) {
    lderr(cct) << "WARNING: no auth protocol defined" << dendl;
  }
  ldout(cct,20) << __func__ << " " << s << " -> " << *v << dendl;
}

void AuthRegistry::_parse_mode_list(const string& s,
				    std::vector<uint32_t> *v)
{
  std::list<std::string> sup_list;
  get_str_list(s, sup_list);
  if (sup_list.empty()) {
    lderr(cct) << "WARNING: empty auth protocol list" << dendl;
  }
  v->clear();
  for (auto& i : sup_list) {
    ldout(cct, 5) << "adding con mode: " << i << dendl;
    if (i == "crc") {
      v->push_back(CEPH_CON_MODE_CRC);
    } else if (i == "secure") {
      v->push_back(CEPH_CON_MODE_SECURE);
    } else {
      lderr(cct) << "WARNING: unknown connection mode " << i << dendl;
    }
  }
  if (v->empty()) {
    lderr(cct) << "WARNING: no connection modes defined" << dendl;
  }
  ldout(cct,20) << __func__ << " " << s << " -> " << *v << dendl;
}

void AuthRegistry::_refresh_config()
{
  if (cct->_conf->auth_supported.size()) {
    _parse_method_list(cct->_conf->auth_supported, &cluster_methods);
    _parse_method_list(cct->_conf->auth_supported, &service_methods);
    _parse_method_list(cct->_conf->auth_supported, &client_methods);
  } else {
    _parse_method_list(cct->_conf->auth_cluster_required, &cluster_methods);
    _parse_method_list(cct->_conf->auth_service_required, &service_methods);
    _parse_method_list(cct->_conf->auth_client_required, &client_methods);
  }
  _parse_mode_list(cct->_conf.get_val<string>("ms_mon_cluster_mode"),
		   &mon_cluster_modes);
  _parse_mode_list(cct->_conf.get_val<string>("ms_mon_service_mode"),
		   &mon_service_modes);
  _parse_mode_list(cct->_conf.get_val<string>("ms_mon_client_mode"),
		   &mon_client_modes);
  _parse_mode_list(cct->_conf.get_val<string>("ms_cluster_mode"),
		   &cluster_modes);
  _parse_mode_list(cct->_conf.get_val<string>("ms_service_mode"),
		   &service_modes);
  _parse_mode_list(cct->_conf.get_val<string>("ms_client_mode"),
		   &client_modes);

  ldout(cct,10) << __func__ << " cluster_methods " << cluster_methods
		<< " service_methods " << service_methods
		<< " client_methods " << client_methods
		<< dendl;
  ldout(cct,10) << __func__ << " mon_cluster_modes " << mon_cluster_modes
		<< " mon_service_modes " << mon_service_modes
		<< " mon_client_modes " << mon_client_modes
		<< "; cluster_modes " << cluster_modes
		<< " service_modes " << service_modes
		<< " client_modes " << client_modes
		<< dendl;

  // if we have no keyring, filter out cephx
  _no_keyring_disabled_cephx = false;
  bool any_cephx = false;
  for (auto *p : {&cluster_methods, &service_methods, &client_methods}) {
    auto q = std::find(p->begin(), p->end(), CEPH_AUTH_CEPHX);
    if (q != p->end()) {
      any_cephx = true;
      break;
    }
  }
  if (any_cephx) {
    KeyRing k;
    int r = k.from_ceph_context(cct);
    if (r == -ENOENT) {
      for (auto *p : {&cluster_methods, &service_methods, &client_methods}) {
	auto q = std::find(p->begin(), p->end(), CEPH_AUTH_CEPHX);
	if (q != p->end()) {
	  p->erase(q);
	  _no_keyring_disabled_cephx = true;
	}
      }
    }
    if (_no_keyring_disabled_cephx) {
      lderr(cct) << "no keyring found at " << cct->_conf->keyring
	       << ", disabling cephx" << dendl;
    }
  }
}

void AuthRegistry::get_supported_methods(
  int peer_type,
  std::vector<uint32_t> *methods,
  std::vector<uint32_t> *modes) const
{
  if (methods) {
    methods->clear();
  }
  if (modes) {
    modes->clear();
  }
  std::scoped_lock l(lock);

  if (modes) {
    switch (get_conn_type(peer_type)) {
    case CONN_TYPE_CLIENT_MON:
      *modes = mon_client_modes;
      break;
    case CONN_TYPE_CLIENT:
      *modes = client_modes;
      break;
    case CONN_TYPE_CLUSTER_MON:
      *modes = mon_cluster_modes;
      break;
    case CONN_TYPE_CLUSTER:
      *modes = cluster_modes;
      break;
    case CONN_TYPE_SERVICE_MON:
      *modes = mon_service_modes;
      break;
    case CONN_TYPE_SERVICE:
      *modes = service_modes;
      break;
    default:
      ceph_abort();
    }
  }

  if (methods) {
    switch (cct->get_module_type()) {
    case CEPH_ENTITY_TYPE_CLIENT:
      // i am client
      *methods = client_methods;
      return;
    case CEPH_ENTITY_TYPE_MON:
    case CEPH_ENTITY_TYPE_MGR:
      // i am mon/mgr
      switch (peer_type) {
      case CEPH_ENTITY_TYPE_MON:
      case CEPH_ENTITY_TYPE_MGR:
        // they are mon/mgr
        *methods = cluster_methods;
        break;
      default:
        // they are anything but mons
        *methods = service_methods;
        break;
      }
      return;
    default:
      // i am a non-mon daemon
      switch (peer_type) {
      case CEPH_ENTITY_TYPE_MON:
      case CEPH_ENTITY_TYPE_MGR:
      case CEPH_ENTITY_TYPE_MDS:
      case CEPH_ENTITY_TYPE_OSD:
        // they are anything but a client
        *methods = cluster_methods;
        break;
      default:
        // they are a client
        *methods = service_methods;
        break;
      }
      return;
    }
  }
}

bool AuthRegistry::is_supported_method(int peer_type, int method) const
{
  std::vector<uint32_t> s;
  get_supported_methods(peer_type, &s);
  return std::find(s.begin(), s.end(), method) != s.end();
}

bool AuthRegistry::any_supported_methods(int peer_type) const
{
  std::vector<uint32_t> s;
  get_supported_methods(peer_type, &s);
  return !s.empty();
}

void AuthRegistry::get_supported_modes(
  int peer_type,
  uint32_t auth_method,
  std::vector<uint32_t> *modes) const
{
  std::vector<uint32_t> s;
  get_supported_methods(peer_type, nullptr, &s);
  if (auth_method == CEPH_AUTH_NONE) {
    // filter out all but crc for AUTH_NONE
    modes->clear();
    for (auto mode : s) {
      if (mode == CEPH_CON_MODE_CRC) {
	modes->push_back(mode);
      }
    }
  } else {
    *modes = s;
  }
}

ConnectionType AuthRegistry::get_conn_type(int peer_type) const
{
  return get_conn_type(cct->get_module_type(), peer_type);
}

ConnectionType AuthRegistry::get_conn_type(int my_type, int peer_type)
{
  switch (my_type) {
  case CEPH_ENTITY_TYPE_CLIENT:
    // i am client
    switch (peer_type) {
    case CEPH_ENTITY_TYPE_MON:
    case CEPH_ENTITY_TYPE_MGR:
      return CONN_TYPE_CLIENT_MON;
    default:
      return CONN_TYPE_CLIENT;
    }
  case CEPH_ENTITY_TYPE_MON:
  case CEPH_ENTITY_TYPE_MGR:
    // i am mon/mgr
    switch (peer_type) {
    case CEPH_ENTITY_TYPE_MON:
    case CEPH_ENTITY_TYPE_MGR:
      // they are mon/mgr
      return CONN_TYPE_CLUSTER_MON;
    default:
      // they are anything but mon/mgr
      return CONN_TYPE_SERVICE_MON;
    }
  default:
    // i am a non-mon daemon
    switch (peer_type) {
    case CEPH_ENTITY_TYPE_MON:
    case CEPH_ENTITY_TYPE_MGR:
      // they are a mon daemon
      return CONN_TYPE_CLUSTER_MON;
    case CEPH_ENTITY_TYPE_MDS:
    case CEPH_ENTITY_TYPE_OSD:
      // they are another daemon
      return CONN_TYPE_CLUSTER;
    default:
      // they are a client
      return CONN_TYPE_SERVICE;
    }
  }
}

uint32_t AuthRegistry::pick_mode(
  int peer_type,
  uint32_t auth_method,
  const std::vector<uint32_t>& preferred_modes)
{
  std::vector<uint32_t> allowed_modes;
  get_supported_modes(peer_type, auth_method, &allowed_modes);
  for (auto mode : preferred_modes) {
    if (std::find(allowed_modes.begin(), allowed_modes.end(), mode)
	!= allowed_modes.end()) {
      return mode;
    }
  }
  ldout(cct,1) << "failed to pick con mode from client's " << preferred_modes
	       << " and our " << allowed_modes << dendl;
  return CEPH_CON_MODE_UNKNOWN;
}

AuthAuthorizeHandler *AuthRegistry::get_handler(int peer_type, int method)
{
  std::scoped_lock l{lock};
  ldout(cct,20) << __func__ << " peer_type " << peer_type << " method " << method
		<< " cluster_methods " << cluster_methods
		<< " service_methods " << service_methods
		<< " client_methods " << client_methods
		<< dendl;
  if (cct->get_module_type() == CEPH_ENTITY_TYPE_CLIENT) {
    return nullptr;
  }
  switch (peer_type) {
  case CEPH_ENTITY_TYPE_MON:
  case CEPH_ENTITY_TYPE_MGR:
  case CEPH_ENTITY_TYPE_MDS:
  case CEPH_ENTITY_TYPE_OSD:
    if (std::find(cluster_methods.begin(), cluster_methods.end(), method) ==
	cluster_methods.end()) {
      return nullptr;
    }
    break;
  default:
    if (std::find(service_methods.begin(), service_methods.end(), method) ==
	service_methods.end()) {
      return nullptr;
    }
    break;
  }

  auto iter = authorize_handlers.find(method);
  if (iter != authorize_handlers.end()) {
    return iter->second;
  }
  AuthAuthorizeHandler *ah = nullptr;
  switch (method) {
  case CEPH_AUTH_NONE:
    ah = new AuthNoneAuthorizeHandler();
    break;
  case CEPH_AUTH_CEPHX:
    ah = new CephxAuthorizeHandler();
    break;
#ifdef HAVE_GSSAPI
  case CEPH_AUTH_GSS:
    ah = new KrbAuthorizeHandler();
    break;
#endif
  }
  if (ah) {
    authorize_handlers[method] = ah;
  }
  return ah;
}

