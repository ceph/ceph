// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "AuthServer.h"
#include "common/ceph_context.h"

AuthServer::AuthServer(CephContext *cct)
  : auth_ah_service_registry(
      new AuthAuthorizeHandlerRegistry(
	cct,
	cct->_conf->auth_supported.empty() ?
	cct->_conf->auth_service_required :
	cct->_conf->auth_supported)),
    auth_ah_cluster_registry(
      new AuthAuthorizeHandlerRegistry(
	cct,
	cct->_conf->auth_supported.empty() ?
	cct->_conf->auth_cluster_required :
	cct->_conf->auth_supported))
{
}

AuthAuthorizeHandler *AuthServer::get_auth_authorize_handler(
  int peer_type,
  int auth_method)
{
  switch (peer_type) {
  case CEPH_ENTITY_TYPE_MDS:
  case CEPH_ENTITY_TYPE_MON:
  case CEPH_ENTITY_TYPE_MGR:
  case CEPH_ENTITY_TYPE_OSD:
    return auth_ah_cluster_registry->get_handler(auth_method);
  default:
    return auth_ah_service_registry->get_handler(auth_method);
  }
}

void AuthServer::get_supported_auth_methods(
  int peer_type,
  vector<uint32_t> *methods)
{
  switch (peer_type) {
  case CEPH_ENTITY_TYPE_MDS:
  case CEPH_ENTITY_TYPE_MON:
  case CEPH_ENTITY_TYPE_MGR:
  case CEPH_ENTITY_TYPE_OSD:
    return auth_ah_cluster_registry->get_supported_methods(methods);
  default:
    return auth_ah_service_registry->get_supported_methods(methods);
  }
}
