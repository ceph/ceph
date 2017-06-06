// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CONFIG_VALIDATORS
#define CEPH_CONFIG_VALIDATORS

#include "config.h"
#include <string>

/**
 * Global config value validators for the Ceph project
 */

int validate(md_config_t::option_rbd_default_pool_t *type,
             std::string *value, std::string *error_message);
int validate(md_config_t::option_rbd_default_data_pool_t *type,
             std::string *value, std::string *error_message);
int validate(md_config_t::option_rbd_default_features_t *type,
             std::string *value, std::string *error_message);

#endif // CEPH_CONFIG_VALIDATORS
