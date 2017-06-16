// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*
// vim: ts=8 sw=2 smarttab

#include "include/rados/librados.h"

std::string set_pg_num(
    rados_t *cluster, const std::string &pool_name, uint32_t pg_num);
