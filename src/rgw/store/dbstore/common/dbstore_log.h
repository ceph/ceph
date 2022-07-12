// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef DB_STORE_LOG_H
#define DB_STORE_LOG_H

#include <cerrno>
#include <cstdlib>
#include <string>
#include <cstdio>
#include <iostream>
#include <fstream>
#include "common/dout.h"

#define dout_subsys ceph_subsys_rgw
#undef dout_prefix
#define dout_prefix *_dout << "rgw dbstore: "

#endif
