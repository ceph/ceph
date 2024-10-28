// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cerrno>
#include <cstdlib>
#include <string>
#include <cstdio>
#include <iostream>
#include <fstream>
#include "common/dout.h"

#undef dout_prefix
#define dout_prefix *_dout << "rgw dbstore: "
