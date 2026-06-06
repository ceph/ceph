// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <string_view>
#include "include/common_fwd.h"
#include "common/dout.h"

int rgw_mime_init(const DoutPrefixProvider *dpp, CephContext *cct);
void rgw_mime_cleanup();
std::string_view rgw_find_mime_by_ext(std::string_view ext);
