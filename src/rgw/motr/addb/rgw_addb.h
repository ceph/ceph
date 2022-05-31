/*
 * Ceph - scalable distributed file system
 *
 * RGW ADDB macro for ADDB probes.
 *
 * Copyright (C) 2022 Seagate Technology LLC and/or its Affiliates
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#ifndef __RGW_ADDB_H__
#define __RGW_ADDB_H__

#include <addb2/addb2_internal.h>

#define ADDB(_addb_id, ...)                                      \
  do {                                                           \
    uint64_t addb_params__[] = {__VA_ARGS__};                    \
    constexpr auto addb_params_size__ =                          \
      sizeof addb_params__ / sizeof addb_params__[0];            \
    m0_addb2_add(_addb_id, addb_params_size__, addb_params__);   \
  } while (false)

enum RGWAddbId {
  RGW_ADDB_RANGE_START = M0_ADDB2__EXT_RANGE_1,
  RGW_ADDB_MEASUREMENT_ID = RGW_ADDB_RANGE_START,
  RGW_ADDB_REQUEST_ID,
  RGW_ADDB_REQUEST_OPCODE_ID,
  RGW_ADDB_REQUEST_TO_MOTR_ID,
  RGW_ADDB_LAST_REQUEST_ID = RGW_ADDB_REQUEST_TO_MOTR_ID,
  RGW_ADDB_RANGE_END = RGW_ADDB_LAST_REQUEST_ID
};

enum RGWAddbPhaseCode {
  RGW_ADDB_PHASE_START,
  RGW_ADDB_PHASE_ERROR,
  RGW_ADDB_PHASE_DONE,
};

enum RGWAddbFuncName {
  RGW_ADDB_FUNC_CREATE_MOBJ,
  RGW_ADDB_FUNC_OPEN_MOBJ,
  RGW_ADDB_FUNC_DELETE_MOBJ,
  RGW_ADDB_FUNC_WRITE_MOBJ,
  RGW_ADDB_FUNC_READ_MOBJ,
  RGW_ADDB_FUNC_WRITE,
  RGW_ADDB_FUNC_GET_NEW_REQ_ID,
  RGW_ADDB_FUNC_DO_IDX_OP,
  RGW_ADDB_FUNC_DO_IDX_NEXT_OP,
  RGW_ADDB_FUNC_DELETE_IDX_BY_NAME,
  RGW_ADDB_FUNC_CREATE_IDX_BY_NAME,
};

#endif
