/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 CERN
 *
 * Author: Joaquim Rocha <joaquim.rocha@cern.ch>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CEPH_LIBRBD_CLS_NUMOPS_CLIENT_H
#define CEPH_LIBRBD_CLS_NUMOPS_CLIENT_H

#include "include/rados/librados.hpp"

namespace rados {
  namespace cls {
    namespace numops {

      extern int add(librados::IoCtx *ioctx,
                     const std::string& oid,
                     const std::string& key,
                     double value_to_add);

      extern int sub(librados::IoCtx *ioctx,
                     const std::string& oid,
                     const std::string& key,
                     double value_to_subtract);

      extern int mul(librados::IoCtx *ioctx,
                     const std::string& oid,
                     const std::string& key,
                     double value_to_multiply);

      extern int div(librados::IoCtx *ioctx,
                     const std::string& oid,
                     const std::string& key,
                     double value_to_divide);

    } // namespace numops
  } // namespace cls
} // namespace rados

#endif // CEPH_LIBRBD_CLS_NUMOPS_CLIENT_H

