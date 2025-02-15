// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Sebastien Ponce <sebastien.ponce@cern.ch>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <stdlib.h>

#include "RadosXattrIter.h"

librados::RadosXattrsIter::RadosXattrsIter()
  : val(NULL)
{
  i = attrset.end();
}

librados::RadosXattrsIter::~RadosXattrsIter()
{
  free(val);
  val = NULL;
}
