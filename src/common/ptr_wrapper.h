
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

template <typename T, int id>
struct ptr_wrapper
{
  T *_p;

  ptr_wrapper(T *v) : _p(v) {}
  ptr_wrapper() : _p(nullptr) {}

  T *operator=(T *p) {
    _p = p;
    return p;
  }

  T& operator*() {
    return *_p;
  }

  T* operator->() {
    return _p;
  }

  T *get() {
    return _p;
  }
};


