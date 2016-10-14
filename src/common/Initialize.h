// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
/* Copyright (c) 2011 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef CEPH_INITIALIZE_H
#define CEPH_INITIALIZE_H

/**
 * This class is used to manage once-only initialization that should occur
 * before main() is invoked, such as the creation of static variables.  It
 * also provides a mechanism for handling dependencies (where one class
 * needs to perform its once-only initialization before another).
 * 
 * The simplest way to use an Initialize object is to define a static
 * initialization method for a class, say Foo::init().  Then, declare
 * a static Initialize object in the class:
 * "static Initialize initializer(Foo::init);".
 * The result is that Foo::init will be invoked when the object is
 * constructed (before main() is invoked).  Foo::init can create static
 * objects and perform any other once-only initialization needed by the
 * class.  Furthermore, if some other class needs to ensure that Foo has
 * been initialized (e.g. as part of its own initialization) it can invoke
 * Foo::init directly (Foo::init should contain an internal guard so that
 * it only performs its functions once, even if invoked several times).
 *
 * There is also a second form of constructor for Initialize that causes a
 * new object to be dynamically allocated and assigned to a pointer, instead
 * of invoking a function. This form allows for the creation of static objects
 * that are never destructed (thereby avoiding issues with the order of
 * destruction).
 */
class Initialize {
 public:
  /**
   * This form of constructor causes its function argument to be invoked
   * when the object is constructed.  When used with a static Initialize
   * object, this will cause \p func to run before main() runs, so that
   * \p func can perform once-only initialization.
   *
   * \param func
   *      This function is invoked with no arguments when the object is
   *      constructed.  Typically the function will create static
   *      objects and/or invoke other initialization functions.  The
   *      function should normally contain an internal guard so that it
   *      only performs its initialization the first time it is invoked.
   */
  explicit Initialize(void (*func)()) {
    (*func)();
  }

  /**
   * This form of constructor causes a new object of a particular class
   * to be constructed with a no-argument constructor and assigned to a
   * given pointer.  This form is typically used with a static Initialize
   * object: the result is that the object will be created and assigned
   * to the pointer before main() runs.
   *
   * \param p
   *      Pointer to an object of any type. If the pointer is NULL then
   *      it is replaced with a pointer to a newly allocated object of
   *      the given type.
   */
  template<typename T>
  explicit Initialize(T*& p) {
    if (p == NULL) {
      p = new T;
    }
  }
};

#endif  // CEPH_INITIALIZE_H
