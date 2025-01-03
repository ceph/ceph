// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_ALLOC_PTR_H
#define CEPH_ALLOC_PTR_H

#include <memory>

template <class T>
class alloc_ptr
{
public:
    typedef typename std::pointer_traits< std::unique_ptr<T> >::pointer pointer;
    typedef typename std::pointer_traits< std::unique_ptr<T> >::element_type element_type;

    alloc_ptr() : ptr() {}

    template<class U>
      alloc_ptr(U&& u) : ptr(std::forward<U>(u)) {}

    alloc_ptr(alloc_ptr<pointer>&& rhs) : ptr(std::move(rhs.ptr)) {}
    alloc_ptr(const alloc_ptr<pointer>& rhs) = delete;
    alloc_ptr& operator=(const alloc_ptr<pointer>&& rhs) {
        ptr = rhs.ptr;
    }
    alloc_ptr& operator=(const alloc_ptr<pointer>& rhs) {
        ptr = rhs.ptr;
    }

    void swap (alloc_ptr<pointer>& rhs) {
        ptr.swap(rhs.ptr);
    }
    element_type* release() {
        return ptr.release();
    }
    void reset(element_type *p = nullptr) {
        ptr.reset(p);
    }
    element_type* get() const {
        if (!ptr)
          ptr.reset(new element_type);
        return ptr.get();
    }
    element_type& operator*() const {
        if (!ptr)
          ptr.reset(new element_type);
        return *ptr;
    }
    element_type* operator->() const {
        if (!ptr)
          ptr.reset(new element_type);
        return ptr.get();
    }
    operator bool() const {
        return !!ptr;
    }

    friend bool operator< (const alloc_ptr& lhs, const alloc_ptr& rhs) {
        return std::less<element_type>(*lhs, *rhs);
    }
    friend bool operator<=(const alloc_ptr& lhs, const alloc_ptr& rhs) {
        return std::less_equal<element_type>(*lhs, *rhs);
    }
    friend bool operator> (const alloc_ptr& lhs, const alloc_ptr& rhs) {
        return std::greater<element_type>(*lhs, *rhs);
    }
    friend bool operator>=(const alloc_ptr& lhs, const alloc_ptr& rhs) {
        return std::greater_equal<element_type>(*lhs, *rhs);
    }
    friend bool operator==(const alloc_ptr& lhs, const alloc_ptr& rhs) {
        return *lhs == *rhs;
    }
    friend bool operator!=(const alloc_ptr& lhs, const alloc_ptr& rhs) {
        return *lhs != *rhs;
    }
private:
    mutable std::unique_ptr<element_type> ptr;
};

#endif
