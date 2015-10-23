// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_SLABPOOL_H
#define CEPH_MSG_SLABPOOL_H

#include <vector>
#include <list>

#include "include/page.h"
#include "include/error.h"

static const uint16_t SLAB_MAGIC_NUMBER = 0x51AB; // meant to be 'SLAB' :-)
typedef uint64_t uintptr_t;

class SlabAllocator;

/*
 * SlabPageDesc is 1:1 mapped to slab page.
 * footprint: 80b for each slab page.
 */
struct SlabPageDesc {
 private:
  void *page;
  std::vector<uintptr_t> free_objects;
  uint32_t id; // index into slab page vector
  uint16_t magic_number;
  uint8_t class_id;

 public:
  SlabPageDesc(void *data, size_t objects, size_t object_size, uint8_t id, uint32_t idx)
    : page(data), id(idx), magic_number(SLAB_MAGIC_NUMBER), class_id(id) {
    uintptr_t object = reinterpret_cast<uintptr_t>(page);
    // we already return the first object to caller, see 'create_from_new_page'
    free_objects.reserve(objects - 1);
    for (size_t i = 1u; i < objects; i++) {
      object += object_size;
      free_objects.push_back(object);
    }
  }

  bool empty() const {
    return free_objects.empty();
  }

  size_t size() const {
    return free_objects.size();
  }

  uint32_t index() const {
    return id;
  }

  uint16_t magic() const {
    return magic_number;
  }

  uint8_t slab_class_id() const {
    return class_id;
  }

  void* slab_page() const {
    return page;
  }

  void* allocate_object() {
    assert(!free_objects.empty());
    void *object = reinterpret_cast<void*>(free_objects.back());
    free_objects.pop_back();
    return object;
  }

  void free_object(void *object) {
    free_objects.push_back(reinterpret_cast<uintptr_t>(object));
  }
};

class SlabClass {
 private:
  std::list<SlabPageDesc*> free_slab_pages;
  size_t num_obj; // the number of objects
  uint8_t slab_class_id;

 public:
  SlabClass(size_t s, uint8_t slab_class_id): num_obj(s), slab_class_id(slab_class_id) {}
  ~SlabClass() {
    free_slab_pages.clear();
  }

  size_t count() const {
    return num_obj;
  }

  bool empty() const {
    return free_slab_pages.empty();
  }

  void *create(uint32_t *idx) {
    assert(!free_slab_pages.empty());
    SlabPageDesc *desc = free_slab_pages.back();
    void *object = desc->allocate_object();
    if (desc->empty()) {
      // if empty, remove desc from the list of slab pages with free objects.
      free_slab_pages.pop_back();
    }
    *idx = desc->index();
    return object;
  }

  int create_from_new_page(uint64_t max_object_size, uint32_t slab_page_index, SlabPageDesc **desc, void **data) {
    // allocate slab page.
    int r = ::posix_memalign(data, CEPH_PAGE_SIZE, max_object_size);
    if (r != 0) {
        return -errno;
    }
    // allocate descriptor to slab page.
    assert(num_obj % CEPH_PAGE_SIZE == 0);
    uint64_t objects = max_object_size / num_obj;

    try {
      *desc = new SlabPageDesc(*data, objects, num_obj, slab_class_id, slab_page_index);
    } catch (const std::bad_alloc& e) {
      ::free(data);
      return -ENOMEM;
    }

    free_slab_pages.push_front(*desc);
    // first object from the allocated slab page is returned.
    return 0;
  }

  void free(void *object, SlabPageDesc *desc) {
    desc->free_object(object);
    if (desc->size() == 1) {
      // push back desc into the list of slab pages with free objects.
      free_slab_pages.push_back(desc);
    }
  }
};

class SlabAllocator {
 private:
  std::vector<size_t> slab_class_sizes;
  std::vector<SlabClass> slab_classes;
  std::vector<SlabPageDesc*> slab_pages_vector;
  uint64_t max_object_size;
  uint64_t available_slab_pages;

 private:
  void initialize_slab_allocator(double growth_factor, uint64_t limit) {
    const size_t initial_size = CEPH_PAGE_SIZE;
    size_t size = initial_size; // initial object size
    uint8_t slab_class_id = 0U;

    while (max_object_size / size > 1) {
      size = (size + CEPH_PAGE_SIZE - 1) & ~(size - 1);
      slab_class_sizes.push_back(size);
      slab_classes.push_back(SlabClass(size, slab_class_id));
      size *= growth_factor;
      assert(slab_class_id < std::numeric_limits<uint8_t>::max());
      slab_class_id++;
    }
    slab_class_sizes.push_back(max_object_size);
    slab_classes.push_back(SlabClass(max_object_size, slab_class_id));

    // If slab limit is zero, enable reclaimer.
    slab_pages_vector.reserve(available_slab_pages);
  }

  SlabClass* get_slab_class(const size_t size) {
      // given a size, find slab class with binary search.
      std::vector<size_t>::iterator i = std::lower_bound(slab_class_sizes.begin(), slab_class_sizes.end(), size);
      if (i == slab_class_sizes.end())
          return NULL;
      return &slab_classes[std::distance(slab_class_sizes.begin(), i)];
  }

  SlabClass* get_slab_class(const uint8_t slab_class_id) {
      assert(slab_class_id < slab_classes.size());
      return &slab_classes[slab_class_id];
  }

 public:
  SlabAllocator(double growth_factor, uint64_t limit, uint64_t max_obj_size)
      : max_object_size(max_obj_size), available_slab_pages(limit / max_obj_size) {
    initialize_slab_allocator(growth_factor, limit);
  }

  ~SlabAllocator() {
    for (std::vector<SlabPageDesc*>::iterator it = slab_pages_vector.begin();
         it != slab_pages_vector.end(); ++it) {
      if (*it == NULL) {
        continue;
      }
      ::free((*it)->slab_page());
      delete *it;
    }
  }

  int create(const size_t size, uint32_t *idx, void **data) {
    SlabClass *slab_class = get_slab_class(size);
    if (!slab_class)
      return -EINVAL;

    int r = 0;
    if (!slab_class->empty()) {
      *data = slab_class->create(idx);
    } else {
      if (available_slab_pages > 0) {
        size_t index_to_insert = slab_pages_vector.size();
        SlabPageDesc *desc;
        r = slab_class->create_from_new_page(max_object_size, index_to_insert, &desc, data);
        if (r < 0)
          return r;
        *idx = index_to_insert;
        slab_pages_vector.push_back(desc);
        if (available_slab_pages > 0)
          --available_slab_pages;
      } else {
        r = -ENOMEM;
      }
    }
    return r;
  }

  /**
   * Free an item back to its original slab class.
   */
  void free(uint32_t slab_class_id, void *data) {
    if (data) {
      SlabPageDesc *desc = slab_pages_vector[slab_class_id];
      assert(desc && desc->magic() == SLAB_MAGIC_NUMBER);
      SlabClass* slab_class = get_slab_class(desc->slab_class_id());
      slab_class->free(data, desc);
    }
  }

  /**
   * Helper function: Print all available slab classes and their respective properties.
   */
  void print_slab_classes() {
    uint8_t class_id = 0;
    for (std::vector<SlabClass>::const_iterator it = slab_classes.begin();
       it != slab_classes.end(); ++it) {
      printf("slab[%3d]\tsize: %10lu\tper-slab-page: %5lu\n", class_id, it->count(), max_object_size / it->count());
      class_id++;
    }
  }

  /**
   * Helper function: Useful for getting a slab class' chunk size from a size parameter.
   */
  size_t class_size(const size_t size) {
    SlabClass *slab_class = get_slab_class(size);
    return (slab_class) ? slab_class->count() : 0;
  }
};

#endif /* CEPH_MSG_SLABPOOL_H */
