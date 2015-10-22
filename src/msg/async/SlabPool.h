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

static const uint16_t SLAB_MAGIC_NUMBER = 0x51AB; // meant to be 'SLAB' :-)
typedef uint64_t uintptr_t;

/*
 * SlabPageDesc is 1:1 mapped to slab page.
 * footprint: 80b for each slab page.
 */
struct SlabPageDesc {
 private:
  void *slab_page;
  std::vector<uintptr_t> free_objects;
  uint32_t index; // index into slab page vector
  uint16_t magic;
  uint8_t slab_class_id;

 public:
  SlabPageDesc(void *slab_page, size_t objects, size_t object_size, uint8_t id, uint32_t idx)
    : slab_page(slab_page), index(idx), magic(SLAB_MAGIC_NUMBER), slab_class_id(id) {
    uintptr_t object = reinterpret_cast<uintptr_t>(slab_page);
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
    return index;
  }

  uint16_t magic() const {
    return magic;
  }

  uint8_t slab_class_id() const {
    return slab_class_id;
  }

  void* slab_page() const {
    return slab_page;
  }

  std::vector<uintptr_t>& free_objects() {
    return free_objects;
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
  size_t count; // the number of objects
  uint8_t slab_class_id;

 public:
  SlabClass(size_t s, uint8_t slab_class_id): count(s), slab_class_id(slab_class_id) {}
  ~SlabClass() {
    free_slab_pages.clear();
  }

  size_t count() const {
    return count;
  }

  bool empty() const {
    return free_slab_pages.empty();
  }

  void *create(uint32_t *idx) {
    assert(!free_slab_pages.empty());
    SlabPageDesc *desc = free_slab_pages.back();
    void *object = desc->allocate_object();
    if (desc.empty()) {
      // if empty, remove desc from the list of slab pages with free objects.
      free_slab_pages.pop_back();
    }
    *idx = desc->index();
    return object;
  }

  int create_from_new_page(SlabAllocator *alloc, uint64_t max_object_size, uint32_t slab_page_index, void **data) {
    // allocate slab page.
    void *slab_page = ::posix_memalign(data, CEPH_PAGE_SIZE, max_object_size);
    if (!slab_page) {
        return -ENOMEM;
    }
    // allocate descriptor to slab page.
    SlabPageDesc *desc = NULL;
    assert(count % CEPH_PAGE_SIZE == 0);
    uint64_t objects = max_object_size / _size;

    try {
      desc = new SlabPageDesc(slab_page, objects, _size, slab_class_id, slab_page_index);
    } catch (const std::bad_alloc& e) {
      ::free(slab_page);
      throw std::bad_alloc{};
    }

    free_slab_pages.push_front(*desc);
    // first object from the allocated slab page is returned.
    alloc->slab_pages_vector.push_back(&desc);
    return 0;
  }

  void free_item(Item *item, SlabPageDesc& desc) {
    void *object = item;
    desc.free_object(object);
    if (desc.size() == 1) {
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
      size = (size + CEPH_PAGE_SIZE - 1) & ~(size - 1)
      slab_class_sizes.push_back(size);
      slab_classes.push_back(SlabClass(size, slab_class_id));
      size *= growth_factor;
      assert(slab_class_id < std::numeric_limits<uint8_t>::max());
      slab_class_id++;
    }
    slab_class_sizes.push_back(_max_object_size);
    slab_classes.push_back(SlabClass(max_object_size, slab_class_id));

    // If slab limit is zero, enable reclaimer.
    slab_pages_vector.reserve(available_slab_pages);
  }

  SlabClass* get_slab_class(const size_t size) {
      // given a size, find slab class with binary search.
      std::vector<size_t>::iterator i = slab_class_sizes.lower_bound(size);
      if (i == slab_class_sizes.end())
          return NULL;
      return &slab_classes[std::distance(slab_class_sizes.begin(), i)];
  }

  SlabClass* get_slab_class(const uint8_t slab_class_id) {
      assert(slab_class_id >= 0 && slab_class_id < _slab_classes.size());
      return &slab_classes[slab_class_id];
  }

 public:
  SlabAllocator(double growth_factor, uint64_t limit, uint64_t max_obj_size)
      : max_object_size(max_obj_size), available_slab_pages(limit / max_obj_size) {
    initialize_slab_allocator(growth_factor, limit);
  }

  ~SlabAllocator() {
    for (std::vector<SlabPageDesc*>::iterator it = slab_pages_vector;
         it != slab_pages_vector.end(); ++it) {
      if (*it == NULL) {
        continue;
      }
      ::free(desc->slab_page());
      delete desc;
    }
  }

  int create(const size_t size, uint32_t *idx, void **data) {
    SlabClass *slab_class = get_slab_class(size);
    if (!slab_class)
      return -EINVAL;

    int r = 0;
    if (!slab_class->empty()) {
      data = slab_class->create(idx);
    } else {
      if (available_slab_pages > 0) {
        size_t index_to_insert = slab_pages_vector.size();
        r = slab_class->create_from_new_page(this, max_object_size, index_to_insert, data);
        *idx = index_to_insert;
        if (r < 0)
          return r;
        slab_pages_vector.push_back(&desc);
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
      assert(desc != NULL);
      assert(desc->magic() == SLAB_MAGIC_NUMBER);
      SlabClass* slab_class = get_slab_class(desc.slab_class_id());
      slab_class->free_item(item, desc);
    }
  }

  /**
   * Helper function: Print all available slab classes and their respective properties.
   */
  void print_slab_classes() {
    uint8_t class_id = 0;
    for (std::vector<SlabClass>::const_iterator it = slab_classes.const_begin();
       it != slab_classes.const_end(); ++it) {
      printf("slab[%3d]\tsize: %10lu\tper-slab-page: %5lu\n", class_id, it->size(), max_object_size / size);
      class_id++;
    }
  }

  /**
   * Helper function: Useful for getting a slab class' chunk size from a size parameter.
   */
  size_t class_size(const size_t size) {
    SlabClass *slab_class = get_slab_class(size);
    return (slab_class) ? slab_class->size() : 0;
  }
};

#endif /* CEPH_MSG_SLABPOOL_H */
