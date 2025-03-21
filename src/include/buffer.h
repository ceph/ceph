// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_BUFFER_H
#define CEPH_BUFFER_H

#if defined(__linux__) || defined(__FreeBSD__)
#include <stdlib.h>
#endif
#include <limits.h>

#ifndef _XOPEN_SOURCE
# define _XOPEN_SOURCE 600
#endif

#include <stdio.h>
#include <sys/uio.h>

#if defined(__linux__)	// For malloc(2).
#include <malloc.h>
#endif

#include <inttypes.h>
#include <stdint.h>
#include <string.h>

#if !defined(__CYGWIN__) && !defined(_WIN32)
# include <sys/mman.h>
#endif

#include <iosfwd>
#include <iomanip>
#include <list>
#include <memory>
#include <vector>
#include <string>
#if __cplusplus >= 201703L
#include <string_view>
#endif // __cplusplus >= 201703L

#include <exception>
#include <type_traits>

#include "page.h"
#include "crc32c.h"
#include "buffer_fwd.h"


#ifdef __CEPH__
# include "include/ceph_assert.h"
#else
# include <assert.h>
#endif

#include "inline_memory.h"

#define CEPH_BUFFER_API

#ifdef HAVE_SEASTAR
namespace seastar {
template <typename T> class temporary_buffer;
namespace net {
class packet;
}
}
#endif // HAVE_SEASTAR
class deleter;

template<typename T> class DencDumper;

namespace ceph {

template <class T>
struct nop_delete {
  void operator()(T*) {}
};

// This is not unique_ptr-like smart pointer! It just signalizes ownership
// but DOES NOT manage the resource. It WILL LEAK if not manually deleted.
// It's rather a replacement for raw pointer than any other smart one.
//
// Considered options:
//  * unique_ptr with custom deleter implemented in .cc (would provide
//    the non-zero-cost resource management),
//  * GSL's owner<T*> (pretty neat but would impose an extra depedency),
//  * unique_ptr with nop deleter,
//  * raw pointer (doesn't embed ownership enforcement - std::move).
template <class T>
struct unique_leakable_ptr : public std::unique_ptr<T, ceph::nop_delete<T>> {
  using std::unique_ptr<T, ceph::nop_delete<T>>::unique_ptr;
};

namespace buffer CEPH_BUFFER_API {
inline namespace v15_2_0 {

/// Actual definitions in common/error_code.h
struct error;
struct bad_alloc;
struct end_of_buffer;
struct malformed_input;
struct error_code;

  /// count of cached crc hits (matching input)
  int get_cached_crc();
  /// count of cached crc hits (mismatching input, required adjustment)
  int get_cached_crc_adjusted();
  /// count of crc cache misses
  int get_missed_crc();
  /// enable/disable tracking of cached crcs
  void track_cached_crc(bool b);

  /*
   * an abstract raw buffer.  with a reference count.
   */
  class raw;
  class raw_malloc;
  class raw_static;
  class raw_posix_aligned;
  class raw_hack_aligned;
  class raw_claimed_char;
  class raw_unshareable; // diagnostic, unshareable char buffer
  class raw_combined;
  class raw_claim_buffer;


  /*
   * named constructors
   */
  ceph::unique_leakable_ptr<raw> copy(const char *c, unsigned len);
  ceph::unique_leakable_ptr<raw> create(unsigned len);
  ceph::unique_leakable_ptr<raw> create(unsigned len, char c);
  ceph::unique_leakable_ptr<raw> create_in_mempool(unsigned len, int mempool);
  ceph::unique_leakable_ptr<raw> claim_char(unsigned len, char *buf);
  ceph::unique_leakable_ptr<raw> create_malloc(unsigned len);
  ceph::unique_leakable_ptr<raw> claim_malloc(unsigned len, char *buf);
  ceph::unique_leakable_ptr<raw> create_static(unsigned len, char *buf);
  ceph::unique_leakable_ptr<raw> create_aligned(unsigned len, unsigned align);
  ceph::unique_leakable_ptr<raw> create_aligned_in_mempool(unsigned len, unsigned align, int mempool);
  ceph::unique_leakable_ptr<raw> create_page_aligned(unsigned len);
  ceph::unique_leakable_ptr<raw> create_small_page_aligned(unsigned len);
  ceph::unique_leakable_ptr<raw> claim_buffer(unsigned len, char *buf, deleter del);

#ifdef HAVE_SEASTAR
  /// create a raw buffer to wrap seastar cpu-local memory, using foreign_ptr to
  /// make it safe to share between cpus
  ceph::unique_leakable_ptr<buffer::raw> create(seastar::temporary_buffer<char>&& buf);
  /// create a raw buffer to wrap seastar cpu-local memory, without the safety
  /// of foreign_ptr. the caller must otherwise guarantee that the buffer ptr is
  /// destructed on this cpu
  ceph::unique_leakable_ptr<buffer::raw> create_local(seastar::temporary_buffer<char>&& buf);
#endif

  /*
   * a buffer pointer.  references (a subsequence of) a raw buffer.
   */
  class CEPH_BUFFER_API ptr {
    friend class list;
  protected:
    raw *_raw;
    unsigned _off, _len;
  private:

    void release();

    template<bool is_const>
    class iterator_impl {
      const ptr *bp;     ///< parent ptr
      const char *start; ///< starting pointer into bp->c_str()
      const char *pos;   ///< pointer into bp->c_str()
      const char *end_ptr;   ///< pointer to bp->end_c_str()
      const bool deep;   ///< if true, do not allow shallow ptr copies

      iterator_impl(typename std::conditional<is_const, const ptr*, ptr*>::type p,
		    size_t offset, bool d)
	: bp(p),
	  start(p->c_str() + offset),
	  pos(start),
	  end_ptr(p->end_c_str()),
	  deep(d)
      {}

      friend class ptr;

    public:
      using pointer = typename std::conditional<is_const, const char*, char *>::type;
      pointer get_pos_add(size_t n) {
	auto r = pos;
	*this += n;
	return r;
      }
      ptr get_ptr(size_t len) {
	if (deep) {
	  return buffer::copy(get_pos_add(len), len);
	} else {
	  size_t off = pos - bp->c_str();
	  *this += len;
	  return ptr(*bp, off, len);
	}
      }

      iterator_impl& operator+=(size_t len);

      const char *get_pos() {
	return pos;
      }
      const char *get_end() {
	return end_ptr;
      }

      size_t get_offset() {
	return pos - start;
      }

      bool end() const {
	return pos == end_ptr;
      }
    };

  public:
    using const_iterator = iterator_impl<true>;
    using iterator = iterator_impl<false>;

    ptr() : _raw(nullptr), _off(0), _len(0) {}
    ptr(ceph::unique_leakable_ptr<raw> r);
    // cppcheck-suppress noExplicitConstructor
    ptr(unsigned l);
    ptr(const char *d, unsigned l);
    ptr(const ptr& p);
    ptr(ptr&& p) noexcept;
    ptr(const ptr& p, unsigned o, unsigned l);
    ptr(const ptr& p, ceph::unique_leakable_ptr<raw> r);
    ptr& operator= (const ptr& p);
    ptr& operator= (ptr&& p) noexcept;
    ~ptr() {
      // BE CAREFUL: this destructor is called also for hypercombined ptr_node.
      // After freeing underlying raw, `*this` can become inaccessible as well!
      release();
    }

    bool have_raw() const { return _raw ? true:false; }

    void swap(ptr& other) noexcept;

    iterator begin(size_t offset=0) {
      return iterator(this, offset, false);
    }
    const_iterator begin(size_t offset=0) const {
      return const_iterator(this, offset, false);
    }
    const_iterator cbegin() const {
      return begin();
    }
    const_iterator begin_deep(size_t offset=0) const {
      return const_iterator(this, offset, true);
    }

    // misc
    bool is_aligned(unsigned align) const {
      return ((uintptr_t)c_str() & (align-1)) == 0;
    }
    bool is_page_aligned() const { return is_aligned(CEPH_PAGE_SIZE); }
    bool is_n_align_sized(unsigned align) const
    {
      return (length() % align) == 0;
    }
    bool is_n_page_sized() const { return is_n_align_sized(CEPH_PAGE_SIZE); }
    bool is_partial() const {
      return have_raw() && (start() > 0 || end() < raw_length());
    }

    int get_mempool() const;
    void reassign_to_mempool(int pool);
    void try_assign_to_mempool(int pool);

    // accessors
    const char *c_str() const;
    char *c_str();
    const char *end_c_str() const;
    char *end_c_str();
    unsigned length() const { return _len; }
    unsigned offset() const { return _off; }
    unsigned start() const { return _off; }
    unsigned end() const { return _off + _len; }
    unsigned unused_tail_length() const;
    const char& operator[](unsigned n) const;
    char& operator[](unsigned n);

    const char *raw_c_str() const;
    unsigned raw_length() const;
    int raw_nref() const;

    void copy_out(unsigned o, unsigned l, char *dest) const;

    unsigned wasted() const;

    int cmp(const ptr& o) const;
    bool is_zero() const;

    // modifiers
    void set_offset(unsigned o) {
#ifdef __CEPH__
      ceph_assert(raw_length() >= o);
#else
      assert(raw_length() >= o);
#endif
      _off = o;
    }
    void set_length(unsigned l) {
#ifdef __CEPH__
      ceph_assert(raw_length() >= l);
#else
      assert(raw_length() >= l);
#endif
      _len = l;
    }

    unsigned append(char c);
    unsigned append(const char *p, unsigned l);
#if __cplusplus >= 201703L
    inline unsigned append(std::string_view s) {
      return append(s.data(), s.length());
    }
#endif // __cplusplus >= 201703L
    void copy_in(unsigned o, unsigned l, const char *src, bool crc_reset = true);
    void zero(bool crc_reset = true);
    void zero(unsigned o, unsigned l, bool crc_reset = true);
    unsigned append_zeros(unsigned l);

#ifdef HAVE_SEASTAR
    /// create a temporary_buffer, copying the ptr as its deleter
    operator seastar::temporary_buffer<char>() &;
    /// convert to temporary_buffer, stealing the ptr as its deleter
    operator seastar::temporary_buffer<char>() &&;
#endif // HAVE_SEASTAR

  };


  struct ptr_hook {
    mutable ptr_hook* next;

    ptr_hook() = default;
    ptr_hook(ptr_hook* const next)
      : next(next) {
    }
  };

  class ptr_node : public ptr_hook, public ptr {
  public:
    struct cloner {
      ptr_node* operator()(const ptr_node& clone_this);
    };
    struct disposer {
      void operator()(ptr_node* const delete_this) {
	if (!__builtin_expect(dispose_if_hypercombined(delete_this), 0)) {
	  delete delete_this;
	}
      }
    };

    ~ptr_node() = default;

    static std::unique_ptr<ptr_node, disposer>
    create(ceph::unique_leakable_ptr<raw> r) {
      return create_hypercombined(std::move(r));
    }
    static std::unique_ptr<ptr_node, disposer>
    create(const unsigned l) {
      return create_hypercombined(buffer::create(l));
    }
    template <class... Args>
    static std::unique_ptr<ptr_node, disposer>
    create(Args&&... args) {
      return std::unique_ptr<ptr_node, disposer>(
	new ptr_node(std::forward<Args>(args)...));
    }

    static ptr_node* copy_hypercombined(const ptr_node& copy_this);

  private:
    friend list;

    template <class... Args>
    ptr_node(Args&&... args) : ptr(std::forward<Args>(args)...) {
    }
    ptr_node(const ptr_node&) = default;

    ptr& operator= (const ptr& p) = delete;
    ptr& operator= (ptr&& p) noexcept = delete;
    ptr_node& operator= (const ptr_node& p) = delete;
    ptr_node& operator= (ptr_node&& p) noexcept = delete;
    void swap(ptr& other) noexcept = delete;
    void swap(ptr_node& other) noexcept = delete;

    static bool dispose_if_hypercombined(ptr_node* delete_this);
    static std::unique_ptr<ptr_node, disposer> create_hypercombined(
      ceph::unique_leakable_ptr<raw> r);
  };
  /*
   * list - the useful bit!
   */

  class CEPH_BUFFER_API list {
  public:
    // this the very low-level implementation of singly linked list
    // ceph::buffer::list is built on. We don't use intrusive slist
    // of Boost (or any other 3rd party) to save extra dependencies
    // in our public headers.
    class buffers_t {
      // _root.next can be thought as _head
      ptr_hook _root;
      ptr_hook* _tail;

    public:
      template <class T>
      class buffers_iterator {
	typename std::conditional<
	  std::is_const<T>::value, const ptr_hook*, ptr_hook*>::type cur;
	template <class U> friend class buffers_iterator;
      public:
	using value_type = T;
	using reference = typename std::add_lvalue_reference<T>::type;
	using pointer = typename std::add_pointer<T>::type;
	using difference_type = std::ptrdiff_t;
	using iterator_category = std::forward_iterator_tag;

	template <class U>
	buffers_iterator(U* const p)
	  : cur(p) {
	}
	// copy constructor
	buffers_iterator(const buffers_iterator<T>& other)
	  : cur(other.cur) {
	}
	// converting constructor, from iterator -> const_iterator only
	template <class U, typename std::enable_if<
	    std::is_const<T>::value && !std::is_const<U>::value, int>::type = 0>
	buffers_iterator(const buffers_iterator<U>& other)
	  : cur(other.cur) {
	}
	buffers_iterator() = default;

	T& operator*() const {
	  return *reinterpret_cast<T*>(cur);
	}
	T* operator->() const {
	  return reinterpret_cast<T*>(cur);
	}

	buffers_iterator& operator++() {
	  cur = cur->next;
	  return *this;
	}
	buffers_iterator operator++(int) {
	  const auto temp(*this);
	  ++*this;
	  return temp;
	}

	template <class U>
	buffers_iterator& operator=(buffers_iterator<U>& other) {
	  cur = other.cur;
	  return *this;
	}

	bool operator==(const buffers_iterator& rhs) const {
	  return cur == rhs.cur;
	}
	bool operator!=(const buffers_iterator& rhs) const {
	  return !(*this==rhs);
	}
      };

      typedef buffers_iterator<const ptr_node> const_iterator;
      typedef buffers_iterator<ptr_node> iterator;

      typedef const ptr_node& const_reference;
      typedef ptr_node& reference;

      buffers_t()
        : _root(&_root),
	  _tail(&_root) {
      }
      buffers_t(const buffers_t&) = delete;
      buffers_t(buffers_t&& other)
	: _root(other._root.next == &other._root ? &_root : other._root.next),
	  _tail(other._tail == &other._root ? &_root : other._tail) {
	other._root.next = &other._root;
	other._tail = &other._root;

	_tail->next = &_root;
      }
      buffers_t& operator=(buffers_t&& other) {
	if (&other != this) {
	  clear_and_dispose();
	  swap(other);
	}
	return *this;
      }

      void push_back(reference item) {
	item.next = &_root;
	// this updates _root.next when called on empty
	_tail->next = &item;
	_tail = &item;
      }

      void push_front(reference item) {
	item.next = _root.next;
	_root.next = &item;
	_tail = _tail == &_root ? &item : _tail;
      }

      // *_after
      iterator erase_after(const_iterator it) {
	const auto* to_erase = it->next;

	it->next = to_erase->next;
	_root.next = _root.next == to_erase ? to_erase->next : _root.next;
	_tail = _tail == to_erase ? (ptr_hook*)&*it : _tail;
	return it->next;
      }

      void insert_after(const_iterator it, reference item) {
	item.next = it->next;
	it->next = &item;
	_root.next = it == end() ? &item : _root.next;
	_tail = const_iterator(_tail) == it ? &item : _tail;
      }

      void splice_back(buffers_t& other) {
	if (other.empty()) {
	  return;
	}

	other._tail->next = &_root;
	// will update root.next if empty() == true
	_tail->next = other._root.next;
	_tail = other._tail;

	other._root.next = &other._root;
	other._tail = &other._root;
      }

      bool empty() const { return _tail == &_root; }

      const_iterator begin() const {
	return _root.next;
      }
      const_iterator before_begin() const {
	return &_root;
      }
      const_iterator end() const {
	return &_root;
      }
      iterator begin() {
	return _root.next;
      }
      iterator before_begin() {
	return &_root;
      }
      iterator end() {
	return &_root;
      }

      reference front() {
	return reinterpret_cast<reference>(*_root.next);
      }
      reference back() {
	return reinterpret_cast<reference>(*_tail);
      }
      const_reference front() const {
	return reinterpret_cast<const_reference>(*_root.next);
      }
      const_reference back() const {
	return reinterpret_cast<const_reference>(*_tail);
      }

      void clone_from(const buffers_t& other) {
	clear_and_dispose();
	for (auto& node : other) {
	  ptr_node* clone = ptr_node::cloner()(node);
	  push_back(*clone);
	}
      }
      void clear_and_dispose() {
        ptr_node::disposer dispose;
        for (auto it = begin(), e = end(); it != e; /* nop */) {
          auto& node = *it++;
          dispose(&node);
        }
        _tail = &_root;
        _root.next = _tail;
      }
      iterator erase_after_and_dispose(iterator it) {
	auto* to_dispose = &*std::next(it);
	auto ret = erase_after(it);
	ptr_node::disposer()(to_dispose);
	return ret;
      }

      void swap(buffers_t& other) {
	const auto copy_root = _root;
	_root.next = \
	  other._root.next == &other._root ? &this->_root : other._root.next;
	other._root.next = \
	  copy_root.next == &_root ? &other._root : copy_root.next;

	const auto copy_tail = _tail;
	_tail = other._tail == &other._root ? &this->_root : other._tail;
	other._tail = copy_tail == &_root ? &other._root : copy_tail;

	_tail->next = &_root;
	other._tail->next = &other._root;
      }
    };

    class iterator;

  private:
    // my private bits
    buffers_t _buffers;

    // track bufferptr we can modify (especially ::append() to). Not all bptrs
    // bufferlist holds have this trait -- if somebody ::push_back(const ptr&),
    // he expects it won't change.
    ptr_node* _carriage;
    unsigned _len, _num;

    template <bool is_const>
    class CEPH_BUFFER_API iterator_impl {
    protected:
      typedef typename std::conditional<is_const,
					const list,
					list>::type bl_t;
      typedef typename std::conditional<is_const,
					const buffers_t,
					buffers_t >::type list_t;
      typedef typename std::conditional<is_const,
					typename buffers_t::const_iterator,
					typename buffers_t::iterator>::type list_iter_t;
      bl_t* bl;
      list_t* ls;  // meh.. just here to avoid an extra pointer dereference..
      list_iter_t p;
      unsigned off; // in bl
      unsigned p_off;   // in *p
      friend class iterator_impl<true>;

    public:
      using iterator_category = std::forward_iterator_tag;
      using value_type = typename std::conditional<is_const, const char, char>::type;
      using difference_type = std::ptrdiff_t;
      using pointer = typename std::add_pointer<value_type>::type;
      using reference = typename std::add_lvalue_reference<value_type>::type;

      // constructor.  position.
      iterator_impl()
	: bl(0), ls(0), off(0), p_off(0) {}
      iterator_impl(bl_t *l, unsigned o=0);
      iterator_impl(bl_t *l, unsigned o, list_iter_t ip, unsigned po)
	: bl(l), ls(&bl->_buffers), p(ip), off(o), p_off(po) {}
      iterator_impl(const list::iterator& i);

      /// get current iterator offset in buffer::list
      unsigned get_off() const { return off; }

      /// get number of bytes remaining from iterator position to the end of the buffer::list
      unsigned get_remaining() const { return bl->length() - off; }

      /// true if iterator is at the end of the buffer::list
      bool end() const {
	return p == ls->end();
	//return off == bl->length();
      }
      void seek(unsigned o);
      char operator*() const;
      iterator_impl& operator+=(unsigned o);
      iterator_impl& operator++();
      ptr get_current_ptr() const;
      bool is_pointing_same_raw(const ptr& other) const;

      bl_t& get_bl() const { return *bl; }

      // copy data out.
      // note that these all _append_ to dest!
      void copy(unsigned len, char *dest);
      // deprecated, use copy_deep()
      void copy(unsigned len, ptr &dest) __attribute__((deprecated));
      void copy_deep(unsigned len, ptr &dest);
      void copy_shallow(unsigned len, ptr &dest);
      void copy(unsigned len, list &dest);
      void copy(unsigned len, std::string &dest);
      template<typename A>
      void copy(unsigned len, std::vector<uint8_t,A>& u8v) {
        u8v.resize(len);
        copy(len, (char*)u8v.data());
      }

      void copy_all(list &dest);

      // get a pointer to the currenet iterator position, return the
      // number of bytes we can read from that position (up to want),
      // and advance the iterator by that amount.
      size_t get_ptr_and_advance(size_t want, const char **p);

      /// calculate crc from iterator position
      uint32_t crc32c(size_t length, uint32_t crc);

      friend bool operator==(const iterator_impl& lhs,
			     const iterator_impl& rhs) {
	return &lhs.get_bl() == &rhs.get_bl() && lhs.get_off() == rhs.get_off();
      }
      friend bool operator!=(const iterator_impl& lhs,
			     const iterator_impl& rhs) {
	return &lhs.get_bl() != &rhs.get_bl() || lhs.get_off() != rhs.get_off();
      }
    };

  public:
    typedef iterator_impl<true> const_iterator;

    class CEPH_BUFFER_API iterator : public iterator_impl<false> {
    public:
      iterator() = default;
      iterator(bl_t *l, unsigned o=0);
      iterator(bl_t *l, unsigned o, list_iter_t ip, unsigned po);
      // copy data in
      void copy_in(unsigned len, const char *src, bool crc_reset = true);
      void copy_in(unsigned len, const list& otherl);
    };

    struct reserve_t {
      char* bp_data;
      unsigned* bp_len;
      unsigned* bl_len;
    };

    class contiguous_appender {
      ceph::bufferlist& bl;
      ceph::bufferlist::reserve_t space;
      char* pos;
      bool deep;

      /// running count of bytes appended that are not reflected by @pos
      size_t out_of_band_offset = 0;

      contiguous_appender(bufferlist& bl, size_t len, bool d)
	: bl(bl),
	  space(bl.obtain_contiguous_space(len)),
	  pos(space.bp_data),
	  deep(d) {
      }

      void flush_and_continue() {
	const size_t l = pos - space.bp_data;
	*space.bp_len += l;
	*space.bl_len += l;
	space.bp_data = pos;
      }

      friend class list;
      template<typename Type> friend class ::DencDumper;

    public:
      ~contiguous_appender() {
	flush_and_continue();
      }

      size_t get_out_of_band_offset() const {
	return out_of_band_offset;
      }
      void append(const char* __restrict__ p, size_t l) {
	maybe_inline_memcpy(pos, p, l, 16);
	pos += l;
      }
      char *get_pos_add(size_t len) {
	char *r = pos;
	pos += len;
	return r;
      }
      char *get_pos() const {
	return pos;
      }

      void append(const bufferptr& p) {
	const auto plen = p.length();
	if (!plen) {
	  return;
	}
	if (deep) {
	  append(p.c_str(), plen);
	} else {
	  flush_and_continue();
	  bl.append(p);
	  space = bl.obtain_contiguous_space(0);
	  out_of_band_offset += plen;
	}
      }
      void append(const bufferlist& l) {
	if (deep) {
	  for (const auto &p : l._buffers) {
	    append(p.c_str(), p.length());
	  }
	} else {
	  flush_and_continue();
	  bl.append(l);
	  space = bl.obtain_contiguous_space(0);
	  out_of_band_offset += l.length();
	}
      }

      size_t get_logical_offset() const {
	return out_of_band_offset + (pos - space.bp_data);
      }
    };

    contiguous_appender get_contiguous_appender(size_t len, bool deep=false) {
      return contiguous_appender(*this, len, deep);
    }

    class contiguous_filler {
      friend buffer::list;
      char* pos;

      contiguous_filler(char* const pos) : pos(pos) {}

    public:
      void advance(const unsigned len) {
	pos += len;
      }
      void copy_in(const unsigned len, const char* const src) {
	memcpy(pos, src, len);
	advance(len);
      }
      char* c_str() {
        return pos;
      }
    };
    // The contiguous_filler is supposed to be not costlier than a single
    // pointer. Keep it dumb, please.
    static_assert(sizeof(contiguous_filler) == sizeof(char*),
		  "contiguous_filler should be no costlier than pointer");

    class page_aligned_appender {
      bufferlist& bl;
      unsigned min_alloc;

      page_aligned_appender(list *l, unsigned min_pages)
	: bl(*l),
	  min_alloc(min_pages * CEPH_PAGE_SIZE) {
      }

      void _refill(size_t len);

      template <class Func>
      void _append_common(size_t len, Func&& impl_f) {
	const auto free_in_last = bl.get_append_buffer_unused_tail_length();
	const auto first_round = std::min(len, free_in_last);
	if (first_round) {
	  impl_f(first_round);
	}
	// no C++17 for the sake of the C++11 guarantees of librados, sorry.
	const auto second_round = len - first_round;
	if (second_round) {
	  _refill(second_round);
	  impl_f(second_round);
	}
      }

      friend class list;

    public:
      void append(const bufferlist& l) {
	bl.append(l);
	bl.obtain_contiguous_space(0);
      }

      void append(const char* buf, size_t entire_len) {
	 _append_common(entire_len,
			[buf, this] (const size_t chunk_len) mutable {
	  bl.append(buf, chunk_len);
	  buf += chunk_len;
	});
      }

      void append_zero(size_t entire_len) {
	_append_common(entire_len, [this] (const size_t chunk_len) {
	  bl.append_zero(chunk_len);
	});
      }

      void substr_of(const list& bl, unsigned off, unsigned len) {
	for (const auto& bptr : bl.buffers()) {
	  if (off >= bptr.length()) {
	    off -= bptr.length();
	    continue;
	  }
	  const auto round_size = std::min(bptr.length() - off, len);
	  append(bptr.c_str() + off, round_size);
	  len -= round_size;
	  off = 0;
	}
      }
    };

    page_aligned_appender get_page_aligned_appender(unsigned min_pages=1) {
      return page_aligned_appender(this, min_pages);
    }

  private:
    // always_empty_bptr has no underlying raw but its _len is always 0.
    // This is useful for e.g. get_append_buffer_unused_tail_length() as
    // it allows to avoid conditionals on hot paths.
    static ptr_node always_empty_bptr;
    ptr_node& refill_append_space(const unsigned len);

    // for page_aligned_appender; never ever expose this publicly!
    // carriage / append_buffer is just an implementation's detail.
    ptr& get_append_buffer() {
      return *_carriage;
    }

  public:
    // cons/des
    list()
      : _carriage(&always_empty_bptr),
        _len(0),
        _num(0) {
    }
    // cppcheck-suppress noExplicitConstructor
    // cppcheck-suppress noExplicitConstructor
    list(unsigned prealloc)
      : _carriage(&always_empty_bptr),
        _len(0),
        _num(0) {
      reserve(prealloc);
    }

    list(const list& other)
      : _carriage(&always_empty_bptr),
        _len(other._len),
        _num(other._num) {
      _buffers.clone_from(other._buffers);
    }

    list(list&& other) noexcept
      : _buffers(std::move(other._buffers)),
        _carriage(other._carriage),
        _len(other._len),
        _num(other._num) {
      other.clear();
    }

    ~list() {
      _buffers.clear_and_dispose();
    }

    list& operator= (const list& other) {
      if (this != &other) {
        _carriage = &always_empty_bptr;
        _buffers.clone_from(other._buffers);
        _len = other._len;
        _num = other._num;
      }
      return *this;
    }
    list& operator= (list&& other) noexcept {
      _buffers = std::move(other._buffers);
      _carriage = other._carriage;
      _len = other._len;
      _num = other._num;
      other.clear();
      return *this;
    }

    uint64_t get_wasted_space() const;
    unsigned get_num_buffers() const { return _num; }
    const ptr_node& front() const { return _buffers.front(); }
    const ptr_node& back() const { return _buffers.back(); }

    int get_mempool() const;
    void reassign_to_mempool(int pool);
    void try_assign_to_mempool(int pool);

    size_t get_append_buffer_unused_tail_length() const {
      return _carriage->unused_tail_length();
    }

    const buffers_t& buffers() const { return _buffers; }
    buffers_t& mut_buffers() { return _buffers; }
    void swap(list& other) noexcept;
    unsigned length() const {
#if 0
      // DEBUG: verify _len
      unsigned len = 0;
      for (std::list<ptr>::const_iterator it = _buffers.begin();
	   it != _buffers.end();
	   it++) {
	len += (*it).length();
      }
#ifdef __CEPH__
      ceph_assert(len == _len);
#else
      assert(len == _len);
#endif // __CEPH__
#endif
      return _len;
    }

    bool contents_equal(const buffer::list& other) const;
    bool contents_equal(const void* other, size_t length) const;

    bool is_provided_buffer(const char *dst) const;
    bool is_aligned(unsigned align) const;
    bool is_page_aligned() const;
    bool is_n_align_sized(unsigned align) const;
    bool is_n_page_sized() const;
    bool is_aligned_size_and_memory(unsigned align_size,
				    unsigned align_memory) const;

    bool is_zero() const;

    // modifiers
    void clear() noexcept {
      _carriage = &always_empty_bptr;
      _buffers.clear_and_dispose();
      _len = 0;
      _num = 0;
    }
    void push_back(const ptr& bp) {
      if (bp.length() == 0)
	return;
      _buffers.push_back(*ptr_node::create(bp).release());
      _len += bp.length();
      _num += 1;
    }
    void push_back(ptr&& bp) {
      if (bp.length() == 0)
	return;
      _len += bp.length();
      _num += 1;
      _buffers.push_back(*ptr_node::create(std::move(bp)).release());
      _carriage = &always_empty_bptr;
    }
    void push_back(const ptr_node&) = delete;
    void push_back(ptr_node&) = delete;
    void push_back(ptr_node&&) = delete;
    void push_back(std::unique_ptr<ptr_node, ptr_node::disposer> bp) {
      _carriage = bp.get();
      _len += bp->length();
      _num += 1;
      _buffers.push_back(*bp.release());
    }
    void push_back(raw* const r) = delete;
    void push_back(ceph::unique_leakable_ptr<raw> r) {
      _buffers.push_back(*ptr_node::create(std::move(r)).release());
      _carriage = &_buffers.back();
      _len += _buffers.back().length();
      _num += 1;
    }

    void zero();
    void zero(unsigned o, unsigned l);

    bool is_contiguous() const;
    void rebuild();
    void rebuild(std::unique_ptr<ptr_node, ptr_node::disposer> nb);
    bool rebuild_aligned(unsigned align);
    // max_buffers = 0 mean don't care _buffers.size(), other
    // must make _buffers.size() <= max_buffers after rebuilding.
    bool rebuild_aligned_size_and_memory(unsigned align_size,
					 unsigned align_memory,
					 unsigned max_buffers = 0);
    bool rebuild_page_aligned();

    void reserve(size_t prealloc);

    [[deprecated("in favor of operator=(list&&)")]] void claim(list& bl) {
      *this = std::move(bl);
    }
    void claim_append(list& bl);
    void claim_append(list&& bl) {
      claim_append(bl);
    }

    // copy with explicit volatile-sharing semantics
    void share(const list& bl)
    {
      if (this != &bl) {
        clear();
	for (const auto& bp : bl._buffers) {
          _buffers.push_back(*ptr_node::create(bp).release());
        }
        _len = bl._len;
        _num = bl._num;
      }
    }

#ifdef HAVE_SEASTAR
    /// convert the bufferlist into a network packet
    operator seastar::net::packet() &&;
#endif

    iterator begin(size_t offset=0) {
      return iterator(this, offset);
    }
    iterator end() {
      return iterator(this, _len, _buffers.end(), 0);
    }

    const_iterator begin(size_t offset=0) const {
      return const_iterator(this, offset);
    }
    const_iterator cbegin(size_t offset=0) const {
      return begin(offset);
    }
    const_iterator end() const {
      return const_iterator(this, _len, _buffers.end(), 0);
    }

    void append(char c);
    void append(const char *data, unsigned len);
    void append(std::string s) {
      append(s.data(), s.length());
    }
#if __cplusplus >= 201703L
    // To forcibly disambiguate between string and string_view in the
    // case of arrays
    template<std::size_t N>
    void append(const char (&s)[N]) {
      append(s, N);
    }
    void append(const char* s) {
      append(s, strlen(s));
    }
    void append(std::string_view s) {
      append(s.data(), s.length());
    }
    template<typename A>
    void append(const std::vector<uint8_t,A>& u8v) {
      append((const char *)u8v.data(), u8v.size());
    }
#endif // __cplusplus >= 201703L
    void append(const ptr& bp);
    void append(ptr&& bp);
    void append(const ptr& bp, unsigned off, unsigned len);
    void append(const list& bl);
    /// append each non-empty line from the stream and add '\n',
    /// so a '\n' will be added even the stream does not end with EOL.
    ///
    /// For example, if the stream contains "ABC\n\nDEF", "ABC\nDEF\n" is
    /// actually appended.
    void append(std::istream& in);
    contiguous_filler append_hole(unsigned len);
    void append_zero(unsigned len);
    void prepend_zero(unsigned len);

    reserve_t obtain_contiguous_space(const unsigned len);

    /*
     * get a char
     */
    const char& operator[](unsigned n) const;
    char *c_str();
    std::string to_str() const;

    void substr_of(const list& other, unsigned off, unsigned len);

    // funky modifer
    void splice(unsigned off, unsigned len, list *claim_by=0 /*, bufferlist& replace_with */);
    void write(int off, int len, std::ostream& out) const;

    void encode_base64(list& o);
    void decode_base64(list& o);

    void write_stream(std::ostream &out) const;
    void hexdump(std::ostream &out, bool trailing_newline = true) const;
    ssize_t pread_file(const char *fn, uint64_t off, uint64_t len, std::string *error);
    int read_file(const char *fn, std::string *error);
    ssize_t read_fd(int fd, size_t len);
    ssize_t recv_fd(int fd, size_t len);
    int write_file(const char *fn, int mode=0644);
    int write_fd(int fd) const;
    int write_fd(int fd, uint64_t offset) const;
    int send_fd(int fd) const;
    template<typename VectorT>
    void prepare_iov(VectorT *piov) const {
#ifdef __CEPH__
      ceph_assert(_num <= IOV_MAX);
#else
      assert(_num <= IOV_MAX);
#endif
      piov->resize(_num);
      unsigned n = 0;
      for (auto& p : _buffers) {
	(*piov)[n].iov_base = (void *)p.c_str();
	(*piov)[n].iov_len = p.length();
	++n;
      }
    }

    struct iovec_t {
      uint64_t offset;
      uint64_t length;
      std::vector<iovec> iov;
    };
    using iov_vec_t = std::vector<iovec_t>;
    iov_vec_t prepare_iovs() const;

    uint32_t crc32c(uint32_t crc) const;
    void invalidate_crc();

    // These functions return a bufferlist with a pointer to a single
    // static buffer. They /must/ not outlive the memory they
    // reference.
    static list static_from_mem(char* c, size_t l);
    static list static_from_cstring(char* c);
    static list static_from_string(std::string& s);
  };

} // inline namespace v15_2_0

  /*
   * efficient hash of one or more bufferlists
   */

  class hash {
    uint32_t crc;

  public:
    hash() : crc(0) { }
    // cppcheck-suppress noExplicitConstructor
    hash(uint32_t init) : crc(init) { }

    void update(const buffer::list& bl) {
      crc = bl.crc32c(crc);
    }

    uint32_t digest() {
      return crc;
    }
  };

inline bool operator==(const bufferlist &lhs, const bufferlist &rhs) {
  if (lhs.length() != rhs.length())
    return false;
  return std::equal(lhs.begin(), lhs.end(), rhs.begin());
}

inline bool operator<(const bufferlist& lhs, const bufferlist& rhs) {
  auto l = lhs.begin(), r = rhs.begin();
  for (; l != lhs.end() && r != rhs.end(); ++l, ++r) {
    if (*l < *r) return true;
    if (*l > *r) return false;
  }
  return (l == lhs.end()) && (r != rhs.end()); // lhs.length() < rhs.length()
}

inline bool operator<=(const bufferlist& lhs, const bufferlist& rhs) {
  auto l = lhs.begin(), r = rhs.begin();
  for (; l != lhs.end() && r != rhs.end(); ++l, ++r) {
    if (*l < *r) return true;
    if (*l > *r) return false;
  }
  return l == lhs.end(); // lhs.length() <= rhs.length()
}

inline bool operator!=(const bufferlist &l, const bufferlist &r) {
  return !(l == r);
}
inline bool operator>(const bufferlist& lhs, const bufferlist& rhs) {
  return rhs < lhs;
}
inline bool operator>=(const bufferlist& lhs, const bufferlist& rhs) {
  return rhs <= lhs;
}

std::ostream& operator<<(std::ostream& out, const buffer::ptr& bp);

std::ostream& operator<<(std::ostream& out, const buffer::raw &r);

std::ostream& operator<<(std::ostream& out, const buffer::list& bl);

inline bufferhash& operator<<(bufferhash& l, const bufferlist &r) {
  l.update(r);
  return l;
}

static inline
void copy_bufferlist_to_iovec(const struct iovec *iov, unsigned iovcnt,
                              bufferlist *bl, int64_t r)
{
  auto iter = bl->cbegin();
  for (unsigned j = 0, resid = r; j < iovcnt && resid > 0; j++) {
         /*
          * This piece of code aims to handle the case that bufferlist
          * does not have enough data to fill in the iov
          */
         const auto round_size = std::min<unsigned>(resid, iov[j].iov_len);
         iter.copy(round_size, reinterpret_cast<char*>(iov[j].iov_base));
         resid -= round_size;
         /* iter is self-updating */
  }
}

} // namespace buffer

} // namespace ceph


#endif
