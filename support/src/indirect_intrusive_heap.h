// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once


#include <memory>
#include <vector>
#include <string>
#include <iostream>
#include <functional>

#include "assert.h"


namespace crimson {
  using IndIntruHeapData = size_t;

  /* T is the ultimate data that's being stored in the heap, although
   *   through indirection.
   *
   * I is the indirect type that will actually be stored in the heap
   *   and that must allow dereferencing (via operator*) to yield a
   *   T&.
   *
   * C is a functor when given two T&'s will return true if the first
   *   must precede the second.
   *
   * heap_info is a data member pointer as to where the heap data in T
   * is stored.
   *
   * K is the branching factor of the heap, default is 2 (binary heap).
   */
  template<typename I,
	   typename T,
	   IndIntruHeapData T::*heap_info,
	   typename C,
	   uint K = 2>
  class IndIntruHeap {

    static_assert(
      std::is_same<T,typename std::pointer_traits<I>::element_type>::value,
      "class I must resolve to class T by indirection (pointer dereference)");

    static_assert(
      std::is_same<bool,
      typename std::result_of<C(const T&,const T&)>::type>::value,
      "class C must define operator() to take two const T& and return a bool");

    static_assert(K >= 2, "K (degree of branching) must be at least 2");

    class Iterator {
      friend IndIntruHeap<I, T, heap_info, C, K>;

      IndIntruHeap<I, T, heap_info, C, K>& heap;
      size_t                            index;

      Iterator(IndIntruHeap<I, T, heap_info, C, K>& _heap, size_t _index) :
	heap(_heap),
	index(_index)
      {
	// empty
      }

    public:

      Iterator(Iterator&& other) :
	heap(other.heap),
	index(other.index)
      {
	// empty
      }

      Iterator(const Iterator& other) :
	heap(other.heap),
	index(other.index)
      {
	// empty
      }

      Iterator& operator=(Iterator&& other) {
	std::swap(heap, other.heap);
	std::swap(index, other.index);
	return *this;
      }

      Iterator& operator=(const Iterator& other) {
	heap = other.heap;
	index = other.index;
      }

      Iterator& operator++() {
	if (index <= heap.count) {
	  ++index;
	}
	return *this;
      }

      bool operator==(const Iterator& other) const {
	return index == other.index;
      }

      bool operator!=(const Iterator& other) const {
	return !(*this == other);
      }

      T& operator*() {
	return *heap.data[index];
      }

      T* operator->() {
	return &(*heap.data[index]);
      }

#if 0
      // the item this iterator refers to
      void increase() {
	heap.siftUp(index);
      }
#endif
    }; // class Iterator

    
    class ConstIterator {
      friend IndIntruHeap<I, T, heap_info, C, K>;

      const IndIntruHeap<I, T, heap_info, C, K>& heap;
      size_t                                  index;

      ConstIterator(const IndIntruHeap<I, T, heap_info, C, K>& _heap, size_t _index) :
	heap(_heap),
	index(_index)
      {
	// empty
      }

    public:

      ConstIterator(ConstIterator&& other) :
	heap(other.heap),
	index(other.index)
      {
	// empty
      }

      ConstIterator(const ConstIterator& other) :
	heap(other.heap),
	index(other.index)
      {
	// empty
      }

      ConstIterator& operator=(ConstIterator&& other) {
	std::swap(heap, other.heap);
	std::swap(index, other.index);
	return *this;
      }

      ConstIterator& operator=(const ConstIterator& other) {
	heap = other.heap;
	index = other.index;
      }

      ConstIterator& operator++() {
	if (index <= heap.count) {
	  ++index;
	}
	return *this;
      }

      bool operator==(const ConstIterator& other) const {
	return &heap == &other.heap && index == other.index;
      }

      bool operator!=(const ConstIterator& other) const {
	return !(*this == other);
      }

      const T& operator*() {
	return *heap.data[index];
      }

      const T* operator->() {
	return &(*heap.data[index]);
      }
    }; // class ConstIterator

    
  protected:
    using index_t = IndIntruHeapData;

    std::vector<I> data;
    index_t        count;
    C              comparator;

  public:

    IndIntruHeap() :
      count(0)
    {
      // empty
    }

    IndIntruHeap(const IndIntruHeap<I,T,heap_info,C,K>& other) :
      count(other.count)
    {
      for (uint i = 0; i < other.count; ++i) {
	data.push_back(other.data[i]);
      }
    }

    bool empty() const { return 0 == count; }

    size_t size() const { return count; }

    T& top() { return *data[0]; }

    const T& top() const { return *data[0]; }

    I& top_ind() { return data[0]; }

    const I& top_ind() const { return data[0]; }

    void push(I&& item) {
      index_t i = count++;
      intru_data_of(item) = i;
      data.emplace_back(std::move(item));
      sift_up(i);
    }

    void push(const I& item) {
      I copy(item);
      push(std::move(copy));
    }

    void pop() {
      remove(0);
    }

    void remove(Iterator& i) {
      remove(i.index);
      i = end();
    }

    Iterator find(const I& item) {
      for (index_t i = 0; i < count; ++i) {
	if (data[i] == item) {
	  return Iterator(*this, i);
	}
      }
      return end();
    }

    // NB: should we be using operator== instead of address check?
    Iterator find(const T& item) {
      for (index_t i = 0; i < count; ++i) {
	if (data[i].get() == &item) {
	  return Iterator(*this, i);
	}
      }
      return end();
    }

    // reverse find -- start looking from bottom of heap
    Iterator rfind(const I& item) {
      // index_t is unsigned, so we can't allow to go negative; so
      // we'll keep it one more than actual index
      for (index_t i = count; i > 0; --i) {
	if (data[i-1] == item) {
	  return Iterator(*this, i-1);
	}
      }
      return end();
    }

    // reverse find -- start looking from bottom of heap
    Iterator rfind(const T& item) {
      // index_t is unsigned, so we can't allow to go negative; so
      // we'll keep it one more than actual index
      for (index_t i = count; i > 0; --i) {
	if (data[i-1].get() == &item) {
	  return Iterator(*this, i-1);
	}
      }
      return end();
    }

    void promote(T& item) {
      sift_up(item.*heap_info);
    }

    void demote(T& item) {
      sift_down(item.*heap_info);
    }

    void adjust(T& item) {
      sift(item.*heap_info);
    }

    Iterator begin() {
      return Iterator(*this, 0);
    }

    Iterator end() {
      return Iterator(*this, count);
    }

    ConstIterator cbegin() const {
      return ConstIterator(*this, 0);
    }

    ConstIterator cend() const {
      return ConstIterator(*this, count);
    }

    friend std::ostream& operator<<(std::ostream& out, const IndIntruHeap& h) {
      auto i = h.data.cbegin();
      if (i != h.data.cend()) {
	out << **i;
	++i;
	while (i != h.data.cend()) {
	  out << ", " << **i;
	}
      }
      return out;
    }

    // can only be called if I is copyable
    std::ostream&
    display_sorted(std::ostream& out,
		   std::function<bool(const T&)> filter = all_filter) const {
      static_assert(std::is_copy_constructible<I>::value,
		    "cannot call display_sorted when class I is not copy"
		    " constructible");

      IndIntruHeap<I,T,heap_info,C,K> copy = *this;

      bool first = true;
      while(!copy.empty()) {
	const T& top = copy.top();
	if (filter(top)) {
	  if (!first) {
	    out << ", ";
	  }
	  out << copy.top();
	  first = false;
	}
	copy.pop();
      }

      return out;
    }


  protected:

    static index_t& intru_data_of(I& item) {
      return (*item).*heap_info;
    }

    void remove(index_t i) {
      std::swap(data[i], data[--count]);
      intru_data_of(data[i]) = i;
      data.pop_back();
      sift_down(i);
    }

    // default value of filter parameter to display_sorted
    static bool all_filter(const T& data) { return true; }

    // when i is negative?
    static inline index_t parent(index_t i) {
      assert(0 != i);
      return (i - 1) / K;
    }

    // index of left child when K==2, index of left-most child when K>2
    static inline index_t lhs(index_t i) { return K*i + 1; }

    // index of right child when K==2, index of right-most child when K>2
    static inline index_t rhs(index_t i) { return K*i + K; }

    void sift_up(index_t i) {
      while (i > 0) {
	index_t pi = parent(i);
	if (!comparator(*data[i], *data[pi])) {
	  break;
	}

	std::swap(data[i], data[pi]);
	intru_data_of(data[i]) = i;
	intru_data_of(data[pi]) = pi;
	i = pi;
      }
    } // sift_up

    // use this sift_down definition when K>2; it's more general and
    // uses a loop; EnableBool insures template uses a template
    // parameter
    template<bool EnableBool=true>
    typename std::enable_if<(K>2)&&EnableBool,void>::type sift_down(index_t i) {
      if (i >= count) return;
      while (true) {
	index_t li = lhs(i);

	if (li < count) {
	  index_t ri = std::min(rhs(i), count - 1);

	  // find the index of min. child
	  index_t min_i = li;
	  for (index_t k = li + 1; k <= ri; ++k) {
	    if (comparator(*data[k], *data[min_i])) {
	      min_i = k;
	    }
	  }

	  if (comparator(*data[min_i], *data[i])) {
	    std::swap(data[i], data[min_i]);
	    intru_data_of(data[i]) = i;
	    intru_data_of(data[min_i]) = min_i;
	    i = min_i;
	  } else {
	    // no child is smaller
	    break;
	  }
	} else {
	  // no children
	  break;
	}
      }
    } // sift_down    

    // use this sift_down definition when K==2; EnableBool insures
    // template uses a template parameter
    template<bool EnableBool=true>
    typename std::enable_if<K==2&&EnableBool,void>::type sift_down(index_t i) {
      if (i >= count) return;
      while (true) {
        const index_t li = lhs(i);
	const index_t ri = 1 + li;
 
        if (li < count) {
	  if (comparator(*data[li], *data[i])) {
	    if (ri < count && comparator(*data[ri], *data[li])) {
	      std::swap(data[i], data[ri]);
	      intru_data_of(data[i]) = i;
	      intru_data_of(data[ri]) = ri;
	      i = ri;
	    } else {
	      std::swap(data[i], data[li]);
	      intru_data_of(data[i]) = i;
	      intru_data_of(data[li]) = li;
	      i = li;
            }
	  } else if (ri < count && comparator(*data[ri], *data[i])) {
	    std::swap(data[i], data[ri]);
            intru_data_of(data[i]) = i;
	    intru_data_of(data[ri]) = ri;
	    i = ri;
          } else {
	    // no child is smaller
            break;
          }
        } else {
	  // no children
          break;
        }
      } // while
    } // sift_down

    void sift(index_t i) {
      if (i == 0) {
	// if we're at top, can only go down
	sift_down(i);
      } else {
	index_t pi = parent(i);
	if (comparator(*data[i], *data[pi])) {
	  // if we can go up, we will
	  sift_up(i);
	} else {
	  // otherwise we'll try to go down
	  sift_down(i);
	}
      }
    } // sift
  }; // class IndIntruHeap

} // namespace crimson
