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
   * C is a functor when given T& will return true if the first
   *   definitely precedes the first.
   *
   * heap_info is a data member pointer as to where the heap data in T
   * is stored.
   */
  template<typename I, typename T, IndIntruHeapData T::*heap_info, typename C>
  class IndIntruHeap {

    static_assert(
      std::is_same<T,typename std::pointer_traits<I>::element_type>::value,
      "class I must resolve to class T by indirection (pointer dereference).");

    static_assert(
      std::is_same<bool,typename std::result_of<C(const T&,const T&)>::type>::value,
      "class C must define operator() to take two const T& and return a bool.");


  protected:
    using index_t = IndIntruHeapData;

    std::vector<I> data;
    index_t count;
    C comparator;

  public:

    IndIntruHeap() :
      count(0)
    {
      // empty
    }

    IndIntruHeap(const IndIntruHeap<I,T,heap_info,C>& other) :
      count(other.count)
    {
      for (uint i = 0; i < other.count; ++i) {
	data.push_back(other.data[i]);
      }
    }

    bool empty() const { return 0 == count; }

    T& top() { return *data[0]; }

    I& top_ind() { return data[0]; }

    template<typename J>
    void push(J&& item) {
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
      std::swap(data[0], data[--count]);
      intru_data_of(data[0]) = 0;
      data.pop_back();
      sift_down(0);
    }

    void adjust_up(T& item) {
      sift_up(item.*heap_info);
    }

    void adjust_down(T& item) {
      sift_down(item.*heap_info);
    }

    void adjust(T& item) {
      sift(item.*heap_info);
    }

    friend std::ostream& operator<<(std::ostream& out, const IndIntruHeap& h) {
      for (uint i = 0; i < h.count; ++i) {
	out << *h.data[i] << ", ";
      }
      return out;
    }

    std::ostream&
    display_sorted(std::ostream& out,
		   bool insert_line_breaks = true,
		   std::function<bool(const T&)> filter = all_filter) const {
      IndIntruHeap<I,T,heap_info,C> copy = *this;

      bool first = true;
      out << "[ ";

      while(!copy.empty()) {
	const T& top = copy.top();
	if (filter(top)) {
	  if (!first) {
	    out << ", ";
	  }
	  if (insert_line_breaks) {
	    out << std::endl << "    ";
	  }
	  out << copy.top();
	  first = false;
	}
	copy.pop();
      }

      out << " ]";
      if (insert_line_breaks) {
	out << std::endl;
      }

      return out;
    }


  protected:

    static index_t& intru_data_of(I& item) {
      return (*item).*heap_info;
    }

    // default value of filter parameter to display_sorted
    static bool all_filter(const T& data) { return true; }

    // when i is negative?
    static inline index_t parent(index_t i) {
      assert(0 != i);
      return (i - 1) / 2;
    }

    static inline index_t lhs(index_t i) { return 2*i + 1; }

    static inline index_t rhs(index_t i) { return 2*i + 2; }

    void sift_up(index_t i) {
      while (i > 0) {
	index_t pi = parent(i);
	if (comparator(*data[pi], *data[i])) {
	  break;
	}

	std::swap(data[i], data[pi]);
	intru_data_of(data[i]) = i;
	intru_data_of(data[pi]) = pi;
	i = pi;
      }
    } // sift_up

    void sift_down(index_t i) {
      while (i < count) {
	index_t li = lhs(i);
	index_t ri = rhs(i);

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
	    break;
	  }
	} else {
	  break;
	}
      }
    } // sift_down

    void sift(index_t i) {
      if (i == 0) {
	// if we're at top, can only go down
	sift_down(i);
      } else {
	index_t pi = parent(i);
	if (comparator(*data[pi], *data[i])) {
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
