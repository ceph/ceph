// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once


#include <vector>
#include <string>
#include <iostream>
#include <functional>

#include "assert.h"


namespace crimson {
  using IntruHeapData = size_t;

  // T = type of data in heap; I = functor that returns a non-const
  // reference to IntruHeapData; C = functor that compares two const
  // refs and return true if the first precedes the second
  template<typename T, typename I, typename C>
  class IntruHeap {

    static_assert(
      std::is_same<IntruHeapData&,typename std::result_of<I(T&)>::type>::value,
      "class I must define operator() to take T& and return a IntruHeapData&.");

    static_assert(
      std::is_same<bool,typename std::result_of<C(const T&,const T&)>::type>::value,
      "class C must define operator() to take two const T& and return a bool.");


  protected:
    using index_t = IntruHeapData;

    std::vector<T> data;
    index_t count;
    I intru_data_of;
    C comparator;

  public:

    IntruHeap() :
      count(0)
    {
      // empty
    }

    IntruHeap(const IntruHeap<T,I,C>& other) :
      count(other.count)
    {
      for (uint i = 0; i < other.count; ++i) {
	data.push_back(other.data[i]);
      }
    }

    bool empty() const { return 0 == count; }

    T& top() { return data[0]; }

    void push(T&& item) {
      index_t i = count++;
      intru_data_of(item) = i;
      data.emplace_back(item);
      sift_up(i);
    }

    void push(const T& item) {
      T copy(item);
      push(std::move(copy));
    }

    void pop() {
      std::swap(data[0], data[--count]);
      intru_data_of(data[0]) = 0;
      data.pop_back();
      sift_down(0);
    }

    void adjust_up(T& item) {
      sift_up(intru_data_of(item));
    }

    void adjust_down(T& item) {
      sift_down(intru_data_of(item));
    }

    void adjust(T& item) {
      sift(intru_data_of(item));
    }

    friend std::ostream& operator<<(std::ostream& out, const IntruHeap& h) {
      for (uint i = 0; i < h.count; ++i) {
	out << h.data[i] << ", ";
      }
      return out;
    }

    std::ostream&
    display_sorted(std::ostream& out,
		   bool insert_line_breaks = true,
		   std::function<bool(const T&)> filter = all_filter) const {
      IntruHeap<T,I,C> copy = *this;

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
	if (!comparator(data[i], data[pi])) {
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
	  if (comparator(data[li], data[i])) {
	    if (ri < count && comparator(data[ri], data[li])) {
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
	  } else if (ri < count && comparator(data[ri], data[i])) {
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
	if (comparator(data[i], data[pi])) {
	  // if we can go up, we will
	  sift_up(i);
	} else {
	  // otherwise we'll try to go down
	  sift_down(i);
	}
      }
    } // sift
  }; // class IntruHeap
} // namespace crimson
