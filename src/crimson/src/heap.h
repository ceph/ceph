// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once


#include <vector>
#include <ostream>

#include "assert.h"


namespace crimson {
  
  /*
   * T : type of data held in the heap.
   * 
   * C : class that implements operator() with two arguments and
   * returns a boolean when the first argument is greater than (higher
   * in priority than) the second.
   */
  template<typename T, typename C>
  class Heap {

  public:
    
    class iterator {

      friend Heap<T,C>;

      Heap<T,C>& heap;
      int        index;

      iterator(Heap<T,C>& _heap, int _index) :
	heap(_heap),
	index(_index)
      {
	// empty
      }

    public:

      iterator(iterator&& other) :
	heap(other.heap),
	index(other.index)
      {
	// empty
      }

      iterator& operator++() {
	++index;
	return *this;
      }

      bool operator==(const iterator& other) const {
	return index == other.index;
      }

      bool operator!=(const iterator& other) const {
	return !(*this == other);
      }

      T& operator*() {
	return heap.data[index];
      }

      // the item this iterator refers to 
      void increase() {
	heap.siftUp(index);
      }
    }; // class iterator

    friend iterator;

  protected:

    std::vector<T> data;
    int count;
    C comparator;

    // parent(0) should be a negative value, which it is due to
    // truncating towards negative infinity
    static inline int parent(int i) { return (i - 1) / 2; }

    static inline int lhs(int i) { return 2*i + 1; }

    static inline int rhs(int i) { return 2*i + 2; }

    void siftUp(int i) {
      assert(i < count);

      while (i > 0) {
	int pi = parent(i);
	if (!comparator(data[i], data[pi])) {
	  break;
	}

	std::swap(data[i], data[pi]);
	i = pi;
      }
    }

    void siftDown(int i) {
      while (i < count) {
	int li = lhs(i);
	int ri = rhs(i);

	if (li < count) {
	  if (comparator(data[li], data[i])) {
	    if (ri < count && comparator(data[ri], data[li])) {
	      std::swap(data[i], data[ri]);
	      i = ri;
	    } else {
	      std::swap(data[i], data[li]);
	      i = li;
	    }
	  } else if (ri < count && comparator(data[ri], data[i])) {
	    std::swap(data[i], data[ri]);
	    i = ri;
	  } else {
	    break;
	  }
	} else {
	  break;
	}
      }
    }


  public:

    Heap() :
      count(0)
    {
      // empty
    }

    Heap(const Heap<T,C>& other) {
      data.resize(other.data.size());
      for (int i = 0; i < other.count; ++i) {
	data[i] = other.data[i];
      }
      count = other.count;
    }

    const Heap<T,C>& operator=(const Heap<T,C>& other) {
      data.resize(other.data.size());
      for (int i = 0; i < other.count; ++i) {
	data[i] = other.data[i];
      }
      count = other.count;
      return *this;
    }

    bool empty() const { return 0 == count; }

    T& top() { return data[0]; }

    void push(T item) {
      int i = count++;
      data.push_back(item);
      siftUp(i);
    }

    void pop() {
      data[0] = data[--count];
      data.resize(count);
      siftDown(0);
    }

    void updateTop() {
      siftDown(0);
    }

    void clear() {
      count = 0;
      data.resize(0);
    }

    iterator begin() {
      return iterator(*this, 0);
    }

    iterator end() {
      return iterator(*this, count);
    }

    std::ostream& displaySorted(std::ostream& out,
				std::function<bool(const T&)> filter,
				bool insert_line_breaks = true) const {
      Heap<T,C> temp = *this;

      bool first = true;
      out << "[ ";

      while(!temp.empty()) {
	const T& top = temp.top();
	if (filter(top)) {
	  if (!first) {
	    out << ", ";
	  }
	  if (insert_line_breaks) {
	    out << std::endl << "    ";
	  }
	  out << temp.top();
	  first = false;
	}
	temp.pop();
      }

      out << " ]";
      if (insert_line_breaks) {
	out << std::endl;
      }
      return out;
    }

    template<typename T1, typename T2>
    friend std::ostream& operator<<(std::ostream&, const Heap<T1,T2>&);
  }; // class Heap

  
  template<typename T1, typename T2>
  std::ostream& operator<<(std::ostream& out, const Heap<T1,T2>& h) {
    out << "[ ";
    if (h.count) {
      out << h.data[0];
    }
    for (int i = 1; i < h.count; i++) {
      out << ", " << h.data[i];
    }
    out << " ]";
    return out;
  }
} // namespace
