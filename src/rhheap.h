// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */

#include <vector>
#include <ostream>

namespace rh {
  template<typename T, typename C>
  class Heap {

  protected:

    std::vector<T> data;
    int count;
    C comparator;

    int parent(int i) { return (i - 1) / 2; }
    int lhs(int i) { return 2*i + 1; }
    int rhs(int i) { return 2*i + 2; }

    void siftDown(int i) {
      while (i < count) {
	int li = lhs(i);
	int ri = rhs(i);

	if (li < count) {
	  if (comparator(data[li], data[i])) {
	    if (ri < count && comparator(data[ri], data[li])) {
	      T temp = data[i];
	      data[i] = data[ri];
	      data[ri] = data[i];
	      i = ri;
	    } else {
	      T temp = data[i];
	      data[i] = data[li];
	      data[li] = data[i];
	      i = li;
	    }
	  } else if (ri < count && comparator(data[ri], data[i])) {
	    T temp = data[i];
	    data[i] = data[ri];
	    data[ri] = data[i];
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

    bool empty() const { return 0 == count; }

    T top() const { return data[0]; }

    void push(T item) {
      int i = count++;
      data.push_back(item);
      while (i > 0) {
	int pi = parent(i);
	if (comparator(data[pi], data[i])) {
	  break;
	}

	T temp = data[pi];
	data[pi] = data[i];
	data[i] = temp;
	i = pi;
      }
    }

    void pop() {
      data[0] = data[--count];
      // data.resize(count);
      siftDown(0);
    }

    void clear() {
      count = 0;
      data.resize(0);
    }

    void display() const {
      std::cout << "{";
      if (count) {
	std::cout << data[0];
      }
      for (int i = 1; i < count; i++) {
	std::cout << ", " << data[i];
      }
      std::cout << "}" << std::endl;
    }

#if 0
    template<typename U>
    friend std::ostream& operator<<(std::ostream&, const U&);
#endif
  }; // class Heap

#if 0
  template<typename T>
  std::ostream& operator<<(std::ostream& out, const T& h) {
    out << "{";
    if (h.count) {
      out << h.data[0];
    }
    for (int i = 1; i < h.count; i++) {
      out << ", " << h.data[i];
    }
    out << "}";
    return out;
  }
#endif

} // namespace
