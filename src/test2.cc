// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */

#include <iostream>

#include "crimson/heap.h"


namespace c = crimson;


struct Less {
  bool operator()(const int& l, const int& r) {
    return l < r;
  }
};

struct More {
  bool operator()(const int& l, const int& r) {
    return l > r;
  }
};


template<typename T, typename C>
void myDisplay(c::Heap<T, C>& h) {
  for (auto i = h.begin(); i != h.end(); ++i) {
    std::cout << *i << " ";
  }
  std::cout << std::endl;
}


template<typename T, typename C>
void addToHeap(c::Heap<T, C>& h, T data[], int count) {
  for (int i = 0; i < count; ++i) {
    h.push(data[i]);
  }
}


template<typename T, typename C>
void destructiveDisplay(c::Heap<T, C>& h) {
  C compare;
    int item = h.top();
    h.pop();
    std::cout << item;
    while (!h.empty()) {
      int next = h.top();
      h.pop();
      std::cout << ", " << next;
      if (compare(next, item)) {
	std::cout << "*";
      }
      item = next;
    }
    std::cout << std::endl;
}


template<typename C>
void test(int data[], int count) {
  c::Heap<int, C> h;

  addToHeap(h, data, count);

  std::cout << "======" << std::endl;

  std::cout << h << std::endl;
  myDisplay(h);

  if (!h.empty()) {
    h.top() = 5;
    h.updateTop();
    std::cout << h << std::endl;
    destructiveDisplay(h);
  }
}


void testIncrease(int data[], int count) {
  c::Heap<int,Less> h1;
  addToHeap(h1, data, count);
  int j = 0;
  for (auto i = h1.begin(); i != h1.end(); ++i, ++j) {
    if (j == count/2) {
      (*i) = -1;
      i.increase();
    }
  }
  std::cout << h1 << std::endl;
  destructiveDisplay(h1);
}


#define COUNT(i) ((sizeof i) / (sizeof i[0]))

int main(int argc, char* argv[]) {
  int d1[] = {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
  int d2[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  int d3[] = {5, 2, 7, 1, 3, 0, 8, 6, 9, 4};
  int d4[] = {4, 9, 6, 8, 0, 3, 1, 7, 2, 5};
  int d5[] = {4};
  int d6[] = {};

  test<Less>(d1, COUNT(d1));
  test<Less>(d2, COUNT(d2));
  test<Less>(d3, COUNT(d3));
  test<Less>(d4, COUNT(d4));
  test<Less>(d5, COUNT(d5));
  test<Less>(d6, COUNT(d6));

  test<More>(d1, COUNT(d1));
  test<More>(d2, COUNT(d2));

  testIncrease(d1, COUNT(d1));
  testIncrease(d2, COUNT(d2));
}
