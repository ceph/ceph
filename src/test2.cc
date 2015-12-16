// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */

#include <iostream>

#include "rhheap.h"


struct Less {
  bool operator()(const int& l, const int& r) {
    return l < r;
  }
};


void myDisplay(rh::Heap<int, Less> h) {
  for (auto i = h.begin(); i != h.end(); ++i) {
    std::cout << *i << " ";
  }
  std::cout << std::endl;
}


void test(int data[], int count) {
  rh::Heap<int, Less> h;

  for (int i = 0; i < count; ++i) {
    h.push(data[i]);
  }

  std::cout << "======" << std::endl;

  std::cout << h << std::endl;
  myDisplay(h);

  if (!h.empty()) {
    h.top() = 5;
    h.updateTop();
    std::cout << h << std::endl;

    int item = h.top();
    h.pop();
    std::cout << item;
    while (!h.empty()) {
      int next = h.top();
      h.pop();
      std::cout << ", " << next;
      if (item > next) {
	std::cout << "*";
      }
      item = next;
    }
    std::cout << std::endl;
  }
}


#define COUNT(i) ((sizeof i) / (sizeof i[0]))

int main(int argc, char* argv[]) {
  int d1[] = {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
  int d2[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  int d3[] = {5, 2, 7, 1, 3, 0, 8, 6, 9, 4};
  int d4[] = {4, 9, 6, 8, 0, 3, 1, 7, 2, 5};
  int d5[] = {4};
  int d6[] = {};

  test(d1, COUNT(d1));
  test(d2, COUNT(d2));
  test(d3, COUNT(d3));
  test(d4, COUNT(d4));
  test(d5, COUNT(d5));
  test(d6, COUNT(d6));
}
