// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#include <string>
#include <iostream>

#include "intrusive_heap.h"


struct TestCompare;
struct TestIntruData;


class Test1 {
    friend TestCompare;
    friend TestIntruData;

    int data;
    crimson::IntruHeapData heap_data;

public:
    Test1(int _data) : data(_data) {}

    friend std::ostream& operator<<(std::ostream& out, const Test1& d) {
        out << d.data << " (" << d.heap_data << ")";
        return out;
    }

    int& the_data() { return data; }
};


struct TestCompare {
    bool operator()(const Test1& d1, const Test1& d2) {
        return d1.data < d2.data;
    }
};


struct TestIntruData {
    crimson::IntruHeapData& operator()(Test1& d) {
        return d.heap_data;
    }
};


int main(int argc, char** argv) {
    Test1 d1(2);
    Test1 d2(3);
    Test1 d3(1);
    Test1 d4(-5);

    crimson::IntruHeap<Test1, TestIntruData, TestCompare> my_heap;

    my_heap.push(d1);
    my_heap.push(d2);
    my_heap.push(d3);
    my_heap.push(d4);
    my_heap.push(Test1(-9));
    my_heap.push(Test1(99));
    my_heap.push(Test1(0));

    std::cout << my_heap << std::endl;

    auto& t = my_heap.top();
    t.the_data() = 17;
    my_heap.adjust_down(t);

    std::cout << my_heap << std::endl;

    my_heap.display_sorted(std::cout);

    while (!my_heap.empty()) {
        auto& top = my_heap.top();
        std::cout << top << std::endl;
        my_heap.pop();
        std::cout << my_heap << std::endl;
    }

    return 0;
}
