// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */

#include <iostream>
#include <memory>

#include "gtest/gtest.h"

#include "indirect_intrusive_heap.h"


struct Elem {
    int data;

    crimson::IndIntruHeapData heap_data;

    Elem(int _data) : data(_data) { }

    friend std::ostream& operator<<(std::ostream& out, const Elem& d) {
        out << d.data << " (" << d.heap_data << ")";
        return out;
    }
};


struct ElemCompare {
    bool operator()(const Elem& d1, const Elem& d2) {
        return d1.data < d2.data;
    }
};


TEST(IndIntruHeap, shared_ptr) {
    crimson::IndIntruHeap<std::shared_ptr<Elem>, Elem, &Elem::heap_data, ElemCompare> heap;

    EXPECT_TRUE(heap.empty());

    heap.push(std::make_shared<Elem>(2));

    EXPECT_FALSE(heap.empty());

    heap.push(std::make_shared<Elem>(99));
    heap.push(std::make_shared<Elem>(1));
    heap.push(std::make_shared<Elem>(-5));
    heap.push(std::make_shared<Elem>(12));
    heap.push(std::make_shared<Elem>(-12));
    heap.push(std::make_shared<Elem>(-7));

    // std::cout << heap << std::endl;

    EXPECT_FALSE(heap.empty());

    EXPECT_EQ(-12, heap.top().data);
    heap.pop();
    EXPECT_EQ(-7, heap.top().data);
    heap.pop();
    EXPECT_EQ(-5, heap.top().data);
    heap.pop();
    EXPECT_EQ(1, heap.top().data);
    heap.pop();
    EXPECT_EQ(2, heap.top().data);
    heap.pop();
    EXPECT_EQ(12, heap.top().data);
    heap.pop();
    EXPECT_EQ(99, heap.top().data);

    EXPECT_FALSE(heap.empty());
    heap.pop();
    EXPECT_TRUE(heap.empty());
}


TEST(IndIntruHeap, unique_ptr) {
    crimson::IndIntruHeap<std::unique_ptr<Elem>, Elem, &Elem::heap_data, ElemCompare> heap;

    EXPECT_TRUE(heap.empty());

    heap.push(std::unique_ptr<Elem>(new Elem(2)));

    EXPECT_FALSE(heap.empty());

    heap.push(std::unique_ptr<Elem>(new Elem(99)));
    heap.push(std::unique_ptr<Elem>(new Elem(1)));
    heap.push(std::unique_ptr<Elem>(new Elem(-5)));
    heap.push(std::unique_ptr<Elem>(new Elem(12)));
    heap.push(std::unique_ptr<Elem>(new Elem(-12)));
    heap.push(std::unique_ptr<Elem>(new Elem(-7)));

    EXPECT_FALSE(heap.empty());

    EXPECT_EQ(-12, heap.top().data);
    heap.pop();
    EXPECT_EQ(-7, heap.top().data);
    heap.pop();
    EXPECT_EQ(-5, heap.top().data);
    heap.pop();
    EXPECT_EQ(1, heap.top().data);
    heap.pop();
    EXPECT_EQ(2, heap.top().data);
    heap.pop();
    EXPECT_EQ(12, heap.top().data);
    heap.pop();
    EXPECT_EQ(99, heap.top().data);

    EXPECT_FALSE(heap.empty());
    heap.pop();
    EXPECT_TRUE(heap.empty());
}


TEST(IndIntruHeap, regular_ptr) {
    crimson::IndIntruHeap<Elem*, Elem, &Elem::heap_data, ElemCompare> heap;

    EXPECT_TRUE(heap.empty());

    heap.push(new Elem(2));

    EXPECT_FALSE(heap.empty());

    heap.push(new Elem(99));
    heap.push(new Elem(1));
    heap.push(new Elem(-5));
    heap.push(new Elem(12));
    heap.push(new Elem(-12));
    heap.push(new Elem(-7));

    EXPECT_FALSE(heap.empty());

    EXPECT_EQ(-12, heap.top().data);
    delete &heap.top();
    heap.pop();
    EXPECT_EQ(-7, heap.top().data);
    delete &heap.top();
    heap.pop();
    EXPECT_EQ(-5, heap.top().data);
    delete &heap.top();
    heap.pop();
    EXPECT_EQ(1, heap.top().data);
    delete &heap.top();
    heap.pop();
    EXPECT_EQ(2, heap.top().data);
    delete &heap.top();
    heap.pop();
    EXPECT_EQ(12, heap.top().data);
    delete &heap.top();
    heap.pop();
    EXPECT_EQ(99, heap.top().data);

    delete &heap.top();

    EXPECT_FALSE(heap.empty());
    heap.pop();
    EXPECT_TRUE(heap.empty());
}
