// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#pragma once


#include <mutex>
#include <condition_variable>
#include <deque>

namespace crimson {
    template<typename T>
    class Queue {

        typedef std::unique_lock<std::mutex> Lock;

        size_t size;
        std::deque<T> queue;
        std::mutex mtx;
        std::condition_variable empty_cv;
        std::condition_variable full_cv;
        

    public:

        Queue(size_t _size) :
            size(_size)
        {
            // empty
        }

        void push(const T& item) {
            Lock l(mtx);
            while(queue.size() >= size) {
                full_cv.wait(l);
            }
            queue.push_back(item);
            empty_cv.notify_one();
        }

        void emplace(T&& item) {
            Lock l(mtx);
            while(queue.size() >= size) {
                full_cv.wait(l);
            }
            queue.emplace_back(item);
            empty_cv.notify_one();
        }

        T pull() {
            Lock l(mtx);
            while(queue.empty()) {
                empty_cv.wait(l);
            }
            auto result = queue.front();
            queue.pop_front();
            full_cv.notify_one();
            return result;
        }
    }; // class Queue
} // namespace crimson
