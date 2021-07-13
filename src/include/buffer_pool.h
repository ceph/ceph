// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_BUFFER_POOL_H
#define CEPH_BUFFER_POOL_H

#include <memory>
#include <mutex>
#include <vector>

template <typename T>
class BufferPool {
public:
    ~BufferPool() {
      std::lock_guard<std::mutex> l{m_lock};
      while (!m_buffers.empty()) {
        auto buf = m_buffers.back();
        ::operator delete(buf);
        m_buffers.pop_back();
      }
      std::vector<T*> empty;
      m_buffers.swap(empty);
    }

    T* get() {
      std::lock_guard<std::mutex> l{m_lock};
      if (m_buffers.empty()) {
        return nullptr;
      }

      auto buf = m_buffers.back();
      m_buffers.pop_back();
      return buf;
    }

    void put(T* buf) {
      std::lock_guard<std::mutex> l(m_lock);
      m_buffers.push_back(buf);
    }

private:
    std::mutex m_lock;
    std::vector<T*> m_buffers;
};

class PooledObject {
	public:
    static void* operator new(std::size_t sz);
    static void operator delete(void* ptr, std::size_t sz);

    static constexpr int POOLED_OBJECT_MAX_SIZE = 2048;
    static std::array<BufferPool<void>, POOLED_OBJECT_MAX_SIZE> object_pools;
};

template <typename T>
class PooledAllocator : public std::allocator<T> {
public:
    PooledAllocator() {
    }

    template <typename U>
    PooledAllocator(const PooledAllocator<U>& other) throw() {
    };

    template<typename U>
    struct rebind {
        typedef PooledAllocator<U> other;
    };

    T* allocate(std::size_t n) {
      return (T*)PooledObject::operator new(n * sizeof(T));
    }

    void deallocate(T* p, std::size_t n) {
      PooledObject::operator delete(p, n * sizeof(T));
    }
};

#endif // CEPH_BUFFER_POOL_H
