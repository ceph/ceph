// Defines global new/delete replacements; include in one test translation unit per executable.
#ifndef CEPH_TEST_CLS_RGW_NEW_LIMIT_H
 #define CEPH_TEST_CLS_RGW_NEW_LIMIT_H

#include <cstddef>
#include <cstdlib>
#include <limits>
#include <new>

namespace rgw_cls_test {

// A limit applies only to the decoding thread, leaving other test threads unaffected.
inline thread_local std::size_t maximum_new_size = std::numeric_limits<std::size_t>::max();

class scoped_new_limit final
{
    std::size_t previous;

public:
    explicit scoped_new_limit(const std::size_t maximum) noexcept
        : previous(maximum_new_size)
    {
        maximum_new_size = maximum;
    }

    scoped_new_limit(const scoped_new_limit&) = delete;

    scoped_new_limit& operator=(const scoped_new_limit&) = delete;

    ~scoped_new_limit() { maximum_new_size = previous; }

};

} // rgw_cls_test

void *operator new(const std::size_t size)
{
    if (size > rgw_cls_test::maximum_new_size) {
        throw std::bad_alloc {};
    }
    if (void *value = std::malloc(size == 0 ? 1 : size)) {
        return value;
    }
    throw std::bad_alloc {};
}

// Keep GCC from misdiagnosing this matched malloc/free pair after inlining.
[[gnu::noinline]] void operator delete(void *value) noexcept { std::free(value); }
[[gnu::noinline]] void operator delete(void *value, std::size_t) noexcept { std::free(value); }

#endif // CEPH_TEST_CLS_RGW_NEW_LIMIT_H
