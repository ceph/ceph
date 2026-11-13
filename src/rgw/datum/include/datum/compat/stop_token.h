// Datum: Cancellation support for libraries without standard stop tokens.

#ifndef CEPH_RGW_DATUM_COMPAT_STOP_TOKEN_H
 #define CEPH_RGW_DATUM_COMPAT_STOP_TOKEN_H

#include <version>

#if __has_include(<stop_token>) and defined(__cpp_lib_jthread) and __cpp_lib_jthread >= 201911L
 #include <stop_token>

namespace ceph::salad::datum::compat {

using std::stop_source;
using std::stop_token;

} // ceph::salad::datum::compat

#else
 #include <atomic>
 #include <memory>

namespace ceph::salad::datum::compat {

// Polling-only fallback: shared lifetime and requests, without callbacks or waits.
class stop_source;

class stop_token final
{
    std::shared_ptr<const std::atomic<bool>> requested;

    friend class stop_source;

private:
    explicit stop_token(const std::shared_ptr<std::atomic<bool>>& state) noexcept
        : requested {state}
    {}

public:
    stop_token() = default;

public:
    [[nodiscard]] bool stop_requested() const noexcept
    {
        return requested and requested->load(std::memory_order_acquire);
    }

    friend bool operator==(const stop_token&, const stop_token&) noexcept = default;
};

class stop_source final
{
    std::shared_ptr<std::atomic<bool>> requested = std::make_shared<std::atomic<bool>>(false);

public:
    [[nodiscard]] stop_token get_token() const noexcept { return stop_token {requested}; }

    bool request_stop() noexcept
    {
        return requested and !requested->exchange(true, std::memory_order_acq_rel);
    }
};

} // ceph::salad::datum::compat

#endif

#endif // CEPH_RGW_DATUM_COMPAT_STOP_TOKEN_H
