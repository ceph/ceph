// RGW CLS: Load the actual plugin and retain registered method callbacks.
#include "module_loader.h"

#include <cstdlib>
#include <dlfcn.h>

#include <map>
#include <memory>
#include <stdexcept>
#include <string>

namespace {

auto& methods()
{
    static std::map<std::string, ::rgw_cls_test::registered_method, std::less<>> registry;

    return registry;
}

void load_module()
{
    using handle = std::unique_ptr<void, decltype([](void *p) noexcept { dlclose(p); })>;

    const char *path = std::getenv("CEPH_TEST_CLS_RGW_MODULE");
    static const handle loaded {dlopen(path ? path : TEST_CLS_RGW_MODULE, RTLD_LAZY | RTLD_LOCAL)};
    if (!loaded) {
        throw std::runtime_error {dlerror()};
    }
    static const bool initialized = [] {
        const auto init = reinterpret_cast<void (*)()>(dlsym(loaded.get(), "__cls_init"));
        if (!init) {
            throw std::runtime_error {dlerror()};
        }
        init();

        return true;
    }();
    static_cast<void>(initialized);
}

} // anonymous namespace

::rgw_cls_test::registered_method rgw_cls_test::get_method(const std::string_view name)
{
    load_module();
    if (const auto found = methods().find(name); found != methods().end()) {
        return found->second;
    }

    throw std::runtime_error {"RGW CLS method is not registered: " + std::string {name}};
}

int cls_register(const char *, cls_handle_t *handle) { *handle = nullptr; return 0; }
int cls_log(int, const char *, ...) { return 0; }
int cls_register_cxx_filter(cls_handle_t, const std::string&,
                            cls_cxx_filter_factory_t, cls_filter_handle_t *) { return 0; }

namespace detail {

int cls_register_cxx_method_impl(cls_handle_t, const char *name, const int flags,
                                 cls_method_cxx_call_t call, cls_method_handle_t *handle)
{
    methods().insert_or_assign(name, ::rgw_cls_test::registered_method {call, flags});
    *handle = nullptr;

    return 0;
}

} // detail
