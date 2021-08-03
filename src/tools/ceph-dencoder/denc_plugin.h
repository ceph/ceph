#include <dlfcn.h>
#include <filesystem>

#include "denc_registry.h"

namespace fs = std::filesystem;

class DencoderPlugin {
public:
  DencoderPlugin(const fs::path& path) {
    mod = dlopen(path.c_str(), RTLD_NOW);
    if (mod == nullptr) {
      std::cerr << "failed to dlopen(" << path << "): " << dlerror() << std::endl;
    }
  }
  ~DencoderPlugin() {
#if !defined(__FreeBSD__)
    if (mod) {
      dlclose(mod);
    }
#endif
  }
  int register_dencoders(DencoderRegistry& registry) {
    static constexpr string_view REGISTER_DENCODERS_FUNCTION = "register_dencoders\0";

    assert(mod);
    using register_dencoders_t = void (*)(DencoderRegistry&);
    const auto do_register =
      reinterpret_cast<register_dencoders_t>(dlsym(mod, REGISTER_DENCODERS_FUNCTION.data()));
    if (do_register == nullptr) {
      std::cerr << "failed to dlsym(" << REGISTER_DENCODERS_FUNCTION << ")" << std::endl;
      return -1;
    }
    const unsigned nr_before = registry.get().size();
    do_register(registry);
    const unsigned nr_after = registry.get().size();
    return nr_after - nr_before;
  }
  bool good() const {
    return mod != nullptr;
  }
private:
  void *mod = nullptr;
};
