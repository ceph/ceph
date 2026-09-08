#include <dlfcn.h>
#include <filesystem>
#include <vector>

#include "denc_registry.h"

namespace fs = std::filesystem;

class DencoderPlugin {
  using dencoders_t = std::vector<std::pair<std::string, Dencoder*>>;
public:
  DencoderPlugin(const fs::path& path) {
    mod = dlopen(path.c_str(), RTLD_NOW);
    if (mod == nullptr) {
      std::cerr << "failed to dlopen(" << path << "): " << dlerror() << std::endl;
    }
  }
  DencoderPlugin(DencoderPlugin&& other)
    : mod{other.mod},
      dencoders{std::move(other.dencoders)}
  {
    other.mod = nullptr;
    other.dencoders.clear();
  }
  ~DencoderPlugin() {
    unregister_dencoders();
#if defined(__has_feature)
#  if __has_feature(address_sanitizer) || __has_feature(leak_sanitizer)
#    define DENC_SKIP_DLCLOSE 1
#  endif
#endif
#if defined(__SANITIZE_ADDRESS__) && !defined(DENC_SKIP_DLCLOSE)
#  define DENC_SKIP_DLCLOSE 1
#endif
#if !defined(__FreeBSD__) && !defined(DENC_SKIP_DLCLOSE)
    // Skip dlclose under ASan/LSan: the leak checker at process exit needs
    // the .so still mapped to resolve symbols. Clang may not define
    // __SANITIZE_ADDRESS__ (e.g. clang-19), hence the __has_feature check.
    if (mod) {
      dlclose(mod);
    }
#endif
#undef DENC_SKIP_DLCLOSE
  }
  const dencoders_t& register_dencoders() {
    static constexpr std::string_view REGISTER_DENCODERS_FUNCTION = "register_dencoders\0";

    assert(mod);
    using register_dencoders_t = void (*)(DencoderPlugin*);
    const auto do_register =
      reinterpret_cast<register_dencoders_t>(dlsym(mod, REGISTER_DENCODERS_FUNCTION.data()));
    if (do_register == nullptr) {
      std::cerr << "failed to dlsym(" << REGISTER_DENCODERS_FUNCTION << "): "
                << dlerror() << std::endl;
      return dencoders;
    }
    do_register(this);
    return dencoders;
  }

  bool good() const {
    return mod != nullptr;
  }

  void unregister_dencoders() {
    while (!dencoders.empty()) {
      delete dencoders.back().second;
      dencoders.pop_back();
    }
  }
  template<typename DencoderT, typename...Args>
  void emplace(const char* name, Args&&...args) {
    dencoders.emplace_back(name, new DencoderT(std::forward<Args>(args)...));
  }

private:
  void *mod = nullptr;
  dencoders_t dencoders;
};

#define TYPE(t) plugin->emplace<DencoderImplNoFeature<t>>(#t, false, false);
#define TYPE_VARARGS(t, ...) plugin->emplace<DencoderImplNoFeature<t>>(#t, false, false, ##__VA_ARGS__);
#define TYPE_STRAYDATA(t) plugin->emplace<DencoderImplNoFeature<t>>(#t, true, false);
#define TYPE_NONDETERMINISTIC(t) plugin->emplace<DencoderImplNoFeature<t>>(#t, false, true);
#define TYPE_FEATUREFUL(t) plugin->emplace<DencoderImplFeatureful<t>>(#t, false, false);
#define TYPE_FEATUREFUL_STRAYDATA(t) plugin->emplace<DencoderImplFeatureful<t>>(#t, true, false);
#define TYPE_FEATUREFUL_NONDETERMINISTIC(t) plugin->emplace<DencoderImplFeatureful<t>>(#t, false, true);
#define TYPE_FEATUREFUL_NOCOPY(t) plugin->emplace<DencoderImplFeaturefulNoCopy<t>>(#t, false, false);
#define TYPE_NOCOPY(t) plugin->emplace<DencoderImplNoFeatureNoCopy<t>>(#t, false, false);
#define MESSAGE(t) plugin->emplace<MessageDencoderImpl<t>>(#t);

#define DENC_API extern "C" [[gnu::visibility("default")]]
