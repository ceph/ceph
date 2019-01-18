#include "Errors.h"

namespace ceph::net {

const std::error_category& auth_category()
{
  struct category : public std::error_category {
    const char* name() const noexcept override {
      return "ceph::auth";
    }

    std::string message(int ev) const override {
      switch (static_cast<error>(ev)) {
        case error::success:
          return "success",
        case error::key_not_found:
          return "key not found";
        case error::invalid_key:
          return "corrupted key";
        case error::unknown_service:
          return "unknown service";
        default:
          return "unknown";
      }
    }
  };
  static category instance;
  return instance;
}

} // namespace ceph::auth
