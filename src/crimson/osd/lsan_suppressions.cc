#ifndef _NDEBUG
// The callbacks we define here will be called from the sanitizer runtime, but
// aren't referenced from the Chrome executable. We must ensure that those
// callbacks are not sanitizer-instrumented, and that they aren't stripped by
// the linker.
#define SANITIZER_HOOK_ATTRIBUTE                                           \
  extern "C"                                                               \
  __attribute__((no_sanitize("address", "thread", "undefined")))           \
  __attribute__((visibility("default")))                                   \
  __attribute__((used))

static char kLSanDefaultSuppressions[] =
  "leak:InitModule\n"
  "leak:MallocExtension::Initialize\n"
  "leak:MallocExtension::Register\n";

SANITIZER_HOOK_ATTRIBUTE const char *__lsan_default_suppressions() {
  return kLSanDefaultSuppressions;
}
#endif // ! _NDEBUG
