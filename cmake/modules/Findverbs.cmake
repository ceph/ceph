# - Find rdma verbs
# Find the rdma verbs library and includes
#
# VERBS_INCLUDE_DIR - where to find ibverbs.h, etc.
# VERBS_LIBRARIES - List of libraries when using ibverbs.
# VERBS_FOUND - True if ibverbs found.
# HAVE_IBV_EXP - True if experimental verbs is enabled.

find_path(VERBS_INCLUDE_DIR infiniband/verbs.h)
find_library(VERBS_LIBRARIES ibverbs)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(verbs DEFAULT_MSG VERBS_LIBRARIES VERBS_INCLUDE_DIR)

if(VERBS_FOUND)
  include(CheckCXXSourceCompiles)
  CHECK_CXX_SOURCE_COMPILES("
    #include <infiniband/verbs.h>
    int main() {
      struct ibv_context* ctxt;
      struct ibv_exp_gid_attr gid_attr;
      ibv_exp_query_gid_attr(ctxt, 1, 0, &gid_attr);
      return 0;
    } " HAVE_IBV_EXP)
  if(NOT TARGET IBVerbs::verbs)
    add_library(IBVerbs::verbs UNKNOWN IMPORTED)
  endif()
  set_target_properties(IBVerbs::verbs PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${VERBS_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${VERBS_LIBRARIES}")
endif()

mark_as_advanced(
  VERBS_LIBRARIES
)
