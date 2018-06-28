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
endif()

mark_as_advanced(
  VERBS_LIBRARIES
)
