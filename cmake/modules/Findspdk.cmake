# Findspdk.cmake -- locate a system-installed SPDK via pkg-config.
#
# Paired with WITH_SYSTEM_SPDK: link a distro-provided spdk-devel instead of
# building the bundled src/spdk fork. Modelled on Finddpdk.cmake.
#
# Provides: spdk::spdk, spdk_FOUND, SPDK_INCLUDE_DIRS

find_package(PkgConfig REQUIRED QUIET)

pkg_check_modules(SPDK IMPORTED_TARGET spdk_nvme spdk_env_dpdk)

# spdk's .so leave ISA-L symbols (crc32_iscsi, xor_gen, ...) undefined for the
# final link and don't Require isa-l in their .pc; pull it in explicitly.
pkg_check_modules(ISAL REQUIRED IMPORTED_TARGET libisal)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(spdk
  REQUIRED_VARS SPDK_FOUND SPDK_INCLUDE_DIRS)

if(spdk_FOUND AND NOT TARGET spdk::spdk)
  add_library(spdk::spdk INTERFACE IMPORTED)
  # SPDK/DPDK register drivers via C constructors; keep the libs in DT_NEEDED
  # (shared-lib equivalent of the bundled target's --whole-archive).
  set_target_properties(spdk::spdk PROPERTIES
    INTERFACE_LINK_LIBRARIES "-Wl,--no-as-needed;PkgConfig::SPDK;PkgConfig::ISAL")
endif()
