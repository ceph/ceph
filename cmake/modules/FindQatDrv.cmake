# - Find_QatDrv
# Find the qat driver library and includes
#
# QatDrv_INCLUDE_DIRS = where to find cap.h, qae_mem.h, etc
# QatDrv_LIBRARIES - List of libraries when using qat driver
# QatDrv_FOUND - True if qat driver found

set(expected_version "v1.7.L.4.14.0-00031")
set(expected_version_url "https://www.intel.com/content/www/us/en/download/19081/intel-quickassist-technology-intel-qat-driver-for-linux-for-intel-server-boards-and-systems-based-on-intel-62x-chipset.html")

function(get_qatdrv_version versionfile)
  file(STRINGS "${versionfile}" QAT_VERSION_LINE
       REGEX "^PACKAGE_VERSION_MAJOR_NUMBER=[0-9]+$")
  string(REGEX REPLACE "^PACKAGE_VERSION_MAJOR_NUMBER=([0-9]+)$"
        "\\1" QAT_VERSION1 "${QAT_VERSION_LINE}")
  unset(QAT_VERSION_LINE)
  file(STRINGS "${versionfile}" QAT_VERSION_LINE
       REGEX "^PACKAGE_VERSION_MINOR_NUMBER=[0-9]+$")
  string(REGEX REPLACE "^PACKAGE_VERSION_MINOR_NUMBER=([0-9]+)$"
         "\\1" QAT_VERSION2 "${QAT_VERSION_LINE}")
  unset(QAT_VERSION_LINE)
  file(STRINGS "${versionfile}" QAT_VERSION_LINE
       REGEX "^PACKAGE_VERSION_PATCH_NUMBER=[0-9]+$")
  string(REGEX REPLACE "^PACKAGE_VERSION_PATCH_NUMBER=([0-9]+)$"
         "\\1" QAT_VERSION3 "${QAT_VERSION_LINE}")
  unset(QAT_VERSION_LINE)
  file(STRINGS "${versionfile}" QAT_VERSION_LINE
       REGEX "^PACKAGE_VERSION_BUILD_NUMBER=[0-9]+$")
  string(REGEX REPLACE "^PACKAGE_VERSION_BUILD_NUMBER=([0-9]+)$"
         "\\1" QAT_VERSION4 "${QAT_VERSION_LINE}")

  set(QatDrv_VERSION_MAJOR ${QAT_VERSION1} PARENT_SCOPE)
  set(QatDrv_VERSION_MINOR ${QAT_VERSION2} PARENT_SCOPE) 
  set(QatDrv_VERSION_PATCH ${QAT_VERSION3} PARENT_SCOPE)  
  set(QatDrv_VERSION_TWEAK ${QAT_VERSION4} PARENT_SCOPE)  
  set(QatDrv_VERSION_COUNT 4 PARENT_SCOPE)
  set(QatDrv_VERSION ${QAT_VERSION1}.${QAT_VERSION2}.${QAT_VERSION3}.${QAT_VERSION4} 
      PARENT_SCOPE) 
endfunction()

find_path(QATDRV_INCLUDE_DIR
  name quickassist/include/cpa.h
  HINTS $ENV{ICP_ROOT} /opt/APP/driver/QAT
  NO_DEFAULT_PATH)
if(QATDRV_INCLUDE_DIR)
  get_qatdrv_version(${QATDRV_INCLUDE_DIR}/versionfile)
  set(QatDrv_INCLUDE_DIRS
      ${QATDRV_INCLUDE_DIR}/quickassist/include
      ${QATDRV_INCLUDE_DIR}/quickassist/include/dc
      ${QATDRV_INCLUDE_DIR}/quickassist/lookaside/access_layer/include
      ${QATDRV_INCLUDE_DIR}/quickassist/include/lac
      ${QATDRV_INCLUDE_DIR}/quickassist/utilities/libusdm_drv
      ${QATDRV_INCLUDE_DIR}/quickassist/utilities/libusdm_drv/include)
endif()
foreach(component ${QatDrv_FIND_COMPONENTS})
  find_library(QatDrv_${component}_LIBRARIES
               NAMES ${component}
               HINTS ${QATDRV_INCLUDE_DIR}/build/)
  mark_as_advanced(QatDrv_INCLUDE_DIRS
                   QatDrv_${component}_LIBRARIES)
  list(APPEND QatDrv_LIBRARIES "${QatDrv_${component}_LIBRARIES}")  
endforeach()

set(failure_message  "Please ensure QAT driver has been installed with version no less than ${expected_version}. And set the environment variable \"ICP_ROOT\" for the QAT driver package root directory, i.e. \"export ICP_ROOT=/the/directory/of/QAT\". Or download and install QAT driver ${expected_version} manually at the link ${expected_version_url}. Remember to set the environment variable \"ICP_ROOT\" for the package root directory.")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(QatDrv
  REQUIRED_VARS QatDrv_LIBRARIES QatDrv_INCLUDE_DIRS
  VERSION_VAR QatDrv_VERSION
  FAIL_MESSAGE ${failure_message})

foreach(component ${QatDrv_FIND_COMPONENTS})
  if(NOT TARGET QatDrv::${component})
    add_library(QatDrv::${component} STATIC IMPORTED GLOBAL)
    set_target_properties(QatDrv::${component} PROPERTIES
                          INTERFACE_INCLUDE_DIRECTORIES "${QatDrv_INCLUDE_DIRS}"
                          INTERFACE_COMPILE_OPTIONS "-DHAVE_QATDRV"
                          IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                          IMPORTED_LOCATION "${QatDrv_${component}_LIBRARIES}")
  endif()

  # add alias targets to match FindQAT.cmake
  if(component STREQUAL "qat_s")
    add_library(QAT::qat ALIAS QatDrv::qat_s)
  elseif(component STREQUAL "usdm_drv_s")
    add_library(QAT::usdm ALIAS QatDrv::usdm_drv_s)
  endif()
endforeach()
