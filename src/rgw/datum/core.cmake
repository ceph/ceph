# Datum's pure data/algebra target, shared by native and SALAD consumers.
# Please keep reflection, codecs and native dependencies in their own targets.
include_guard(GLOBAL)

add_library(datum_core INTERFACE)
add_library(datum::core ALIAS datum_core)
target_include_directories(datum_core INTERFACE "${CMAKE_CURRENT_LIST_DIR}/include")
target_compile_features(datum_core INTERFACE cxx_std_26)
