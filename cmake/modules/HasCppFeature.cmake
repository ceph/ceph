
#[[
Check for presence of C++ features against feature test macros.
If not found, optionally use CPM to try to provide support for the feature.

C++ library feature test macros may be found here:
https://en.cppreference.com/cpp/feature_test#Library_features

Note: CMake 3.8 adds a similar feature, WriteCompilerDetectionHeader, which when
a bit more universal could be used to possibly make this all a bit faster.

EXAMPLE:
Test for the presence of a feature:
include(HasCppFeature)
has_cpp_feature(__cpp_lib_generator HAVE_CPP_DECLTYPE)
          
if(HAVE_CPP_DECLTYPE)
 message("-- C++ decltype support is active")
else()    
 message(FATAL_ERROR "-- C++ decltype is not present")
endif() 

EXAMPLE:
Test for the presence of a feature, try to install support if not
found:

include(HasCppFeature)
require_cpp_feature(__cpp_lib_generator HAVE_CPP_LIB_GENERATOR
    GIT_REPOSITORY https://github.com/lewissbaker/generator
    GIT_TAG main)

if(HAVE_CPP_LIB_GENERATOR)
 message("-- C++ std::generator support is active")
else()
 message("-- C++ std::generator support could not be provided")
endif()

]]#

include(CPM)
include(CheckCXXSourceCompiles)

# Check for cpp_feature_macro support, set variable ${our_name} if present:
# HAVE_CPP_LIB_GENERATOR if std::generator support is present: 
function(has_cpp_feature cpp_feature_macro our_name)
  message(STATUS "Checking for C++ feature: ${cpp_feature_macro} / ${our_name}")

  set(src "
      #include <version>
      #ifndef ${cpp_feature_macro}
       #error feature is not present: ${cpp_feature_macro}
      #endif
      int main() {}
    ")

   try_compile(${our_name}
      SOURCE_FROM_CONTENT src-check-for-${cpp_feature_macro}.cpp "${src}"
   )

  set(${our_name} ${${our_name}} PARENT_SCOPE)

  if(${our_name}) 
      message(STATUS "C++ feature available: ${cpp_feature_macro} / ${our_name}")
  else()
      message(STATUS "C++ feature not available: ${cpp_feature_macro} / ${our_name}")
  endif()
endfunction()

# Try to get the C++ feature using CPM if not already installed:
# (The extra arguments are passed right to CPM.)
function(obtain_cpp_feature cpp_feature_macro feature_tag_name our_name)
 has_cpp_feature(${cpp_feature_macro} feature_found)

 # Nothing to do if it's already ok:
 if(feature_found)
     set(${our_name} TRUE PARENT_SCOPE)
     return()
 endif()

 # Try to install; pass any remaining parameters to CPM and see 
 # if it can sort out the situation:
 CPMAddPackage(NAME ${feature_tag_name} ${ARGN})
 set(${our_name} TRUE PARENT_SCOPE)

 message(STATUS "C++ feature obtained: ${cpp_feature_macro} / ${our_name}")
endfunction()

# Try to obtain C++ feature if not available; fail if couldn't
# obtain (the extra arguments are passed right to CPM); the tag name will
# be the same as uppercase "feature tag name":
function(require_cpp_feature cpp_feature_macro feature_tag_name)
 string(TOUPPER "${feature_tag_name}" local_feature_tag_name)
 obtain_cpp_feature(${cpp_feature_macro} ${feature_tag_name} ${local_feature_tag_name} ${ARGN})

 if(NOT ${local_feature_tag_name})
  message(FATAL_ERROR "C++ feature ${cpp_feature_macro} / ${feature_tag_name} / ${local_feature_tag_name} could not be made available.")
 endif()

 set(${local_feature_tag_name} ${${local_feature_tag_name}} PARENT_SCOPE)
endfunction()

