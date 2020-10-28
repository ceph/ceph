macro(check_yasm_support _object_format _support_x64 _support_avx2 _support_avx512)
  execute_process(
    COMMAND yasm -f "${_object_format}" ${CMAKE_SOURCE_DIR}/src/common/crc32c_intel_fast_asm.s -o /dev/null
    RESULT_VARIABLE no_yasm
    OUTPUT_QUIET
    ERROR_QUIET)
  if(NOT no_yasm)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "amd64|x86_64")
      set(save_quiet ${CMAKE_REQUIRED_QUIET})
      set(CMAKE_REQUIRED_QUIET true)
      include(CheckCXXSourceCompiles)
      check_cxx_source_compiles("
      #if defined(__x86_64__) && defined(__ILP32__)
      #error x32
      #endif
      int main() {}
      " not_arch_x32)
      set(CMAKE_REQUIRED_QUIET ${save_quiet})
      if(not_arch_x32)
        set(${_support_x64} TRUE)
        execute_process(COMMAND yasm -f ${object_format} -i
          ${CMAKE_SOURCE_DIR}/src/isa-l/include/
          ${CMAKE_SOURCE_DIR}/src/isa-l/erasure_code/gf_vect_dot_prod_avx2.asm
          -o /dev/null
          RESULT_VARIABLE rc
          OUTPUT_QUIET
          ERROR_QUIET)
        if(NOT rc)
          set(${_support_avx2} TRUE)
        endif(NOT rc)
        execute_process(COMMAND yasm -D HAVE_AS_KNOWS_AVX512 -f ${object_format}
          -i ${CMAKE_SOURCE_DIR}/src/isa-l/include/
          ${CMAKE_SOURCE_DIR}/src/isa-l/erasure_code/gf_vect_dot_prod_avx512.asm
          -o /dev/null
          RESULT_VARIABLE not_support_avx512
          OUTPUT_QUIET
          ERROR_QUIET)
        if(NOT not_support_avx512)
          set(${_support_avx512} TRUE)
        endif(NOT not_support_avx512)
      endif(not_arch_x32)
    endif(CMAKE_SYSTEM_PROCESSOR MATCHES "amd64|x86_64")
  endif(NOT no_yasm)
  if(no_yasm)
    message(STATUS "Could NOT find Yasm")
  elseif(NOT not_arch_x32)
    message(STATUS "Found Yasm: but x86_64 with x32 ABI is not supported")
  endif()
  if(${_support_avx512})
    message(STATUS "Found Yasm: best -- capable of assembling AVX512")
  endif()
  if(${_support_avx2})
    message(STATUS "Foudd Yasm: better -- capable of assembling AVX2")
  endif()
  if(${_support_x64})
    message(STATUS "Found Yasm: good -- capable of assembling x86_64")
  endif()
endmacro()
