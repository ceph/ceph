macro(check_nasm_support _object_format _support_x64 _support_x64_and_avx2 _support_x64_and_avx512)
  execute_process(
    COMMAND which nasm
    RESULT_VARIABLE no_nasm
    OUTPUT_QUIET
    ERROR_QUIET)
  if(NOT no_nasm)
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
        execute_process(COMMAND nasm -f ${object_format} -i
          ${CMAKE_SOURCE_DIR}/src/isa-l/include/
          ${CMAKE_SOURCE_DIR}/src/isa-l/erasure_code/gf_vect_dot_prod_avx2.asm
          -o /dev/null
          RESULT_VARIABLE rc
          OUTPUT_QUIET
          ERROR_QUIET)
        if(NOT rc)
          set(${_support_x64_and_avx2} TRUE)
        endif(NOT rc)
        execute_process(COMMAND nasm -D HAVE_AS_KNOWS_AVX512 -f ${object_format}
          -i ${CMAKE_SOURCE_DIR}/src/isa-l/include/
          ${CMAKE_SOURCE_DIR}/src/isa-l/erasure_code/gf_vect_dot_prod_avx512.asm
          -o /dev/null
          RESULT_VARIABLE rt
          OUTPUT_QUIET
          ERROR_QUIET)
        if(NOT rt)
          set(${_support_x64_and_avx512} TRUE)
        endif(NOT rt)
      endif(not_arch_x32)
    endif(CMAKE_SYSTEM_PROCESSOR MATCHES "amd64|x86_64")
  endif(NOT no_nasm)
  if(no_nasm)
    message(STATUS "Could NOT find nasm")
  elseif(NOT not_arch_x32)
    message(STATUS "Found nasm: but x86_64 with x32 ABI is not supported")
  endif()
  if(${_support_x64_and_avx512})
    message(STATUS "Found nasm: best -- capable of assembling AVX512")
  endif()
  if(${_support_x64_and_avx2})
    message(STATUS "Found nasm: better -- capable of assembling AVX2")
  endif()
  if(${_support_x64})
    message(STATUS "Found nasm: good -- capable of assembling x86_64")
  endif()
endmacro()
