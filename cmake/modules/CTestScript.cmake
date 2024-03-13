function(build_tests)
  find_program(MEMORYCHECK_COMMAND valgrind)  
  set(MEMORYCHECK_COMMAND_OPTIONS  
    "--trace-children=no --leak-check=full --soname-synonyms=somalloc=*tcmalloc* \
    --child-silent-after-fork=yes")  
  set(MEMORYCHECK_SUPPRESSIONS_FILE  
    "${CMAKE_SOURCE_DIR}/qa/valgrind.supp")
endfunction()
