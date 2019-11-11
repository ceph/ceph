unset(CPPCHECK_BIN CACHE)
find_program(CPPCHECK_BIN cppcheck)

if(CPPCHECK_BIN)
  find_file(PROJECT_FILE compile_commands.json PATH .)
  if(NOT PROJECT_FILE)
    message(STATUS "Found cppcheck, but no \"compile_commands.json\" file. To enable cppcheck, set CMAKE_EXPORT_COMPILE_COMMANDS to \"ON\" and build again.")
    return()
  endif()

  include(ProcessorCount)
  ProcessorCount(N)
   
  execute_process(COMMAND cppcheck --version OUTPUT_VARIABLE CPPCHECK_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE)
  separate_arguments(CPPCHECK_VERSION)
  list(GET CPPCHECK_VERSION 1 VERSION_NUMBER)
  if(VERSION_NUMBER VERSION_GREATER_EQUAL 1.88)
    set(CPP_STD_VERSION "c++17")
  else()
    set(CPP_STD_VERSION "c++14")
  endif()

  add_custom_target(cppcheck
    COMMAND ${CPPCHECK_BIN} --verbose -j${N} --std=${CPP_STD_VERSION} --inline-suppr --project=${PROJECT_FILE} 2> cppcheck.txt | grep done
    )

  message(STATUS "Found cppcheck. To perform static analysis using cppcheck, use: \"make cppcheck\". Results will be stored in \"cppcheck.txt\".")
else()
  message(STATUS "Could not find cppcheck. To perform static analysis using cppcheck install the tool (e.g. \"yum install cppcheck\").")
endif()
