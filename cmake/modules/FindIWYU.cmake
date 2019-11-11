unset(IWYU_BIN CACHE)
find_program(IWYU_BIN iwyu_tool.py)

if(IWYU_BIN)
  find_file(PROJECT_FILE compile_commands.json PATH .)
  if(NOT PROJECT_FILE)
    message(STATUS "Found IWYU, but no \"compile_commands.json\" file. To enable IWYU, set CMAKE_EXPORT_COMPILE_COMMANDS to \"ON\" and build again.")
    return()
  endif()

  if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    message(STATUS "Found IWYU, but clang++ is not used used. To enable IWYU, set CMAKE_C_COMPILER to \"clang\" and CMAKE_CXX_COMPILER to \"clang++\" and build again.")
    return()
  endif()

  include(ProcessorCount)
  ProcessorCount(N)
   
  add_custom_target(iwyu
    COMMAND echo "IWYU is analyzing includes - this may take a while..."
    COMMAND ${IWYU_BIN} -j${N} -p . > iwyu.txt
    )

  message(STATUS "Found IWYU. To perform header analysis using IWYU, use: \"make iwyu\". Results will be stored in \"iwyu.txt\".")
else()
  message(STATUS "Could not find IWYU. To perform header analysis using IWYU install the tool (e.g. \"yum install iwyu\").")
endif()
