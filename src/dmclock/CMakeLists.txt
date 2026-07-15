add_subdirectory(src)

# Determine if dmclock is built as a subproject (using add_subdirectory)
# or if it is the master project.
set(MASTER_PROJECT FALSE)

option(dmclock_TEST "Generate test targets" ${MASTER_PROJECT})
if(dmclock_TEST)
  if (NOT(TARGET gtest AND TARGET gtest_main))
    if (NOT GTEST_FOUND)
      find_package(GTest QUIET)
      if (NOT GTEST_FOUND)
        include(BuildGTest)
      endif()
    endif()
  endif()

  enable_testing()
  add_subdirectory(test)
  add_subdirectory(support/test)
  add_subdirectory(sim)
endif()
