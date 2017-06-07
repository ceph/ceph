if(K_WAY_HEAP)
  if(K_WAY_HEAP LESS 2)
    message(FATAL_ERROR "K_WAY_HEAP value should be at least 2")
  else()
    set(CMAKE_CXX_SIM_FLAGS "-DK_WAY_HEAP=${K_WAY_HEAP}")
  endif()
endif()

add_subdirectory(src)
