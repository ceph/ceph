set(MAX_COMPILE_MEM 3500 CACHE INTERNAL "maximum memory used by each compiling job (in MiB)")
set(MAX_LINK_MEM 4500 CACHE INTERNAL "maximum memory used by each linking job (in MiB)")

cmake_host_system_information(RESULT _num_cores QUERY NUMBER_OF_LOGICAL_CORES)
cmake_host_system_information(RESULT _total_mem QUERY TOTAL_PHYSICAL_MEMORY)

function(setup_ninja_job_pools kind memory_per_job)
  string(TOUPPER "${kind}" upper_kind)
  set(limit NINJA_MAX_${upper_kind}_JOBS)
  if(NOT ${limit})
    math(EXPR jobs "${_total_mem} / ${memory_per_job}")
    if(jobs LESS 1)
      set(jobs 1)
    elseif(jobs GREATER _num_cores)
      set(jobs ${_num_cores})
    endif()
    set(${limit} ${jobs} CACHE STRING
      "Maximum number of concurrent ${kind} jobs for Ninja." FORCE)
    mark_as_advanced(${limit})
  endif()

  math(EXPR heavy_jobs "${${limit}} / 2")
  if(heavy_jobs LESS 1)
    set(heavy_jobs 1)
  endif()
  set_property(GLOBAL APPEND PROPERTY JOB_POOLS
    avg_${kind}_job_pool=${${limit}}
    heavy_${kind}_job_pool=${heavy_jobs})
  set(CMAKE_JOB_POOL_${upper_kind} avg_${kind}_job_pool PARENT_SCOPE)
  set(_heavy_${kind}_jobs ${heavy_jobs} PARENT_SCOPE)
endfunction()

setup_ninja_job_pools(compile ${MAX_COMPILE_MEM})
setup_ninja_job_pools(link ${MAX_LINK_MEM})

# Share CPU threads among heavy links. Apply the same cap in both pools.
math(EXPR _ltrans_jobs "${_num_cores} / ${_heavy_link_jobs}")
if(_ltrans_jobs LESS 1)
  set(_ltrans_jobs 1)
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  # GCC 11 remembers -flto=auto even if a later flag specifies a job count.
  string(TOUPPER "${CMAKE_BUILD_TYPE}" _config)
  foreach(_flags CMAKE_C_FLAGS CMAKE_CXX_FLAGS CMAKE_EXE_LINKER_FLAGS
                 CMAKE_SHARED_LINKER_FLAGS CMAKE_MODULE_LINKER_FLAGS)
    foreach(_var "${_flags}" "${_flags}_${_config}")
      string(REPLACE "-flto=auto" "-flto=${_ltrans_jobs}" ${_var} "${${_var}}")
    endforeach()
  endforeach()
  foreach(_lang C CXX)
    list(TRANSFORM CMAKE_${_lang}_COMPILE_OPTIONS_IPO
      REPLACE "^-flto=auto$" "-flto=${_ltrans_jobs}")
  endforeach()
elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  # The driver only passes the job count to the linker when LTO is enabled.
  add_link_options("$<$<LINK_LANG_AND_ID:C,Clang,AppleClang>:-flto-jobs=${_ltrans_jobs}>"
                   "$<$<LINK_LANG_AND_ID:CXX,Clang,AppleClang>:-flto-jobs=${_ltrans_jobs}>")
endif()
