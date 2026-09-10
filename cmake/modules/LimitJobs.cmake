set(MAX_COMPILE_MEM 3500 CACHE INTERNAL "maximum memory used by each compiling job (in MiB)")
set(MAX_LINK_MEM 4500 CACHE INTERNAL "maximum memory used by each linking job (in MiB)")

# Upstream's 3500/4500 MiB defaults appear tuned against gcc (Ceph's official
# CI and most Linux distro packages use gcc). An initial top -o res -s 1
# sample of clang++ compile jobs on this codebase showed a peak of ~819 MiB,
# but that was a handful of samples, not the full pool under load: at the
# resulting pool depth (26, from 1200 MiB), gstat showed mirror/swap and its
# underlying disks pegged at 100% busy on reads (i.e. actively paging back in,
# not writing out), and top showed most compile jobs sitting in "swread"
# state at <3% WCPU instead of compiling -- real swap thrashing, not a safe
# margin. 1800 MiB is the corrected estimate; revisit with another full-pool
# top/gstat pass if jobs still end up in swread.
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
  set(MAX_COMPILE_MEM 1800 CACHE INTERNAL "maximum memory used by each compiling job (in MiB)" FORCE)
  set(MAX_LINK_MEM 1000 CACHE INTERNAL "maximum memory used by each linking job (in MiB)" FORCE)
endif()

cmake_host_system_information(RESULT _num_cores QUERY NUMBER_OF_LOGICAL_CORES)
cmake_host_system_information(RESULT _total_mem QUERY TOTAL_PHYSICAL_MEMORY)

if(FREEBSD)
  # cmake_host_system_information() can come back empty (not just "0") on
  # FreeBSD if the underlying query is unsupported or fails. math(EXPR ...)
  # treats an empty operand as a hard configure-time error, not a silent
  # zero -- so guard both variables before any math() call ever sees them,
  # and warn loudly instead of failing quietly or aborting the configure.
  if(NOT _num_cores MATCHES "^[0-9]+$")
    message(STATUS "LimitJobs: NUMBER_OF_LOGICAL_CORES query returned '${_num_cores}' "
      "(not a positive integer) -- falling back to 1. Pass -DNINJA_MAX_COMPILE_JOBS= "
      "and -DNINJA_MAX_LINK_JOBS= explicitly to avoid relying on this detection.")
    set(_num_cores 1)
  endif()
  if(NOT _total_mem MATCHES "^[0-9]+$" OR _total_mem EQUAL 0)
    message(STATUS "LimitJobs: TOTAL_PHYSICAL_MEMORY query returned '${_total_mem}' "
      "(not a usable positive integer) -- trying sysctl(hw.physmem) directly.")
    execute_process(
      COMMAND sysctl -n hw.physmem
      OUTPUT_VARIABLE _physmem_bytes
      OUTPUT_STRIP_TRAILING_WHITESPACE
      ERROR_QUIET
      RESULT_VARIABLE _sysctl_result)
    if(_sysctl_result EQUAL 0 AND _physmem_bytes MATCHES "^[0-9]+$" AND NOT _physmem_bytes EQUAL 0)
      math(EXPR _total_mem "${_physmem_bytes} / 1048576")
      message(STATUS "LimitJobs: sysctl(hw.physmem) reports ${_total_mem} MiB -- using that instead.")
    else()
      message(WARNING "LimitJobs: sysctl(hw.physmem) also failed or returned 0 -- "
        "falling back to ${MAX_COMPILE_MEM} MiB (1 compile job). Pass "
        "-DNINJA_MAX_COMPILE_JOBS= and -DNINJA_MAX_LINK_JOBS= explicitly to avoid "
        "relying on this detection.")
      set(_total_mem "${MAX_COMPILE_MEM}")
    endif()
  endif()
endif()

# Reserve headroom for OS/filesystem cache pressure (on FreeBSD in particular,
# ZFS ARC competes for the same RAM and doesn't reliably shrink fast enough
# under sudden build-time memory pressure). Basing the job-count math on 85%
# of detected physical memory rather than the full amount leaves that margin
# without needing a second manually-tuned constant.
math(EXPR _total_mem "${_total_mem} * 85 / 100")

if(NINJA_MAX_COMPILE_JOBS)
  set(_avg_compile_jobs "${NINJA_MAX_COMPILE_JOBS}")
else()
  math(EXPR _avg_compile_jobs "${_total_mem} / ${MAX_COMPILE_MEM}")
  if(_avg_compile_jobs EQUAL 0)
    set(_avg_compile_jobs 1)
  endif()
  if(_num_cores LESS _avg_compile_jobs)
    set(_avg_compile_jobs "${_num_cores}")
  endif()
  set(NINJA_MAX_COMPILE_JOBS "${_avg_compile_jobs}" CACHE STRING
    "The maximum number of concurrent compilation jobs, for Ninja build system." FORCE)
  mark_as_advanced(NINJA_MAX_COMPILE_JOBS)
endif()
if(NINJA_MAX_COMPILE_JOBS)
  math(EXPR _heavy_compile_jobs "${_avg_compile_jobs} / 2")
  if(_heavy_compile_jobs EQUAL 0)
    set(_heavy_compile_jobs 1)
  endif()
  set_property(GLOBAL APPEND PROPERTY JOB_POOLS
    avg_compile_job_pool=${NINJA_MAX_COMPILE_JOBS}
    heavy_compile_job_pool=${_heavy_compile_jobs})
  set(CMAKE_JOB_POOL_COMPILE avg_compile_job_pool)
  if(FREEBSD)
    message(STATUS "LimitJobs: compile job pool depth = ${NINJA_MAX_COMPILE_JOBS} "
      "(cores=${_num_cores}, mem=${_total_mem}MiB)")
  endif()
endif()
if(NINJA_MAX_LINK_JOBS)
  set(_avg_link_jobs "${NINJA_MAX_LINK_JOBS}")
else()
  math(EXPR _avg_link_jobs "${_total_mem} / ${MAX_LINK_MEM}")
  if(_avg_link_jobs EQUAL 0)
    set(_avg_link_jobs 1)
  endif()
  if(_num_cores LESS _avg_link_jobs)
    set(_avg_link_jobs "${_num_cores}")
  endif()
  set(NINJA_MAX_LINK_JOBS "${_avg_link_jobs}" CACHE STRING
    "The maximum number of concurrent link jobs, for Ninja build system." FORCE)
  mark_as_advanced(NINJA_MAX_LINK_JOBS)
endif()
if(NINJA_MAX_LINK_JOBS)
  math(EXPR _heavy_link_jobs "${_avg_link_jobs} / 2")
  if(_heavy_link_jobs EQUAL 0)
    set(_heavy_link_jobs 1)
  endif()
  set_property(GLOBAL APPEND PROPERTY JOB_POOLS
    avg_link_job_pool=${NINJA_MAX_LINK_JOBS}
    heavy_link_job_pool=${_heavy_link_jobs})
  set(CMAKE_JOB_POOL_LINK avg_link_job_pool)
  if(FREEBSD)
    message(STATUS "LimitJobs: link job pool depth = ${NINJA_MAX_LINK_JOBS} "
      "(cores=${_num_cores}, mem=${_total_mem}MiB)")
  endif()
endif()
