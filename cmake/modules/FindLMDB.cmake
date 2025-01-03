# Copyright (c) 2014, The Monero Project
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without modification, are
# permitted provided that the following conditions are met:
# 
# 1. Redistributions of source code must retain the above copyright notice, this list of
#    conditions and the following disclaimer.
# 
# 2. Redistributions in binary form must reproduce the above copyright notice, this list
#    of conditions and the following disclaimer in the documentation and/or other
#    materials provided with the distribution.
# 
# 3. Neither the name of the copyright holder nor the names of its contributors may be
#    used to endorse or promote products derived from this software without specific
#    prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
# THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
# THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

MESSAGE(STATUS "Looking for liblmdb")

FIND_PATH(LMDB_INCLUDE_DIR
  NAMES lmdb.h
  PATH_SUFFIXES include/ include/lmdb/
  PATHS "${PROJECT_SOURCE_DIR}"
  ${LMDB_ROOT}
  $ENV{LMDB_ROOT}
  /usr/local/
  /usr/
)

if(STATIC)
  if(MINGW)
    find_library(LMDB_LIBRARIES liblmdb.dll.a)
  else()
    find_library(LMDB_LIBRARIES liblmdb.a)
  endif()
else()
  find_library(LMDB_LIBRARIES lmdb)
endif()

IF(LMDB_INCLUDE_DIR)
  MESSAGE(STATUS "Found liblmdb include (lmdb.h) in ${LMDB_INCLUDE_DIR}")
  IF(LMDB_LIBRARIES)
    MESSAGE(STATUS "Found liblmdb library")
    set(LMDB_INCLUDE ${LMDB_INCLUDE_DIR})
    set(LMDB_LIBRARY ${LMDB_LIBRARIES})
  ELSE()
    MESSAGE(FATAL_ERROR "${BoldRed}Could not find liblmdb library, please make sure you have installed liblmdb or liblmdb-dev or the equivalent${ColourReset}")
  ENDIF()
ELSE()
  MESSAGE(FATAL_ERROR "${BoldRed}Could not find liblmdb library, please make sure you have installed liblmdb or liblmdb-dev or the equivalent${ColourReset}")
ENDIF()
