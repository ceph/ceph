# after https://www.topbug.net/blog/2012/03/17/generate-ctags-files-for-c-slash-c-plus-plus-source-files-and-all-of-their-included-header-files/ and
# https://stackoverflow.com/questions/9827208/run-a-shell-command-ctags-in-cmake-and-make

set_source_files_properties(tags PROPERTIES GENERATED true)
add_custom_target(ctags
   VERBATIM
   WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src
   COMMENT "Build ctags file src/tags (no submodules)"
   COMMAND
   ctags -R --c++-kinds=+p --fields=+iaS --extra=+q 
   --exclude=*ceph-object-corpus*
   --exclude=*Beast/*
   --exclude=*boost/*
   --exclude=*civetweb/*
   --exclude=*dpdk/*
   --exclude=*erasure-code/jerasure/*
   --exclude=*erasure-code/gf-complete/*
   --exclude=*googletest/*
   --exclude=*isa-l/*
   --exclude=*lua/*
   --exclude=*rocksdb/*
   --exclude=*spdk/*
   --exclude=*xxHash/*
   --exclude=*zstd/*
   --exclude=*.js
)
