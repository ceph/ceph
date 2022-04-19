file(REMOVE_RECURSE
  "../../lib/libcompressor.a"
  "../../lib/libcompressor.pdb"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/compressor.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
