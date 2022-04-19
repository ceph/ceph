file(REMOVE_RECURSE
  "../../lib/liblibneorados.a"
  "../../lib/liblibneorados.pdb"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/libneorados.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
