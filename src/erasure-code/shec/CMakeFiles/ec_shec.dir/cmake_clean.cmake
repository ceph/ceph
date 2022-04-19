file(REMOVE_RECURSE
  "../../../lib/libec_shec.pdb"
  "../../../lib/libec_shec.so"
)

# Per-language clean rules from dependency scanning.
foreach(lang C CXX)
  include(CMakeFiles/ec_shec.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
