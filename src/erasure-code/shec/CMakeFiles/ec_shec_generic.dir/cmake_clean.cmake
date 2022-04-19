file(REMOVE_RECURSE
  "../../../lib/libec_shec_generic.pdb"
  "../../../lib/libec_shec_generic.so"
)

# Per-language clean rules from dependency scanning.
foreach(lang C CXX)
  include(CMakeFiles/ec_shec_generic.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
