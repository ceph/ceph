file(REMOVE_RECURSE
  "../../../lib/libec_jerasure.pdb"
  "../../../lib/libec_jerasure.so"
)

# Per-language clean rules from dependency scanning.
foreach(lang C CXX)
  include(CMakeFiles/ec_jerasure.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
