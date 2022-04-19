file(REMOVE_RECURSE
  "../../../lib/libec_jerasure_generic.pdb"
  "../../../lib/libec_jerasure_generic.so"
)

# Per-language clean rules from dependency scanning.
foreach(lang C CXX)
  include(CMakeFiles/ec_jerasure_generic.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
