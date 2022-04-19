file(REMOVE_RECURSE
  "../../lib/libglobal.a"
  "../../lib/libglobal.pdb"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/global.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
