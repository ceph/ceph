file(REMOVE_RECURSE
  "../../lib/libglobal-static.a"
  "../../lib/libglobal-static.pdb"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/global-static.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
