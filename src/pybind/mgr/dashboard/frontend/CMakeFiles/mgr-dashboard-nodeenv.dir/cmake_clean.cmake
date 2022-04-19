file(REMOVE_RECURSE
  "dist"
  "node_modules"
  "CMakeFiles/mgr-dashboard-nodeenv"
  "node-env/bin/npm"
)

# Per-language clean rules from dependency scanning.
foreach(lang )
  include(CMakeFiles/mgr-dashboard-nodeenv.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
