file(REMOVE_RECURSE
  "dist"
  "node_modules"
  "CMakeFiles/mgr-dashboard-frontend-deps"
  "node_modules"
  "package.json"
)

# Per-language clean rules from dependency scanning.
foreach(lang )
  include(CMakeFiles/mgr-dashboard-frontend-deps.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
