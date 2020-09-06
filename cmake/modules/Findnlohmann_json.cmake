
find_path(nlohmann_json_DIR nlohmann_jsonConfig.cmake)

if(EXISTS ${nlohmann_json_DIR})
    set(nlohmann_json_FOUND TRUE)
else()
    set(nlohmann_json_FOUND FALSE)
endif()