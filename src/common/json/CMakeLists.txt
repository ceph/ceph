add_library(json_structures STATIC
                BalancerStructures.cc ConfigStructures.cc OSDStructures.cc)

    target_link_libraries(json_structures global)