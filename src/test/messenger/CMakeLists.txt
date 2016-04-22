add_executable(simple_server
  simple_server.cc
  simple_dispatcher.cc
  )
target_link_libraries(simple_server
  os global common ${Boost_REGEX_LIBRARY}
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(simple_client
  simple_client.cc
  simple_dispatcher.cc
  )
target_link_libraries(simple_client
  os global common ${Boost_REGEX_LIBRARY}
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

if(HAVE_XIO)
  add_executable(xio_server
    xio_server.cc
    xio_dispatcher.cc
    )
  target_link_libraries(xio_server
    os global common ${Boost_REGEX_LIBRARY}
    ${Xio_LIBRARY} pthread rt
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )

  add_executable(xio_client
    xio_client.cc
    xio_dispatcher.cc
    )
  target_link_libraries(xio_client
    os global common ${Boost_REGEX_LIBRARY}
    ${Xio_LIBRARY} pthread rt
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
endif(HAVE_XIO)

