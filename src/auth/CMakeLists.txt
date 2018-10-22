set(auth_srcs
  AuthAuthorizeHandler.cc
  AuthClientHandler.cc
  AuthMethodList.cc
  AuthSessionHandler.cc
  Crypto.cc
  KeyRing.cc
  RotatingKeyRing.cc
  cephx/CephxAuthorizeHandler.cc
  cephx/CephxClientHandler.cc
  cephx/CephxProtocol.cc
  cephx/CephxSessionHandler.cc
  none/AuthNoneAuthorizeHandler.cc
  unknown/AuthUnknownAuthorizeHandler.cc)

add_library(common-auth-objs OBJECT ${auth_srcs})
if(WITH_SEASTAR)
  add_library(crimson-auth OBJECT ${auth_srcs})
  target_compile_definitions(crimson-auth PRIVATE
    "WITH_SEASTAR=1")
  target_include_directories(crimson-auth PRIVATE
    $<TARGET_PROPERTY:Seastar::seastar,INTERFACE_INCLUDE_DIRECTORIES>)
endif()
