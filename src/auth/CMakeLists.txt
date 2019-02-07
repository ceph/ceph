set(auth_srcs
  AuthClientHandler.cc
  AuthMethodList.cc
  AuthRegistry.cc
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

if(HAVE_GSSAPI)
  list(APPEND auth_srcs
    krb/KrbAuthorizeHandler.cpp
    krb/KrbClientHandler.cpp
    krb/KrbProtocol.cpp
    krb/KrbSessionHandler.hpp)
endif()

add_library(common-auth-objs OBJECT ${auth_srcs})
if(WITH_SEASTAR)
  add_library(crimson-auth OBJECT ${auth_srcs})
  target_compile_definitions(crimson-auth PRIVATE
    "WITH_SEASTAR=1")
  target_include_directories(crimson-auth PRIVATE
    $<TARGET_PROPERTY:Seastar::seastar,INTERFACE_INCLUDE_DIRECTORIES>)
endif()
