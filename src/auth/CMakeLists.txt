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
  none/AuthNoneAuthorizeHandler.cc)

if(HAVE_GSSAPI)
  list(APPEND auth_srcs
    krb/KrbAuthorizeHandler.cpp
    krb/KrbClientHandler.cpp
    krb/KrbProtocol.cpp
    krb/KrbSessionHandler.hpp)
endif()

add_library(common-auth-objs OBJECT ${auth_srcs})
target_include_directories(common-auth-objs PRIVATE ${OPENSSL_INCLUDE_DIR})
target_link_libraries(common-auth-objs legacy-option-headers)
