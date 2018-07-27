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
