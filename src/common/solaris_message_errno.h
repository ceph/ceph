#ifndef CEPH_SOLARIS_MESSAGE_ERRNO_H
#define CEPH_SOLARIS_MESSAGE_ERRNO_H

void translate_message_errno(CephContext *cct, Message *m);

#endif
