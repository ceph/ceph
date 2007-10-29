// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */



#include <ext/rope>
#include "include/types.h"

#include "Message.h"
#include "Messenger.h"
#include "messages/MGenericMessage.h"

#include <cassert>
#include <iostream>
using namespace std;


// ---------
// incoming messages

void Messenger::dispatch(Message *m) 
{
  assert(dispatcher);
  dispatcher->dispatch(m);
}



