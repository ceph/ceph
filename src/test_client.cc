// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#include "test_client.h"


TestClient::TestClient(int _iops_goal, int _outstanding_ops_allowed) :
  iops_goal(_iops_goal),
  outstanding_ops_allowed(_outstanding_ops_allowed)
{
  thread = std::thread(&TestClient::run, this);
}


TestClient::~TestClient() {
  if (thread.joinable()) {
    thread.join();
  }
}

void TestClient::run() {
}

void TestClient::submitResponse() {
  Guard g(mtx);
  --outstanding_ops;
  cv.notify_one();
}

void TestClient::waitForDone() {
  thread.join();
}
