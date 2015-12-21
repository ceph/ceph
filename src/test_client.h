// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#include <mutex>
#include <thread>


class TestClient {

    int iops_goal;
    int outstanding_ops_allowed;
    int outstanding_ops;
    

public:

    TestClient() :
    {
        // empty
    }

    void submitResponse() {
        
    }
};
