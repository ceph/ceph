/*
 * file:        request.h
 * description: generic inter-layer request mechanism
 * 
 * author:      Peter Desnoyers, Northeastern University
 * Copyright 2021, 2022 Peter Desnoyers
 * license:     GNU LGPL v2.1 or newer
 *              LGPL-2.1-or-later
 */

#ifndef REQUEST_H
#define REQUEST_H

/* generic interface for requests.
 *  - run(parent): begin execution
 *  - notify(rv): notification of completion
 *  - TODO: wait(): wait for completion
 */
class request {
public:
    virtual void wait() = 0;
    virtual void run(request *parent) = 0;
    virtual void notify(request *child) = 0;
    virtual void release() = 0;
    virtual ~request(){}
    request() {}
};

/* for callback-only request classes
 */
class trivial_request : public request {
public:
    trivial_request() {}
    ~trivial_request() {}
    virtual void notify(request *child) = 0;
    void wait() {}
    void run(request *parent) {}
    void release() {}
};

#endif
