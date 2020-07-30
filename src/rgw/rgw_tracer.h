
#pragma once


//forward declaration of req_state defined /rgw/rgw_common.h
struct req_state;

#ifdef WITH_JAEGER
    #include "../common/tracer.h"
    //for getting the file (not a absolute path name)_name of the function
    #define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
    // structure to manage spans to trace functions who have access to req_state in , defined in req_common.cc*
    struct req_state_span{
        req_state* state = nullptr;
        bool is_inserted = false;
        inline void set_req_state(req_state* _s);
        inline void set_span(Span& span);
        inline ~req_state_span();
    };
    //method to trace functions who has access to req_state
    static inline void start_trace(req_state_span&& ss, Span&& sp, req_state* const s, const char* name);

    //jaeger initializer
    static inline void init_jager(){
        init_tracer("RGW_Client_Process","/home/abhinav/GSOC/ceph/src/tracerConfig.yaml");
    }
#else
    #define __FILENAME__ ""
    typedef char* Span;
    typedef char req_state_span;
    static inline Span trace(...) {return NULL;}
    static inline void finish_trace(...) {}
    static inline void start_trace(req_state_span&& ss, Span&& sp, req_state* const s, const char* name) {}
    static inline void init_jager(...) {}
    static inline void set_span_tag(...) {}
    static inline void get_span_name(...) {}

#endif
