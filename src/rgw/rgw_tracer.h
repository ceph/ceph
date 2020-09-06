
#pragma once

#ifdef WITH_JAEGER
    #include "../common/tracer.h"
    //for getting the file (not a absolute path name)_name of the function
    #define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
    //jaeger initializer
    static inline void init_jaeger(){
        init_tracer("RGW_Client_Process","/home/abhinav/Desktop/tracerConfig.yaml");
    }
#else
    #define __FILENAME__ ""
    typedef char* Span;
    static inline Span child_span(...) {return nullptr;}
    static inline Span new_span(...) {return nullptr;}
    static inline void finish_trace(...) {}
    static inline void init_jaeger(...) {}
    static inline void set_span_tag(...) {}
#endif
