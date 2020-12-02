
#pragma once

    #ifdef WITH_JAEGER
        #include "../common/tracer.h"
        //for getting the file (not a absolute path name)_name of the function
        #define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
        //jaeger initializer
        namespace jaeger_tracing
        {
            static inline void init_jaeger(){
                init_tracer("RGW_Client_Process");
            }
            static inline void disable_jaeger_tracing(bool&& state){
                disable(state);
            }
        }
    #else
    #include<memory>
    typedef char jspan;
    namespace jaeger_tracing
    {
            #define __FILENAME__ ""
            static inline std::unique_ptr<jspan> child_span(...) {return nullptr;}
            static inline std::unique_ptr<jspan> new_span(...) {return nullptr;}
            static inline void finish_span(...) {}
            static inline void init_jaeger(...) {}
            static inline void set_span_tag(...) {}
    }
    #endif