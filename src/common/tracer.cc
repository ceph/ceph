#include "tracer.h"

Jaeger_Tracer tracer;
std::atomic<bool> jaeger_initialized(false);

Jaeger_Tracer::~Jaeger_Tracer()
{
    if(this->tracer)
        this->tracer->Close();
    jaeger_initialized = false;
}