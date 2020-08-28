#include "tracer.h"

Jager_Tracer tracer;
std::atomic<bool> jager_initialized(false);

Jager_Tracer::~Jager_Tracer()
{
    if(this->tracer)
        this->tracer->Close();
    jager_initialized = false;
}