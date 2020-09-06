#ifndef TRACER_H_
#define TRACER_H_

#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1

#include<cstring>
#include <arpa/inet.h>
#include <yaml-cpp/yaml.h>
#include<atomic>

#include <jaegertracing/Tracer.h>
#include <jaegertracing/net/IPAddress.h>
#include <jaegertracing/net/Socket.h>

static std::atomic<bool> jaeger_initialized(false);

struct Jaeger_Tracer{
    Jaeger_Tracer(){}
    ~Jaeger_Tracer(){
      if(this->tracer)
        this->tracer->Close();
      jaeger_initialized = false;
    }
  std::shared_ptr<opentracing::v3::Tracer> tracer = NULL;
};

//will be used throughout ceph to create spans
extern Jaeger_Tracer tracer;
extern std::atomic<bool> jaeger_initialized;

namespace jaeger_tracing{

  typedef std::unique_ptr<opentracing::Span> Span;

  static inline void init_tracer(const char* tracerName,const char* filePath){
        
        if(jaeger_initialized) return;

        try{
            auto yaml = YAML::LoadFile(filePath);
            auto configuration = jaegertracing::Config::parse(yaml);

            jaegertracing::net::Socket socket;
            socket.open(AF_INET, SOCK_STREAM);
            const std::string serverURL = configuration.sampler().samplingServerURL();
            socket.connect(serverURL); // this is used to check if the tracer is able to connect with server successfully

            tracer.tracer = jaegertracing::Tracer::make(
            tracerName,
            configuration,
            jaegertracing::logging::consoleLogger());
        }catch(...) {return;}
        opentracing::Tracer::InitGlobal(
        std::static_pointer_cast<opentracing::Tracer>(tracer.tracer));
        jaeger_initialized = true;
    }

  static inline Span new_span(const char* span_name){
        Span span=opentracing::Tracer::Global()->StartSpan(span_name);
        return span;
    }
  static inline Span child_span(const char* span_name, const Span& parent_span){
      if(parent_span){
          Span span = opentracing::Tracer::Global()->StartSpan(span_name, {opentracing::ChildOf(&parent_span->context())});
          return span;
      }
      return nullptr;
    }

  //method to finish tracing of a single Span
  static inline void finish_trace(Span& span)
  {
      if(span){
        Span s = std::move(span);
        s->Finish();
      }
  }

  //setting tags in spans
  static inline void set_span_tag(Span& span, const char* key, const char* value)
  {
    if(span)
      span->SetTag(key, value);
  }
}

#endif
