#include "tracer.h"

namespace jaeger_tracing
{
    static std::shared_ptr<opentracing::v3::Tracer> tracer = nullptr;

    constexpr auto jaeger_config = R"cfg(
            disabled: false
            sampler:
                type: const
                param: 1
            reporter:
                queueSize: 100
                bufferFlushInterval: 10
                logSpans: false
                localAgentHostPort: 127.0.0.1:6831
            headers:
                jaegerDebugHeader: debug-id
                jaegerBaggageHeader: baggage
                TraceContextHeaderName: trace-id
                traceBaggageHeaderPrefix: "testctx-"
            baggage_restrictions:
                denyBaggageOnInitializationFailure: false
                hostPort: 127.0.0.1:5778
                refreshInterval: 60
            )cfg";

    void init_tracer(const char* tracerName){
        auto configuration = jaegertracing::Config::parse(YAML::Load(jaeger_config));
        tracer = jaegertracing::Tracer::make(tracerName, configuration, jaegertracing::logging::consoleLogger());
        opentracing::Tracer::InitGlobal(std::static_pointer_cast<opentracing::Tracer>(tracer));
    }

    std::unique_ptr<jspan> new_span(const char* span_name){
        return opentracing::Tracer::Global()->StartSpan(span_name);
    }

    std::unique_ptr<jspan> child_span(const char* span_name, const jspan* const parent_span){
      if(parent_span){
          return opentracing::Tracer::Global()->StartSpan(span_name, {opentracing::ChildOf(&parent_span->context())});
      }
      return nullptr;
    }

    void finish_span(jspan* span){
        if(span){
            span->Finish();
        }
    }

    void set_span_tag(jspan* span, const char* key, const char* value){
        if(span)
        span->SetTag(key, value);
    }
}
