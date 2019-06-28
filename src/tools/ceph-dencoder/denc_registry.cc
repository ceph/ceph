#include "denc_registry.h"

void DencoderRegistry::add(const char* name, std::unique_ptr<Dencoder>&& denc)
{
  dencoders.emplace(name, std::move(denc));
};

DencoderRegistry& DencoderRegistry::instance()
{
  static DencoderRegistry registry;
  return registry;
}
