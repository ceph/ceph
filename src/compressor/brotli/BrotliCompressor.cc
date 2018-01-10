#include "brotli/encode.h"
#include "brotli/decode.h"
#include "BrotliCompressor.h"
#include "include/scope_guard.h"

#define MAX_LEN (CEPH_PAGE_SIZE)

int BrotliCompressor::compress(const bufferlist &in, bufferlist &out) 
{
  BrotliEncoderState* s = BrotliEncoderCreateInstance(nullptr,
                                                      nullptr,
                                                      nullptr);
  if (!s) {
    return -1;
  }
  auto sg = make_scope_guard([&s] { BrotliEncoderDestroyInstance(s); });
  BrotliEncoderSetParameter(s, BROTLI_PARAM_QUALITY, (uint32_t)9);
  BrotliEncoderSetParameter(s, BROTLI_PARAM_LGWIN, 22);
  for (auto i = in.buffers().begin(); i != in.buffers().end();) {
    size_t available_in = i->length();
    size_t max_comp_size = BrotliEncoderMaxCompressedSize(available_in);
    size_t available_out =  max_comp_size;
    bufferptr ptr = buffer::create_page_aligned(max_comp_size);
    uint8_t* next_out = (uint8_t*)ptr.c_str();
    const uint8_t* next_in = (uint8_t*)i->c_str();
    ++i;
    BrotliEncoderOperation finish = i != in.buffers().end() ?
                                         BROTLI_OPERATION_PROCESS :
                                         BROTLI_OPERATION_FINISH;
    do {
      if (!BrotliEncoderCompressStream(s,
                                       finish,
                                       &available_in,
                                       &next_in,
                                       &available_out,
                                       &next_out,
                                       nullptr)) {
        return -1;
      }
      unsigned have = max_comp_size - available_out;
      out.append(ptr, 0, have);
    } while (available_out == 0);
    if (BrotliEncoderIsFinished(s)) {
      break;
    }
  }
  return 0;
}

int BrotliCompressor::decompress(bufferlist::iterator &p,
                                 size_t compressed_size,
                                 bufferlist &out) 
{
  BrotliDecoderState* s = BrotliDecoderCreateInstance(nullptr,
                                                      nullptr,
                                                      nullptr);
  if (!s) {
    return -1;
  }
  auto sg = make_scope_guard([&s] { BrotliDecoderDestroyInstance(s); });
  size_t remaining = std::min<size_t>(p.get_remaining(), compressed_size);
  while (remaining) {
    const uint8_t* next_in;
    size_t len = p.get_ptr_and_advance(remaining, (const char**)&next_in);
    remaining -= len;
    size_t available_in = len;
    BrotliDecoderResult result = BROTLI_DECODER_RESULT_ERROR;
    do {
      size_t available_out = MAX_LEN;
      bufferptr ptr = buffer::create_page_aligned(MAX_LEN);
      uint8_t* next_out = (uint8_t*)ptr.c_str();
      result = BrotliDecoderDecompressStream(s,
                                             &available_in,
                                             &next_in,
                                             &available_out,
                                             &next_out,
                                             0);
      if (!result) {
        return -1;
      }
      unsigned have = MAX_LEN - available_out;
      out.append(ptr, 0, have);
    } while (result == BROTLI_DECODER_RESULT_NEEDS_MORE_OUTPUT);
    if (BrotliDecoderIsFinished(s)) {
      break;
    }
  }
  return 0;
}

int BrotliCompressor::decompress(const bufferlist &in, bufferlist &out) 
{  
  bufferlist::iterator i = const_cast<bufferlist&>(in).begin();
  return decompress(i, in.length(), out);
}
