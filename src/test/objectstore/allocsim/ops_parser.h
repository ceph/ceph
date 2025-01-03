#ifndef OPS_PARSER_H
#define OPS_PARSER_H
#include <cstdint>
#include <vector>
#include <string>
#include <set>
#include "include/rados/librados.hpp"
#include <functional>


// compare shared_ptr<string>
struct StringPtrCompare
{
  int operator()(
    const std::shared_ptr<std::string>& lhs, 
    const std::shared_ptr<std::string>& rhs) const {
    if (lhs && rhs) {
        // Compare the content of the strings
        return *lhs < *rhs;
    }
    return lhs < rhs;
  }
};

enum op_type {
  Write,
  WriteFull,
  Read,
  Truncate,
  Zero
};

struct Op {
  uint64_t at;
  op_type type;
  uint64_t offset;
  uint64_t length;
  std::shared_ptr<std::string> object;
  std::shared_ptr<std::string> collection;
  std::shared_ptr<std::string> who;
  librados::AioCompletion *completion;
  bufferlist read_bl;

  Op(
    uint64_t at,
    op_type type,
    uint64_t offset,
    uint64_t length,
    std::shared_ptr<std::string> object,
    std::shared_ptr<std::string> collection,
    std::shared_ptr<std::string> who
  ) : at(at), type(type), offset(offset), length(length), object(object), collection(collection), who(who), completion(nullptr) {}

};

struct ParserContext {
    std::set<std::shared_ptr<std::string>, StringPtrCompare> collection_cache;
    std::set<std::shared_ptr<std::string>, StringPtrCompare> object_cache;
    std::set<std::shared_ptr<std::string>, StringPtrCompare> who_cache;
    std::vector<Op> ops;
    char *start; // starts and ends in new line or eof
    char *end;
    uint64_t max_buffer_size;
};

void parse_entry_point(
  std::shared_ptr<ParserContext> context,
  std::function<void(Op&&)> op_action
  );

void parse_files(
  const std::vector<std::string>& input_files,
  int nparser_threads,
  std::vector<Op>& ops,
  uint64_t& max_buffer_size);

#endif
