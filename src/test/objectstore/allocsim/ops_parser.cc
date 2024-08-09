#include "ops_parser.h"
#include <cassert>
#include <fmt/format.h>
#include <iostream>
#include <thread>
#include <map>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;

int64_t timestamp_parser2(const char* t) {
  // expected format
  // 2024-05-10 12:06:24.792232+00:00
  // 0123456789012345678------------
  static constexpr uint32_t time_len = sizeof("2024-05-10 12:06:24");
  thread_local char previous_str[time_len]("0000-00-00 00:00:00");
  thread_local uint64_t previous_time = 0;
  int usec = atoi(t + 20);
  if (usec < 0 || usec > 999999) return 0;
  if (strncmp(t, previous_str, time_len) == 0) {
    return previous_time * 1000000 + usec;
  }
  struct tm a_tm;
  a_tm.tm_zone = 0;
  a_tm.tm_isdst = 0;
  int x;
  x = atoi(t);
  if (x < 2024 || x > 2100) return 0;
  a_tm.tm_year = x;
  x = atoi(t + 5);
  if (x < 1 || x > 12) return 0;
  a_tm.tm_mon = x - 1;
  x = atoi(t + 8);
  if (x < 1 || x > 31) return 0;
  a_tm.tm_mday = x;
  x = atoi(t + 11);
  if (x < 0 || x > 23) return 0;
  a_tm.tm_hour = x;
  x = atoi(t + 14);
  if (x < 0 || x > 59) return 0;
  a_tm.tm_min = x;
  x = atoi(t + 17);
  if (x < 0 || x > 60) return 0;
  a_tm.tm_sec = x;
  time_t timep = mktime(&a_tm);
  previous_time = timep;
  memcpy(previous_str, t, time_len);
  return previous_time * 1000000 + usec;
}



void parse_entry_point(
  std::shared_ptr<ParserContext> context,
  std::function<void(Op&&)> op_action)
{
  //std::cout << fmt::format("Starting parser thread start={:p} end={:p}", context->start, context->end) << endl;
  assert(context->end[-1] == '\n');
  // we expect this input:
  // 2024-05-10 12:06:24.990831+00:00 client.607247697.0:5632274 write 4096~4096 2:d03a455a:::08b0f2fd5f20f504e76c2dd3d24683a1:head 2.1c0b
  char* pos = context->start;
  char* end = context->end;
  while (pos != end) {
    char* line_end = pos;
    auto go_after_space = [&](char* p) -> char* {
      while ((p != line_end) && (*(p++) != ' ')) {}
      return p;
    };
    while (*line_end != '\n')
      line_end++;
    char* date = pos;
    char* time = go_after_space(date);
    char* who = go_after_space(time);
    char* type = go_after_space(who);
    char* range = go_after_space(type);
    char* object = go_after_space(range);
    char* collection = go_after_space(object);
    pos = line_end + 1;
    if (collection >= line_end) 
      continue;
    if (false) {
      cout << 
      string(date, time-date-1) << "/" <<
      string(time, who-time-1) << "/" <<
      string(who, type-who-1) << "/" <<
      string(type, range-type-1) << "/" <<
      string(range, object-range-1) << "/" <<
      string(object, collection-object-1) << "/" <<
      string(collection, line_end-collection-1) << "/" <<
      endl;
    }
    if (who - date != sizeof("2024-05-10 12:06:24.990831+00:00"))
      continue;
    uint64_t at = timestamp_parser2(date);
    if (at == 0)
      continue;
    char* who_end = (char*)memchr(who, '.', type-who-1);
    if (who_end == nullptr)
      continue;
    who_end = (char*)memchr(who_end + 1, '.', type-who_end-1);
    if (who_end == nullptr)
      continue;
    shared_ptr<string> who_ptr = make_shared<string>(who, who_end-who);
    auto who_it = context->who_cache.find(who_ptr);
    if (who_it == context->who_cache.end()) {
      context->who_cache.insert(who_ptr);
    } else {
      who_ptr = *who_it;
    }
    shared_ptr<string> object_ptr = make_shared<string>(object, collection-object-1);
    auto object_it = context->object_cache.find(object_ptr);
    if (object_it == context->object_cache.end()) {
      context->object_cache.insert(object_ptr);
    } else {
      object_ptr = *object_it;
    }
    op_type ot;
    switch (type[0]) {
      case 'r': {
        ot = Read;
        break;
      }
      case 's': {
        ot = Read;
        break;
      }
      case 'z': {
        ot = Zero;
        break;
      }
      case 't': {
        ot = Truncate;
        break;
      }
      case 'w': {
        if (range-type-1 > 6) {
          ot = WriteFull;
        } else {
          ot = Write;
        }
        break;
      }
      default: {
        cout << "invalid type " << std::string(type, range-type-1) << endl;
        exit(1);
      }
    }
    shared_ptr<string> collection_ptr = make_shared<string>(collection, line_end-collection-1);
    auto collection_it = context->collection_cache.find(collection_ptr);
    if (collection_it == context->collection_cache.end()) {
      context->collection_cache.insert(collection_ptr);
    } else {
      collection_ptr = *collection_it;
    }
    uint64_t offset = 0, length = 0;
    char* endp;
    offset = strtol(range,&endp,10);

    if (ot != Truncate) {
        // Truncate only has one number
        if (*endp != '~')
          continue;
        length = atoi(endp+1);
    }

    context->max_buffer_size = max(length, context->max_buffer_size);
    
    op_action(Op(at, ot, offset, length, object_ptr, collection_ptr, who_ptr));
    //context->ops.push_back(Op(at, ot, offset, length, object_ptr, collection_ptr, who_ptr));
  }
}



void parse_files(
  const vector<string>& input_files,
  int nparser_threads,
  vector<Op>& ops,
  uint64_t& max_buffer_size)
{
  size_t sum_ops = 0;
  for (auto &file : input_files) {
    // Parse input file
    vector<std::thread> parser_threads;
    vector<shared_ptr<ParserContext>> parser_contexts;
    cout << fmt::format("parsing file {}", file) << endl;
    int fd = open(file.c_str(), O_RDONLY);
    if (fd == -1) {
        cout << "Error opening file" << endl;
        exit(EXIT_FAILURE);
    }
    struct stat file_stat;
    fstat(fd, &file_stat);
    char* mapped_buffer = (char*)mmap(NULL, file_stat.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (mapped_buffer == nullptr) {
        cout << "error mapping buffer" << endl;
        exit(EXIT_FAILURE);
    }
    uint64_t step_size = file_stat.st_size / nparser_threads;
    char* start = mapped_buffer;
    for (int i = 0; i < nparser_threads; i++) {
      char* end = start + step_size;
      if (i == nparser_threads - 1) {
          end = mapped_buffer + file_stat.st_size;
      }
      while(*(end - 1) != '\n') {
          end--;
      }
      shared_ptr<ParserContext> context = make_shared<ParserContext>();
      context->start = start;
      context->end = end;
      context->max_buffer_size = 0;
      parser_contexts.push_back(context);
      auto op_action = [context](Op&& op) {
        context->ops.push_back(op);
      };
      parser_threads.push_back(std::thread(parse_entry_point, context, op_action));
      start = end;
    }
    for (auto& t : parser_threads) {
        t.join();
    }
    // reduce
    for (auto context : parser_contexts) {
      sum_ops += context->ops.size();
    }
    ops.reserve(sum_ops);
    for (auto context : parser_contexts) {
        ops.insert(ops.end(), context->ops.begin(), context->ops.end());
        max_buffer_size = max(context->max_buffer_size, max_buffer_size);
    }
    munmap(mapped_buffer, file_stat.st_size);
  }
}
