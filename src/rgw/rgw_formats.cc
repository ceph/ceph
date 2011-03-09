#include "rgw_common.h"
#include "rgw_formats.h"

/* Plain */

void RGWFormatter_Plain::open_obj_section(const char *name)
{
}

void RGWFormatter_Plain::open_array_section(const char *name)
{
}

void RGWFormatter_Plain::close_section(const char *name)
{
}

void RGWFormatter_Plain::dump_value_int(const char *name, const char *fmt, ...)
{
#define LARGE_SIZE 8192
  char buf[LARGE_SIZE];
  va_list ap;

  va_start(ap, fmt);
  int n = vsnprintf(buf, LARGE_SIZE, fmt, ap);
  va_end(ap);
  if (n >= LARGE_SIZE)
    return;
  CGI_PRINTF(s, "%s\n", buf);
}

void RGWFormatter_Plain::dump_value_str(const char *name, const char *fmt, ...)
{
  char buf[LARGE_SIZE];
  va_list ap;

  va_start(ap, fmt);
  int n = vsnprintf(buf, LARGE_SIZE, fmt, ap);
  va_end(ap);
  if (n >= LARGE_SIZE)
    return;
  CGI_PRINTF(s, "%s\n", buf);
}

/* XML */

void RGWFormatter_XML::formatter_init()
{
  indent = 0;
}

void RGWFormatter_XML::open_section(const char *name)
{
  CGI_PRINTF(s, "<%s>", name);
  ++indent;
}

void RGWFormatter_XML::close_section(const char *name)
{
  --indent;
  CGI_PRINTF(s, "</%s>", name);
}

void RGWFormatter_XML::dump_value_int(const char *name, const char *fmt, ...)
{
#define LARGE_SIZE 8192
  char buf[LARGE_SIZE];
  va_list ap;

  va_start(ap, fmt);
  int n = vsnprintf(buf, LARGE_SIZE, fmt, ap);
  va_end(ap);
  if (n >= LARGE_SIZE)
    return;
  CGI_PRINTF(s, "<%s>%s</%s>", name, buf, name);
}

void RGWFormatter_XML::dump_value_str(const char *name, const char *fmt, ...)
{
  char buf[LARGE_SIZE];
  va_list ap;

  va_start(ap, fmt);
  int n = vsnprintf(buf, LARGE_SIZE, fmt, ap);
  va_end(ap);
  if (n >= LARGE_SIZE)
    return;
  CGI_PRINTF(s, "<%s>%s</%s>", name, buf, name);
}

/* JSON */

void RGWFormatter_JSON::formatter_init()
{
  stack.clear();
}

void RGWFormatter_JSON::open_section(bool is_array)
{
  if (stack.size()) {
    struct json_stack_entry& entry = stack.back();
    CGI_PRINTF(s, "%s\n", (entry.size ? "," : ""));
    entry.size++;
  }
  CGI_PRINTF(s, "%c", (is_array ? '[' : '{'));

  struct json_stack_entry new_entry;
  new_entry.is_array = is_array;
  new_entry.size = 0;
  stack.push_back(new_entry);
}

void RGWFormatter_JSON::open_obj_section(const char *name)
{
  open_section(false);
}

void RGWFormatter_JSON::open_array_section(const char *name)
{
  open_section(true);
}

void RGWFormatter_JSON::close_section(const char *name)
{
  struct json_stack_entry& entry = stack.back();

  CGI_PRINTF(s, "%c", (entry.is_array ? ']' : '}'));

  stack.pop_back();
}

void RGWFormatter_JSON::dump_value_int(const char *name, const char *fmt, ...)
{
#define LARGE_SIZE 8192
  char buf[LARGE_SIZE];
  va_list ap;

  struct json_stack_entry& entry = stack.back();

  va_start(ap, fmt);
  int n = vsnprintf(buf, LARGE_SIZE, fmt, ap);
  va_end(ap);
  if (n >= LARGE_SIZE)
    return;
  CGI_PRINTF(s, "%s\"%s\":%s", (entry.size ? ", " : ""), name, buf);
  entry.size++;
}

void RGWFormatter_JSON::dump_value_str(const char *name, const char *fmt, ...)
{
  char buf[LARGE_SIZE];
  va_list ap;

  struct json_stack_entry& entry = stack.back();

  va_start(ap, fmt);
  int n = vsnprintf(buf, LARGE_SIZE, fmt, ap);
  va_end(ap);
  if (n >= LARGE_SIZE)
    return;
  CGI_PRINTF(s, "%s\"%s\":\"%s\"", (entry.size ? ", " : ""), name, buf);
  entry.size++;
}

