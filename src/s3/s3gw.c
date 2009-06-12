#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "fcgiapp.h"

#include "s3access.h"


struct req_state {
   FCGX_Stream *in;
   FCGX_Stream *out;
   int indent;
};

static void init_state(struct req_state *s, FCGX_Stream *in, FCGX_Stream *out)
{
  s->in = in;
  s->out = out;
  s->indent = 0;
}

static void buf_to_hex(const unsigned char *buf, int len, char *str)
{
  int i;
  str[0] = '\0';
  for (i = 0; i < len; i++) {
    sprintf(&str[i*2], "%02x", (int)buf[i]);
  }
}

static void open_section(struct req_state *s, const char *name)
{
  FCGX_FPrintF(s->out, "%*s<%s>\n", s->indent, "", name);
  ++s->indent;
}

static void close_section(struct req_state *s, const char *name)
{
  --s->indent;
  FCGX_FPrintF(s->out, "%*s</%s>\n", s->indent, "", name);
}

static void dump_value(struct req_state *s, const char *name, const char *val)
{
  FCGX_FPrintF(s->out, "%*s<%s>%s</%s>\n", s->indent, "", name, val, name);
}

static void dump_entry(struct req_state *s, const char *val)
{
  FCGX_FPrintF(s->out, "%*s<?%s?>\n", s->indent, "", val);
}



static void dump_owner(struct req_state *s, const char *id, int id_size, const char *name)
{
  char id_str[id_size*2 + 1];

  buf_to_hex((const unsigned char *)id, id_size, id_str);

  open_section(s, "Owner");
  dump_value(s, "ID", id_str);
  dump_value(s, "DisplayName", name);
  close_section(s, "Owner");
}

static void dump_start(struct req_state *s)
{
  dump_entry(s, "xml version=\"1.0\" encoding=\"UTF-8\"");
}

static void list_all_buckets_start(struct req_state *s)
{
  open_section(s, "ListAllMyBucketsResult");
}

static void list_all_buckets_end(struct req_state *s)
{
  close_section(s, "ListAllMyBucketsResult");
}


static void dump_bucket(struct req_state *s, const char *name, const char *date)
{
  open_section(s, "Bucket");
  dump_value(s, "Name", name);
  dump_value(s, "CreationDate", date);
  close_section(s, "Bucket");
}

void do_retrieve(struct req_state *s, const char *path_name)
{
  unsigned long long id = 0x0123456789ABCDEF;
  S3AccessHandle handle;
  int r;
#define BUF_SIZE 256
  char buf[BUF_SIZE];
  dump_start(s);
  list_all_buckets_start(s);
  dump_owner(s, (const char *)&id, sizeof(id), "foobi");
  r = list_buckets_init((const char *)&id, &handle);
  open_section(s, "Buckets");
  while (r >= 0) {
    r = list_buckets_next((const char *)&id, buf, BUF_SIZE, &handle);
    if (r < 0)
      continue;
    dump_bucket(s, buf, "123123");
  }
  close_section(s, "Buckets");
  list_all_buckets_end(s);
}

int main(void)
{
  int i, scale;
  FCGX_Stream *in, *out, *err;
  FCGX_ParamArray envp;
  struct req_state s;

  while (FCGX_Accept(&in, &out, &err, &envp) >= 0) 
  {
    char *method;
    char* path_name;

    FCGX_FPrintF(out,"Content-type: text/plain\r\n\r\n");      

    method = FCGX_GetParam("REQUEST_METHOD",envp);
    if (!method)
      continue;

    path_name = FCGX_GetParam("SCRIPT_NAME",envp);
    if (!path_name)  /* really shouldn't happen.. */
      continue;

    init_state(&s, in, out);

    if (strcmp(method, "GET") == 0)
      do_retrieve(&s, path_name);
  }
  return 0;
}
