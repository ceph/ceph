#include <windows.h>
#include <syslog.h>
#include "event_logging.h"
#include "common/code_environment.h"

static HANDLE g_event_source = NULL;

bool get_event_source()
{
  if (!g_event_source) {
    HANDLE temp = RegisterEventSourceA(NULL, get_process_name_cpp().c_str());
    if (!temp)
      return false;

    if (InterlockedCompareExchangePointer(&g_event_source, temp, NULL)) {
      // There already was an event source, let's cleanup the one that we've
      // just created.
      DeregisterEventSource(temp);
    }
  }

  return true;
}

void write_event_log_entry(int level, const char* msg)
{
  if (!get_event_source()) {
    return;
  }

  WORD type;
  DWORD event_id;
  switch (level) {
    case LOG_DEBUG:
      event_id = SUCCESS_EVENTMSG;
      type = EVENTLOG_SUCCESS;
      break;

    case LOG_INFO:
    case LOG_NOTICE:
      event_id = INFO_EVENTMSG;
      type = EVENTLOG_INFORMATION_TYPE;
      break;

    case LOG_WARNING:
      event_id = WARN_EVENTMSG;
      type = EVENTLOG_WARNING_TYPE;
      break;

    default:
      event_id = ERROR_EVENTMSG;
      type = EVENTLOG_ERROR_TYPE;
  }

  ReportEventA(g_event_source, type,
	       0, event_id, NULL, 1, 0, &msg, NULL);
}

void syslog(int priority, const char* format, ...)
{
  va_list args;
  va_start(args, format);

  size_t length = (size_t)_vscprintf(format, args) + 1;

  char* buffer = (char*) malloc(length);
  if (NULL == buffer) {
    va_end(args);
    return;
  }

  vsnprintf_s(buffer, length, length - 1, format, args);
  va_end(args);

  write_event_log_entry(LOG_PRI(priority), buffer);
  free(buffer);
}
