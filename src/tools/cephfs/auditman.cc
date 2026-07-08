#include <ctime>
#include <exception>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/program_options.hpp>
#include <fmt/format.h>

#include "common/AuditDB.h"
#include "common/JSONFormatter.h"
#include "common/TextTable.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"

using std::cerr;
using std::cout;
using std::endl;

namespace po = boost::program_options;

const std::unordered_map<std::string, std::string> TOOL_TO_TABLE_NAME = {
    {"cephfs-data-scan", "cephfs_data_scan"},
    {"cephfs-meta-injection", "cephfs_meta_injection"},
    {"cephfs-table-tool", "cephfs_table_tool"},
    {"cephfs-journal-tool", "cephfs_journal_tool"}
};

const std::unordered_set<std::string> VALID_FIELDS = {
    "seq",
    "cmd",
    "cmd_args",
    "init_time",
    "comp_time",
    "status",
    "retval"
};

const std::vector<std::string> DEFAULT_FIELDS = {
    "seq",
    "cmd",
    "cmd_args",
    "init_time",
    "comp_time",
    "status",
    "retval"
};

const std::vector<std::string> BRIEF_FIELDS = {
    "seq",
    "cmd"
};

const std::unordered_set<std::string> VALID_FORMATS = {
    "plain",
    "json",
    "json-pretty"
};

// const std::unordered_set<std::string> VALID_STATUSES = {

// };

// Configuration for the audit query attempt
struct AuditCliConfig {
  AuditQuery query;
  std::string table_name;
  bool count_mode = false;
  bool brief = false;
  std::string format = "plain";
  std::vector<std::string> selected_fields = DEFAULT_FIELDS;
};

// Helper to process cli options
template <typename T, typename Func>
void process_command(
    const po::variables_map& vm,
    const std::string& cmd_name,
    Func handler) {
  if (vm.count(cmd_name)) {
    handler(vm[cmd_name].as<T>());
  }
}

// Helper for converting string "YYYY-MM-DD" or ""YYYY-MM-DD HH:MM:SS" to time_t
time_t str_to_time_t(const std::string& str_time) {
  const std::string format_error =
      "Invalid date format. Expected \"YYYY-MM-DD\" or \"YYYY-MM-DD HH:MM:SS\"";
  const std::string date_error =
      "Invalid date or time entered. Please ensure the date is valid.";
  const bool date_only = (str_time.size() == 10);
  const bool date_and_time = (str_time.size() == 19);

  if (!date_only && !date_and_time) {
    throw std::invalid_argument(format_error);
  }

  // date error check
  if (str_time[4] != '-' || str_time[7] != '-') {
    throw std::invalid_argument(format_error);
  }

  // time error check
  if (date_and_time &&
      (str_time[10] != ' ' || str_time[13] != ':' || str_time[16] != ':')) {
    throw std::invalid_argument(format_error);
  }

  std::tm tm = {};
  std::istringstream ss(str_time);
  if (date_only) {
    ss >> std::get_time(&tm, "%Y-%m-%d");
  } else if (date_and_time) {
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
  }

  if (ss.fail()) {
    throw std::invalid_argument(date_error);
  }

  tm.tm_isdst = -1;
  time_t t = std::mktime(&tm);
  if (t == static_cast<time_t>(-1)) {
    throw std::invalid_argument("Invalid calendar date entered.");
  }

  return t;
}

// Helper for converting input range to time_t pair
std::pair<time_t, time_t> range_str_to_time(const std::string& range) {
  const std::string error_msg =
      "Invalid range format. Expected \"YYYY-MM-DD,YYYY-MM-DD\" "
      "OR \"YYYY-MM-DD HH:MM:SS,YYYY-MM-DD HH:MM:SS\"";
  const bool dates_only = (range.size() == 21);
  const bool dates_and_times = (range.size() == 39);

  if (!dates_only && !dates_and_times) {
    throw std::invalid_argument(error_msg);
  }

  const size_t comma = range.find(',');
  if (comma == std::string::npos || (dates_only && comma != 10) ||
      (dates_and_times && comma != 19)) {
    throw std::invalid_argument(error_msg);
  }
  std::string range_start = range.substr(0, comma);
  std::string range_end = range.substr(comma + 1);

  time_t start_time = str_to_time_t(range_start);
  time_t end_time = str_to_time_t(range_end);
  if (start_time > end_time) {
    throw std::invalid_argument(
        "Invalid range. Range start must be before or equal to range end.");
  }
  return {start_time, end_time};
}

// Helper for formatting time during output
std::string format_time(time_t value) {
  std::tm tm{};
  if (localtime_r(&value, &tm) == nullptr) {
    return "";
  }
  std::ostringstream out;
  out << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
  return out.str();
}

// Helper for printing formatted result of a count() call to AuditDB
void print_count_result_formatted(const std::string& format, int64_t count) {
  if (format == "plain") {
    TextTable table;
    table.define_column("count", TextTable::LEFT, TextTable::LEFT);
    table << count << TextTable::endrow;
    cout << table;
    return;
  }

  if (format == "json" || format == "json-pretty") {
    JSONFormatter formatter((format == "json-pretty"));
    formatter.open_object_section("");
    formatter.dump_int("count", count);
    formatter.close_section();
    formatter.flush(cout);
    cout << endl;
  }
}

// Helper for printing formatted result of a query() call to AuditDB
void print_query_result_formatted(
    const std::string& format,
    const std::vector<std::string>& selected_fields,
    const std::vector<AuditEntry>& entries) {
  if (format == "plain") {
    TextTable table;
    for (const auto& f : selected_fields) {
      table.define_column(f, TextTable::LEFT, TextTable::LEFT);
    }
    for (const auto& e : entries) {
      for (const auto& f : selected_fields) {
        if (f == "seq") {
          table << e.seq;
        } else if (f == "cmd") {
          table << e.cmd;
        } else if (f == "cmd_args") {
          table << e.cmd_args;
        } else if (f == "init_time") {
          table << format_time(e.init_time);
        } else if (f == "comp_time") {
          table << (e.comp_time ? format_time(*e.comp_time) : "");
        } else if (f == "status") {
          table << e.status.value_or("");
        } else if (f == "retval") {
          table << (e.retval ? std::to_string(*e.retval) : "");
        }
      }
      table << TextTable::endrow;
    }
    cout << table;
    return;
  }

  if (format == "json" || format == "json-pretty") {
    JSONFormatter jf((format == "json-pretty"));
    jf.open_array_section("audit_entries");
    for (const auto& e : entries) {
      jf.open_object_section("entry");
      for (const auto& f : selected_fields) {
        if (f == "seq") {
          jf.dump_unsigned("seq", e.seq);
        } else if (f == "cmd") {
          jf.dump_string("cmd", e.cmd);
        } else if (f == "cmd_args") {
          jf.dump_string("cmd_args", e.cmd_args);
        } else if (f == "init_time") {
          jf.dump_string("init_time", format_time(e.init_time));
        } else if (f == "comp_time") {
          e.comp_time ? jf.dump_string("comp_time", format_time(*e.comp_time))
                      : jf.dump_null("comp_time");
        } else if (f == "status") {
          e.status ? jf.dump_string("status", *e.status)
                   : jf.dump_null("status");
        } else if (f == "retval") {
          e.retval ? jf.dump_int("retval", *e.retval) : jf.dump_null("retval");
        }
      }
      jf.close_section();
    }
    jf.close_section();
    jf.flush(cout);
    cout << endl;
  }
}

// Helper to error check and build AuditDB query with all cli options specified
void apply_cli_opts(const po::variables_map& vm, AuditCliConfig& config) {
  if (vm.count("range") &&
      (vm.count("since") || vm.count("until") || vm.count("last"))) {
    throw std::invalid_argument(
        "Cannot combine --range with --since, --until, or --last.");
  }

  if (vm.count("last") &&
      (vm.count("since") || vm.count("until") || vm.count("range"))) {
    throw std::invalid_argument(
        "Cannot combine --last with --since, --until, or --range.");
  }

  if (vm.count("limit") && vm.count("recent")) {
    throw std::invalid_argument("Cannot use both --limit and --recent.");
  }

  process_command<std::string>(
      vm, "tool-name", [&config](const std::string& value) {
        if (!TOOL_TO_TABLE_NAME.contains(value)) {
          std::string accepted_tools_str;
          for (const auto& [tool_name, _] : TOOL_TO_TABLE_NAME) {
            if (!accepted_tools_str.empty()) {
              accepted_tools_str += ", ";
            }
            accepted_tools_str += tool_name;
          }
          throw std::invalid_argument(
              "Tool channel not found. Valid channels: " + accepted_tools_str);
        }
        config.table_name = TOOL_TO_TABLE_NAME.at(value);
      });

  process_command<int64_t>(vm, "limit", [&config](const int64_t value) {
    if (value < 0) {
      throw std::invalid_argument("--limit must be >= 0.");
    }
    config.query.limit = value;
  });

  process_command<int64_t>(vm, "before-seq", [&config](const int64_t value) {
    if (value < 0) {
      throw std::invalid_argument("--before-seq must be >= 0.");
    }
    config.query.before_seq = value;
  });

  process_command<int64_t>(vm, "after-seq", [&config](const int64_t value) {
    if (value < 0) {
      throw std::invalid_argument("--after-seq must be >= 0.");
    }
    config.query.after_seq = value;
  });

  if (vm.count("before-seq") && vm.count("after-seq") &&
      (config.query.before_seq && config.query.after_seq &&
       config.query.before_seq <= config.query.after_seq)) {
    throw std::invalid_argument(
        "--before-seq must be greater than --after-seq.");
  }

  process_command<std::string>(vm, "since", [&config](const std::string& value) {
    config.query.since = str_to_time_t(value);
  });

  process_command<std::string>(vm, "until", [&config](const std::string& value) {
    config.query.until = str_to_time_t(value);
  });

  if (vm.count("since") && vm.count("until") &&
      (config.query.since > config.query.until)) {
    throw std::invalid_argument("--since must be <= --until");
  }

  process_command<std::string>(vm, "range", [&config](const std::string& value) {
    std::pair<time_t, time_t> r = range_str_to_time(value);
    config.query.since = r.first;
    config.query.until = r.second;
  });

  process_command<double>(vm, "last", [&config](const double& value) {
    if (value < 0.0) {
      throw std::invalid_argument("--last must be a nonnegative number.");
    }
    time_t now = std::time(nullptr);
    time_t timestamp_ago = now - static_cast<time_t>(value * 3600);
    config.query.since = timestamp_ago;
  });

  process_command<int64_t>(vm, "recent", [&config](const int64_t value) {
    if (value < 0) {
      throw std::invalid_argument("--recent must be a nonnegative number.");
    }
    config.query.limit = value;
  });

  process_command<std::string>(vm, "status", [&config](const std::string& value) {
    // TO DO: add error check for valid status values?
    config.query.status = value;
  });

  process_command<std::string>(
      vm, "order-by", [&config](const std::string& value) {
        if (value != "seq" && value != "init_time" && value != "comp_time" &&
            value != "retval") {
          throw std::invalid_argument(
              "--order-by must be in: seq, init_time, comp_time, retval.");
        }
        config.query.order_by = value;
      });

  process_command<std::string>(vm, "order", [&config](const std::string& value) {
    if (value != "ASC" && value != "DESC") {
      throw std::invalid_argument("--order must be ASC or DESC.");
    }
    config.query.ascending = (value == "ASC");
  });

  process_command<std::string>(vm, "fields", [&config](const std::string& value) {
    config.selected_fields.clear();
    std::istringstream ss(value);
    std::string field;
    while (std::getline(ss, field, ',')) {
      if (!VALID_FIELDS.contains(field)) {
        throw std::invalid_argument("Invalid field: " + field + ". Ensure all fields are valid and comma-separated with no spaces.");
      }
      config.selected_fields.push_back(field);
    }
  });

  if (config.count_mode && vm.count("fields")) {
    throw std::invalid_argument("Cannot combine --count with --fields.");
  }

  if (config.brief && vm.count("fields")) {
    throw std::invalid_argument("Cannot combine --brief with --fields.");
  }

  if (config.brief && config.count_mode) {
    throw std::invalid_argument("Cannot combine --brief with --count");
  }

  if (config.brief) {
    config.selected_fields = BRIEF_FIELDS;
  }

  if (!VALID_FORMATS.contains(config.format)) {
    throw std::invalid_argument(
        "--format must be in: plain (default), json, json-pretty.");
  }
}

// Helper to query AuditDB with calls to count() or query() 
// and print formatted output
void do_audit_query(AuditDB& db, const AuditCliConfig& config) {
  if (config.count_mode) {
    auto count_res = db.count(config.query);
    if (!count_res) {
      const auto& error = count_res.error();
      throw std::runtime_error(
          fmt::format(
              "Failed to count audit entries. Code: {}, Detail: {}",
              static_cast<int>(error.code), error.detail));
    }
    print_count_result_formatted(config.format, count_res.value());
    return;
  }

  const auto query_res = db.query(config.query);
  if (!query_res) {
    const auto& error = query_res.error();
    throw std::runtime_error(
        fmt::format(
            "Failed to query AuditDB. Code: {}, Detail: {}",
            static_cast<int>(error.code), error.detail));
  }

  const std::vector<AuditEntry>& entries = query_res.value();
  print_query_result_formatted(config.format, config.selected_fields, entries);
}

int main(int argc, const char** argv) {
  // Construct ceph context
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(
      NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct.get());
  
  // General options
  po::options_description general("General Options");
  general.add_options()
    ("help,h",    "Produce help message")
    ("conf,c",    po::value<std::string>(), "Ceph config file path")
    ("id",        po::value<std::string>(), "Client ID (default: admin)")
    ("keyring,k", po::value<std::string>(), "Path to keyring file")
  ;

  // Query specific options
  AuditCliConfig config;
  po::options_description query_opts("Query Options");
  query_opts.add_options()
    ("tool-name", po::value<std::string>()->required(), "Tool name (channel you want to query).")
    ("limit", po::value<int64_t>(), "Maximum number of entries to return.")
    ("before-seq", po::value<int64_t>(), "Get entries before seq number n.")
    ("after-seq", po::value<int64_t>(), "Get entries after seq number n.")
    ("since", po::value<std::string>(), "Get entries since timestamp \"YYYY-MM-DD\" or \"YYYY-MM-DD HH:MM:SS\" .")
    ("until", po::value<std::string>(), "Get entries until timestamp \"YYYY-MM-DD\" or \"YYYY-MM-DD HH:MM:SS\" .")
    ("range", po::value<std::string>(), "Get entries between timestamp range \"YYYY-MM-DD\",\"YYYY-MM-DD\"")
    ("last", po::value<double>(), "Get entries from the last N hours (supports decimals, e.g. 0.5 for last 30 mins).")
    ("recent", po::value<int64_t>(), "Get last n recent entries (in DESC order by default).")
    ("status", po::value<std::string>(), "Get entries with status s.") // TO DO: Ensure these are just success, failure, timeout or completed, unkown error, aborted
    ("order-by", po::value<std::string>(), "Order returned entries by a specific field")        
    ("order", po::value<std::string>()->default_value("DESC"), "Specify ASC or DESC ordering (default order is DESC)")
    ("count", po::bool_switch(&config.count_mode), "Get count of entries that match filters.")
    ("fields", po::value<std::string>(), "Specify one or more fields to retrieve.")
    ("brief", po::bool_switch(&config.brief), "Return seq and cmd fields only.")
    ("format", po::value<std::string>(&config.format)->default_value("plain"), 
        "Specify output format: plain (default), json, json-pretty.")
  ;

  // Visible options for help output
  po::options_description visible("Allowed options");
  visible.add(general).add(query_opts);
  
  // Process cli options and build AuditCliConfig (Audit DB query)
  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, visible), vm);
    if (vm.count("help")) {
      cout << "Usage: audit-cli --tool-name <tool> [query-options]\n\n";
      cout << "Query audit records stored in AuditDB for a specific tool "
              "(channel).\n\n";

      cout << "Supported tools:\n";
      for (const auto& [tool_name, table_name] : TOOL_TO_TABLE_NAME) {
        cout << "  " << tool_name << "\n";
      }
      cout << "\n";
      cout << visible << std::endl;
      return 0;
    }
    po::notify(vm);
    apply_cli_opts(vm, config);
  } catch (const std::invalid_argument& e) {
    cerr << "Error: " << e.what() << endl;
    return 1;
  } catch (const std::exception& e) {
    cerr << "Error: " << e.what() << endl;
    cout << "Usage: audit-cli --tool-name <tool> [query-options]\n";
    cout << "Query audit records stored in AuditDB for a specific tool "
            "(channel).\n";
    cout << "Try --help for more information.\n";
    return 1;
  }

  // Init aduitdb
  std::string db_uri =
      fmt::format("file:///.audit:/{}_audit.db?vfs=ceph", config.table_name);
  AuditDB db(cct.get(), db_uri.c_str(), true, config.table_name, true);
  if (auto init_res = db.init(); !init_res) {
    cerr << "Failed to initialize db" << endl;
    return 1;
  }

  // Query auditdb
  try {
    do_audit_query(db, config);
  } catch (const std::exception& e) {
    cerr << e.what() << endl;
    return 1;
  }

  return 0;
}

// TO DO: Integrate status error checking
// TO DO: command arguments (filter command that were run with --yes-i-really-really-mean-it flag)