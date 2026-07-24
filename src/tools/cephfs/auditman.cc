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

#include <boost/program_options.hpp>
#include <fmt/format.h>

#include "common/AuditDB.h"
#include "common/JSONFormatter.h"
#include "common/TextTable.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "json_spirit/json_spirit.h"

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

const std::unordered_set<std::string> VALID_FORMATS = {
    "plain",
    "json",
    "json-pretty"
};

// Configuration for query attempt
struct AuditCliConfig {
  AuditQuery query;
  std::string table_name;
  bool count_mode = false;
  std::string format = "plain";
};

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
    const std::vector<AuditEntry>& entries) {
  if (format == "plain") {
    TextTable table;
    table.define_column("seq", TextTable::LEFT, TextTable::LEFT);
    table.define_column("init_time", TextTable::LEFT, TextTable::LEFT);
    table.define_column("data", TextTable::LEFT, TextTable::LEFT);
    
    for (const auto& e : entries) {
      table << e.seq
            << format_time(e.init_time)
            << e.json_dump
            << TextTable::endrow;
    }
    cout << table;
    return;
  }

  if (format == "json" || format == "json-pretty") {
    json_spirit::Array output;

    for (const auto& e : entries) {
      json_spirit::Value data;
      if (!json_spirit::read(e.json_dump, data)) {
        throw std::runtime_error(
            fmt::format(
                "Invalid json_dump in audit entry seq={}",
                e.seq));
      }

      json_spirit::Object entry;
      entry.push_back(json_spirit::Pair(
          "seq",
          static_cast<int64_t>(e.seq)));
      entry.push_back(json_spirit::Pair(
          "init_time",
          format_time(e.init_time)));
      entry.push_back(json_spirit::Pair(
          "data",
          data));
      output.push_back(entry);
    }

    const json_spirit::Value root(output);
    if (format == "json-pretty") {
      cout << json_spirit::write_formatted(root) << endl;
    } else {
      cout << json_spirit::write(root) << endl;
    }

    return;
  }
}

// Helper to error check cli options and build AuditQuery based on input
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

  if (vm.count("tool-name")) {
    const std::string& tool_name = vm["tool-name"].as<std::string>();
    if (!TOOL_TO_TABLE_NAME.contains(tool_name)) {
      std::string accepted_tools_str;
      for (const auto& [name, _] : TOOL_TO_TABLE_NAME) {
        if (!accepted_tools_str.empty()) {
          accepted_tools_str += ", ";
        }
        accepted_tools_str += name;
      }
      throw std::invalid_argument(
          "Tool channel not found. Valid channels: " + accepted_tools_str);
    }
    config.table_name = TOOL_TO_TABLE_NAME.at(tool_name);
  }

  if (vm.count("limit")) {
    int64_t limit = vm["limit"].as<int64_t>();
    if (limit < 0) {
      throw std::invalid_argument("--limit must be >= 0.");
    }
    config.query.limit = limit;
  }

  if (vm.count("before-seq")) {
    int64_t before_seq = vm["before-seq"].as<int64_t>();
    if (before_seq < 0) {
      throw std::invalid_argument("--before-seq must be >= 0.");
    }
    config.query.before_seq = before_seq;
  }

  if (vm.count("after-seq")) {
    int64_t after_seq = vm["after-seq"].as<int64_t>();
    if (after_seq < 0) {
      throw std::invalid_argument("--after-seq must be >= 0.");
    }
    config.query.after_seq = after_seq;
  }

  if (vm.count("before-seq") && vm.count("after-seq") &&
      (config.query.before_seq && config.query.after_seq &&
       config.query.before_seq <= config.query.after_seq)) {
    throw std::invalid_argument(
        "--before-seq must be greater than --after-seq.");
  }

  if (vm.count("since")) {
    const std::string& since_str = vm["since"].as<std::string>();
    config.query.since = str_to_time_t(since_str);
  }

  if (vm.count("until")) {
    const std::string& until_str = vm["until"].as<std::string>();
    config.query.until = str_to_time_t(until_str);
  }

  if (vm.count("since") && vm.count("until") &&
      (config.query.since > config.query.until)) {
    throw std::invalid_argument("--since must be <= --until");
  }

  if (vm.count("range")) {
    const std::string& range_str = vm["range"].as<std::string>();
    std::pair<time_t, time_t> r = range_str_to_time(range_str);
    config.query.since = r.first;
    config.query.until = r.second;
  }

  if (vm.count("last")) {
    double hours = vm["last"].as<double>();
    if (hours < 0.0) {
      throw std::invalid_argument("--last must be a nonnegative number.");
    }
    time_t now = std::time(nullptr);
    config.query.since = now - static_cast<time_t>(hours * 3600);
  }

  if (vm.count("recent")) {
    int64_t n = vm["recent"].as<int64_t>();
    if (n < 0) {
      throw std::invalid_argument("--recent must be a nonnegative number.");
    }
    config.query.limit = n;
    config.query.order_by = "init_time";
    config.query.ascending = false;
  }

  if (vm.count("filter")) {
    for (const auto& raw : vm["filter"].as<std::vector<std::string>>()) {
      const auto eq = raw.find('=');
      if (eq == std::string::npos || eq == 0) {
        throw std::invalid_argument(
            "Invalid --filter value: '" + raw +
            "'. Expected format: field=value");
      }
      std::string field = raw.substr(0, eq);
      std::string value = raw.substr(eq + 1);
      config.query.json_filters.push_back({std::move(field), std::move(value)});
    }
  }

  if (vm.count("order-by")) {
    const std::string& order_by = vm["order-by"].as<std::string>();
    if (order_by != "seq" && order_by != "init_time") {
      throw std::invalid_argument("--order-by must be seq or init_time.");
    }
    config.query.order_by = order_by;
  }

  if (vm.count("order")) {
    const std::string& order = vm["order"].as<std::string>();
    if (order != "ASC" && order != "DESC") {
      throw std::invalid_argument("--order must be ASC or DESC.");
    }
    config.query.ascending = (order == "ASC");
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
  print_query_result_formatted(config.format, entries);
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
    ("tool-name", po::value<std::string>()->required(),
        "The audit channel to query (e.g. cephfs-data-scan).")
    ("limit", po::value<int64_t>(),
        "Cap the number of rows returned.")
    ("before-seq", po::value<int64_t>(),
        "Return only entries whose seq number is less than N.")
    ("after-seq", po::value<int64_t>(),
        "Return only entries whose seq number is greater than N.")
    ("since", po::value<std::string>(),
        "Return entries recorded on or after this timestamp. "
        "Format: \"YYYY-MM-DD\" or \"YYYY-MM-DD HH:MM:SS\".")
    ("until", po::value<std::string>(),
        "Return entries recorded on or before this timestamp. "
        "Format: \"YYYY-MM-DD\" or \"YYYY-MM-DD HH:MM:SS\".")
    ("range", po::value<std::string>(),
        "Return entries within an inclusive timestamp range. "
        "Format: \"YYYY-MM-DD,YYYY-MM-DD\" or \"YYYY-MM-DD HH:MM:SS,YYYY-MM-DD HH:MM:SS\". "
        "Cannot be combined with --since, --until, or --last.")
    ("last", po::value<double>(),
        "Return entries from the last N hours before now. "
        "Accepts decimals (e.g. 0.5 for the last 30 minutes).")
    ("recent", po::value<int64_t>(),
        "Return the N most recent entries, ordered by init_time descending.")
    ("filter", po::value<std::vector<std::string>>()->composing(),
        "Filter results by a field inside the json_dump column. "
        "Format: field=value. Repeatable for multiple filters "
        "(e.g. --filter status=ok --filter cmd=data-scan).")
    ("order-by", po::value<std::string>(),
        "Column to sort results by. Valid values: seq, init_time.")
    ("order", po::value<std::string>()->default_value("DESC"),
        "Sort direction: ASC or DESC (default: DESC).")
    ("count", po::bool_switch(&config.count_mode),
        "Print the number of matching entries.")
    ("format", po::value<std::string>(&config.format)->default_value("plain"),
        "Output format: plain (default), json, json-pretty.")
  ;

  // Visible options for help output
  po::options_description visible("Allowed options");
  visible.add(general).add(query_opts);
  
  // Process cli options and build AuditCliConfig (AuditDB's AuditQuery)
  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, visible), vm);
    if (vm.count("help")) {
      cout << "Usage: auditman --tool-name <tool> [query-options]\n\n";
      cout << "Query audit records stored in AuditDB for a specific channel.\n\n";
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
    cout << "Usage: auditman --tool-name <tool> [query-options]\n";
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