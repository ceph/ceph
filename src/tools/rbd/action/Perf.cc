// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/stringify.h"
#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include "global/global_context.h"
#ifdef HAVE_CURSES
#include <ncurses.h>
#endif
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <iostream>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/assign.hpp>
#include <boost/bimap.hpp>
#include <boost/program_options.hpp>
#include "json_spirit/json_spirit.h"

namespace rbd {
namespace action {
namespace perf {

namespace at = argument_types;
namespace po = boost::program_options;

namespace {

enum class StatDescriptor {
  WRITE_OPS = 0,
  READ_OPS,
  WRITE_BYTES,
  READ_BYTES,
  WRITE_LATENCY,
  READ_LATENCY
};

typedef boost::bimap<StatDescriptor, std::string> StatDescriptors;

static const StatDescriptors STAT_DESCRIPTORS =
  boost::assign::list_of<StatDescriptors::relation>
    (StatDescriptor::WRITE_OPS, "write_ops")
    (StatDescriptor::READ_OPS, "read_ops")
    (StatDescriptor::WRITE_BYTES, "write_bytes")
    (StatDescriptor::READ_BYTES, "read_bytes")
    (StatDescriptor::WRITE_LATENCY, "write_latency")
    (StatDescriptor::READ_LATENCY, "read_latency");

std::ostream& operator<<(std::ostream& os, const StatDescriptor& val) {
  auto it = STAT_DESCRIPTORS.left.find(val);
  if (it == STAT_DESCRIPTORS.left.end()) {
    os << "unknown (" << static_cast<int>(val) << ")";
  } else {
    os << it->second;
  }
  return os;
}

void validate(boost::any& v, const std::vector<std::string>& values,
              StatDescriptor *target_type, int) {
  po::validators::check_first_occurrence(v);
  std::string s = po::validators::get_single_string(values);
  boost::replace_all(s, "_", " ");
  boost::replace_all(s, "-", "_");

  auto it = STAT_DESCRIPTORS.right.find(s);
  if (it == STAT_DESCRIPTORS.right.end()) {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }
  v = boost::any(it->second);
}

struct ImageStat {
  ImageStat(const std::string& pool_name, const std::string& pool_namespace,
            const std::string& image_name)
    : pool_name(pool_name), pool_namespace(pool_namespace),
      image_name(image_name) {
    stats.resize(STAT_DESCRIPTORS.size());
  }

  std::string pool_name;
  std::string pool_namespace;
  std::string image_name;
  std::vector<double> stats;
};

typedef std::vector<ImageStat> ImageStats;

typedef std::pair<std::string, std::string> SpecPair;

std::string format_pool_spec(const std::string& pool,
                             const std::string& pool_namespace) {
  std::string pool_spec{pool};
  if (!pool_namespace.empty()) {
    pool_spec += "/" + pool_namespace;
  }
  return pool_spec;
}

int query_iostats(librados::Rados& rados, const std::string& pool_spec,
                  StatDescriptor sort_by, ImageStats* image_stats,
                  std::ostream& err_os) {
  auto sort_by_str = STAT_DESCRIPTORS.left.find(sort_by)->second;

  std::string cmd = R"(
    {
      "prefix": "rbd perf image stats",
      "pool_spec": ")" + pool_spec + R"(",
      "sort_by": ")" + sort_by_str + R"(",
      "format": "json"
    }")";

  bufferlist in_bl;
  bufferlist out_bl;
  std::string outs;
  int r = rados.mgr_command(cmd, in_bl, &out_bl, &outs);
  if (r == -EOPNOTSUPP) {
    err_os << "rbd: 'rbd_support' mgr module is not enabled."
           << std::endl << std::endl
           << "Use 'ceph mgr module enable rbd_support' to enable."
           << std::endl;
    return r;
  } else if (r < 0) {
    err_os << "rbd: mgr command failed: " << cpp_strerror(r);
    if (!outs.empty()) {
      err_os << ": " << outs;
    }
    err_os << std::endl;
    return r;
  }

  json_spirit::mValue json_root;
  if (!json_spirit::read(out_bl.to_str(), json_root)) {
    err_os << "rbd: error parsing perf stats" << std::endl;
    return -EINVAL;
  }

  image_stats->clear();
  try {
    auto& root = json_root.get_obj();

    // map JSON stat descriptor order to our internal order
    std::map<uint32_t, uint32_t> json_to_internal_stats;
    auto& json_stat_descriptors = root["stat_descriptors"].get_array();
    for (size_t idx = 0; idx < json_stat_descriptors.size(); ++idx) {
      auto it = STAT_DESCRIPTORS.right.find(
        json_stat_descriptors[idx].get_str());
      if (it == STAT_DESCRIPTORS.right.end()) {
        continue;
      }
      json_to_internal_stats[idx] = static_cast<uint32_t>(it->second);
    }

    // cache a mapping from pool descriptors back to pool-specs
    std::map<std::string, SpecPair> json_to_internal_pools;
    auto& pool_descriptors = root["pool_descriptors"].get_obj();
    for (auto& pool : pool_descriptors) {
      auto& pool_spec = pool.second.get_str();
      auto pos = pool_spec.rfind("/");

      SpecPair pair{pool_spec.substr(0, pos), ""};
      if (pos != std::string::npos) {
        pair.second = pool_spec.substr(pos + 1);
      }

      json_to_internal_pools[pool.first] = pair;
    }

    auto& stats = root["stats"].get_array();
    for (auto& stat : stats) {
      auto& stat_obj = stat.get_obj();
      if (!stat_obj.empty()) {
        auto& image_spec = stat_obj.begin()->first;

        auto pos = image_spec.find("/");
        SpecPair pair{image_spec.substr(0, pos), ""};
        if (pos != std::string::npos) {
          pair.second = image_spec.substr(pos + 1);
        }

        const auto pool_it = json_to_internal_pools.find(pair.first);
        if (pool_it == json_to_internal_pools.end()) {
          continue;
        }

        image_stats->emplace_back(
          pool_it->second.first, pool_it->second.second, pair.second);

        auto& image_stat = image_stats->back();
        auto& data = stat_obj.begin()->second.get_array();
        for (auto& indexes : json_to_internal_stats) {
          image_stat.stats[indexes.second] = data[indexes.first].get_real();
        }
      }
    }
  } catch (std::runtime_error &e) {
    err_os << "rbd: error parsing perf stats: " << e.what() << std::endl;
    return -EINVAL;
  }

  return 0;
}

void format_stat(StatDescriptor stat_descriptor, double stat,
                 std::ostream& os) {
  switch (stat_descriptor) {
  case StatDescriptor::WRITE_OPS:
  case StatDescriptor::READ_OPS:
    os << si_u_t(stat) << "/s";
    break;
  case StatDescriptor::WRITE_BYTES:
  case StatDescriptor::READ_BYTES:
    os << byte_u_t(stat) << "/s";
    break;
  case StatDescriptor::WRITE_LATENCY:
  case StatDescriptor::READ_LATENCY:
    os << std::fixed << std::setprecision(2);
    if (stat >= 1000000000) {
      os << (stat / 1000000000) << " s";
    } else if (stat >= 1000000) {
      os << (stat / 1000000) << " ms";
    } else if (stat >= 1000) {
      os << (stat / 1000) << " us";
    } else {
      os << stat << " ns";
    }
    break;
  default:
    ceph_assert(false);
    break;
  }
}

} // anonymous namespace

namespace iostat {

struct Iterations {};

void validate(boost::any& v, const std::vector<std::string>& values,
              Iterations *target_type, int) {
  po::validators::check_first_occurrence(v);
  auto& s = po::validators::get_single_string(values);

  try {
    auto iterations = boost::lexical_cast<uint32_t>(s);
    if (iterations > 0) {
      v = boost::any(iterations);
      return;
    }
  } catch (const boost::bad_lexical_cast &) {
  }
  throw po::validation_error(po::validation_error::invalid_option_value);
}

void format(const ImageStats& image_stats, Formatter* f, bool global_search) {
  TextTable tbl;
  if (f) {
    f->open_array_section("images");
  } else {
    tbl.define_column("NAME", TextTable::LEFT, TextTable::LEFT);
    for (auto& stat : STAT_DESCRIPTORS.left) {
      std::string title;
      switch (stat.first) {
      case StatDescriptor::WRITE_OPS:
        title = "WR ";
        break;
      case StatDescriptor::READ_OPS:
        title = "RD ";
        break;
      case StatDescriptor::WRITE_BYTES:
        title = "WR_BYTES ";
        break;
      case StatDescriptor::READ_BYTES:
        title = "RD_BYTES ";
        break;
      case StatDescriptor::WRITE_LATENCY:
        title = "WR_LAT ";
        break;
      case StatDescriptor::READ_LATENCY:
        title = "RD_LAT ";
        break;
      default:
        ceph_assert(false);
        break;
      }
      tbl.define_column(title, TextTable::RIGHT, TextTable::RIGHT);
    }
  }

  for (auto& image_stat : image_stats) {
    if (f)  {
      f->open_object_section("image");
      f->dump_string("pool", image_stat.pool_name);
      f->dump_string("pool_namespace", image_stat.pool_namespace);
      f->dump_string("image", image_stat.image_name);
      for (auto& pair : STAT_DESCRIPTORS.left) {
        f->dump_float(pair.second.c_str(),
                      image_stat.stats[static_cast<size_t>(pair.first)]);
      }
      f->close_section();
    } else {
      std::string name;
      if (global_search) {
        name += image_stat.pool_name + "/";
        if (!image_stat.pool_namespace.empty()) {
          name += image_stat.pool_namespace + "/";
        }
      }
      name += image_stat.image_name;

      tbl << name;
      for (auto& pair : STAT_DESCRIPTORS.left) {
        std::stringstream str;
        format_stat(pair.first,
                    image_stat.stats[static_cast<size_t>(pair.first)], str);
        str << ' ';
        tbl << str.str();
      }
      tbl << TextTable::endrow;
    }
  }

  if (f) {
    f->close_section();
    f->flush(std::cout);
  } else {
    std::cout << tbl << std::endl;
  }
}

} // namespace iostat

#ifdef HAVE_CURSES
namespace iotop {

class MainWindow {
public:
  MainWindow(librados::Rados& rados, const std::string& pool_spec)
  : m_rados(rados), m_pool_spec(pool_spec) {
    initscr();
    curs_set(0);
    cbreak();
    noecho();
    keypad(stdscr, TRUE);
    nodelay(stdscr, TRUE);

    init_columns();
  }

  int run() {
    redraw();

    int r = 0;
    std::stringstream err_str;
    while (true) {
      r = query_iostats(m_rados, m_pool_spec, m_sort_by, &m_image_stats,
                        err_str);
      if (r < 0) {
        break;
        return r;
      }

      redraw();
      wait_for_key_or_delay();

      int ch = getch();
      if (ch == 'q' || ch == 'Q') {
        break;
      } else if (ch == '<' || ch == KEY_LEFT) {
        auto it = STAT_DESCRIPTORS.left.find(m_sort_by);
        if (it != STAT_DESCRIPTORS.left.begin()) {
          m_sort_by = (--it)->first;
        }
      } else if (ch == '>' || ch == KEY_RIGHT) {
        auto it = STAT_DESCRIPTORS.left.find(m_sort_by);
        if (it != STAT_DESCRIPTORS.left.end() &&
            ++it != STAT_DESCRIPTORS.left.end()) {
          m_sort_by = it->first;
        }
      }
    }

    endwin();

    if (r < 0) {
      std::cerr << err_str.str() << std::endl;
    }
    return r;
  }

private:
  static const size_t STAT_COLUMN_WIDTH = 12;

  librados::Rados& m_rados;
  std::string m_pool_spec;

  ImageStats m_image_stats;
  StatDescriptor m_sort_by = StatDescriptor::WRITE_OPS;

  bool m_pending_win_opened = false;
  WINDOW* m_pending_win = nullptr;

  int m_height = 1;
  int m_width = 1;

  std::map<StatDescriptor, std::string> m_columns;

  void init_columns() {
    m_columns.clear();
    for (auto& pair : STAT_DESCRIPTORS.left) {
      std::string title;
      switch (pair.first) {
      case StatDescriptor::WRITE_OPS:
        title = "WRITES OPS";
        break;
      case StatDescriptor::READ_OPS:
        title = "READS OPS";
        break;
      case StatDescriptor::WRITE_BYTES:
        title = "WRITE BYTES";
        break;
      case StatDescriptor::READ_BYTES:
        title = "READ BYTES";
        break;
      case StatDescriptor::WRITE_LATENCY:
        title = "WRITE LAT";
        break;
      case StatDescriptor::READ_LATENCY:
        title = "READ LAT";
        break;
      default:
        ceph_assert(false);
        break;
      }
      m_columns[pair.first] = (title);
    }
  }

  void redraw() {
    getmaxyx(stdscr, m_height, m_width);

    redraw_main_window();
    redraw_pending_window();

    doupdate();
  }

  void redraw_main_window() {
    werase(stdscr);
    mvhline(0, 0, ' ' | A_REVERSE, m_width);

    // print header for all metrics
    int remaining_cols = m_width;
    std::stringstream str;
    for (auto& pair : m_columns) {
      int attr = A_REVERSE;
      std::string title;
      if (pair.first == m_sort_by) {
        title += '>';
        attr |= A_BOLD;
      } else {
        title += ' ';
      }
      title += pair.second;

      str.str("");
      str << std::right << std::setfill(' ')
         << std::setw(STAT_COLUMN_WIDTH)
         << title << ' ';

      attrset(attr);
      addstr(str.str().c_str());
      remaining_cols -= title.size();
    }

    attrset(A_REVERSE);
    addstr("IMAGE");
    attrset(A_NORMAL);

    // print each image (one per line)
    int row = 1;
    int remaining_lines = m_height - 1;
    for (auto& image_stat : m_image_stats) {
      if (remaining_lines <= 0) {
        break;
      }
      --remaining_lines;

      move(row++, 0);
      for (auto& pair : m_columns) {
        str.str("");
        format_stat(pair.first,
                    image_stat.stats[static_cast<size_t>(pair.first)], str);
        auto value = str.str().substr(0, STAT_COLUMN_WIDTH);

        str.str("");
        str << std::right << std::setfill(' ')
            << std::setw(STAT_COLUMN_WIDTH)
            << value << ' ';
        addstr(str.str().c_str());
      }

      std::string image;
      if (m_pool_spec.empty()) {
        image = format_pool_spec(image_stat.pool_name,
                                 image_stat.pool_namespace) + "/";
      }
      image += image_stat.image_name;
      addstr(image.substr(0, remaining_cols).c_str());
    }

    wnoutrefresh(stdscr);
  }

  void redraw_pending_window() {
    // draw a "please by patient" window while waiting
    const char* msg = "Waiting for initial stats";
    int height = 5;
    int width = strlen(msg) + 4;;
    int starty = (m_height - height) / 2;
    int startx = (m_width - width) / 2;

    if (m_image_stats.empty() && !m_pending_win_opened) {
      m_pending_win_opened = true;
      m_pending_win = newwin(height, width, starty, startx);
    }

    if (m_pending_win != nullptr) {
      if (m_image_stats.empty()) {
        box(m_pending_win, 0 , 0);
        mvwaddstr(m_pending_win, 2, 2, msg);
        wnoutrefresh(m_pending_win);
      } else {
        delwin(m_pending_win);
        m_pending_win = nullptr;
      }
    }
  }

  void wait_for_key_or_delay() {
    fd_set fds;
    FD_ZERO(&fds);
    FD_SET(STDIN_FILENO, &fds);

    // no point to refreshing faster than the stats period
    struct timeval tval;
    tval.tv_sec = std::min<uint32_t>(
      10, g_conf().get_val<int64_t>("mgr_stats_period"));
    tval.tv_usec = 0;

    select(STDIN_FILENO + 1, &fds, NULL, NULL, &tval);
  }
};

} // namespace iotop
#endif // HAVE_CURSES


void get_arguments_iostat(po::options_description *positional,
                          po::options_description *options) {
  at::add_pool_options(positional, options, true);
  options->add_options()
    ("iterations", po::value<iostat::Iterations>(),
     "iterations of metric collection [> 0]")
    ("sort-by", po::value<StatDescriptor>()->default_value(StatDescriptor::WRITE_OPS),
     "sort-by IO metric "
     "(write-ops, read-ops, write-bytes, read-bytes, write-latency, read-latency) "
     "[default: write-ops]");
  at::add_format_options(options);
}

int execute_iostat(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  std::string pool;
  std::string pool_namespace;
  size_t arg_index = 0;
  int r = utils::get_pool_and_namespace_names(vm, false, &pool,
                                              &pool_namespace, &arg_index);
  if (r < 0) {
    return r;
  }

  uint32_t iterations = 0;
  if (vm.count("iterations")) {
    iterations = vm["iterations"].as<uint32_t>();
  }
  auto sort_by = vm["sort-by"].as<StatDescriptor>();

  at::Format::Formatter formatter;
  r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  auto f = formatter.get();
  if (iterations > 1 && f != nullptr) {
    std::cerr << "rbd: specifying iterations is not valid with formatted output"
              << std::endl;
    return -EINVAL;
  }

  librados::Rados rados;
  r = utils::init_rados(&rados);
  if (r < 0) {
    return r;
  }

  r = rados.wait_for_latest_osdmap();
  if (r < 0) {
    std::cerr << "rbd: failed to retrieve OSD map" << std::endl;
    return r;
  }

  if (!pool_namespace.empty()) {
    // default empty pool name only if namespace is specified to allow
    // for an empty pool_spec (-> GLOBAL_POOL_KEY)
    utils::normalize_pool_name(&pool);
  }
  std::string pool_spec = format_pool_spec(pool, pool_namespace);

  // no point to refreshing faster than the stats period
  auto delay = std::min<uint32_t>(10, g_conf().get_val<int64_t>("mgr_stats_period"));

  ImageStats image_stats;
  uint32_t count = 0;
  bool printed_notice = false;
  while (count++ < iterations || iterations == 0) {
    r = query_iostats(rados, pool_spec, sort_by, &image_stats, std::cerr);
    if (r < 0) {
      return r;
    }

    if (count == 1 && image_stats.empty()) {
      count = 0;
      if (!printed_notice) {
        std::cerr << "rbd: waiting for initial image stats"
                  << std::endl << std::endl;;
        printed_notice = true;
      }
    } else {
      iostat::format(image_stats, f, pool_spec.empty());
      if (f != nullptr) {
        break;
      }
    }

    sleep(delay);
  }

  return 0;
}

#ifdef HAVE_CURSES
void get_arguments_iotop(po::options_description *positional,
                         po::options_description *options) {
  at::add_pool_options(positional, options, true);
}

int execute_iotop(const po::variables_map &vm,
                  const std::vector<std::string> &ceph_global_init_args) {
  std::string pool;
  std::string pool_namespace;
  size_t arg_index = 0;
  int r = utils::get_pool_and_namespace_names(vm, false, &pool,
                                              &pool_namespace, &arg_index);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  r = utils::init_rados(&rados);
  if (r < 0) {
    return r;
  }

  r = rados.wait_for_latest_osdmap();
  if (r < 0) {
    std::cerr << "rbd: failed to retrieve OSD map" << std::endl;
    return r;
  }

  if (!pool_namespace.empty()) {
    // default empty pool name only if namespace is specified to allow
    // for an empty pool_spec (-> GLOBAL_POOL_KEY)
    utils::normalize_pool_name(&pool);
  }
  iotop::MainWindow mainWindow(rados, format_pool_spec(pool, pool_namespace));
  r = mainWindow.run();
  if (r < 0) {
    return r;
  }

  return 0;
}

Shell::Action top_action(
  {"perf", "image", "iotop"}, {}, "Display a top-like IO monitor.", "",
  &get_arguments_iotop, &execute_iotop);

#endif // HAVE_CURSES

Shell::Action stat_action(
  {"perf", "image", "iostat"}, {}, "Display image IO statistics.", "",
  &get_arguments_iostat, &execute_iostat);
} // namespace perf
} // namespace action
} // namespace rbd
