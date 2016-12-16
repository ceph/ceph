// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/Cond.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "include/stringify.h"
#include <fstream>
#include <sstream>
#include <boost/program_options.hpp>
#include "cls/rbd/cls_rbd_client.h"
#include "cls/journal/cls_journal_types.h"
#include "cls/journal/cls_journal_client.h"

#include "journal/Journaler.h"
#include "journal/ReplayEntry.h"
#include "journal/ReplayHandler.h"
#include "journal/Settings.h"
#include "librbd/journal/Types.h"

namespace rbd {
namespace action {
namespace journal {

namespace at = argument_types;
namespace po = boost::program_options;

static int do_show_journal_info(librados::Rados& rados, librados::IoCtx& io_ctx,
				const std::string& journal_id, Formatter *f)
{
  int r;
  C_SaferCond cond;

  std::string header_oid = ::journal::Journaler::header_oid(journal_id);
  std::string object_oid_prefix = ::journal::Journaler::object_oid_prefix(
    io_ctx.get_id(), journal_id);
  uint8_t order;
  uint8_t splay_width;
  int64_t pool_id;

  cls::journal::client::get_immutable_metadata(io_ctx, header_oid, &order,
					       &splay_width, &pool_id, &cond);
  r = cond.wait();
  if (r < 0) {
    std::cerr << "failed to get journal metadata: "  << cpp_strerror(r)
	      << std::endl;
    return r;
  }

  std::string object_pool_name;
  if (pool_id >= 0) {
    r = rados.pool_reverse_lookup(pool_id, &object_pool_name);
    if (r < 0) {
      std::cerr << "error looking up pool name for pool_id=" << pool_id << ": "
		<< cpp_strerror(r) << std::endl;
    }
  }

  if (f) {
    f->open_object_section("journal");
    f->dump_string("journal_id", journal_id);
    f->dump_string("header_oid", header_oid);
    f->dump_string("object_oid_prefix", object_oid_prefix);
    f->dump_int("order", order);
    f->dump_int("splay_width", splay_width);
    if (!object_pool_name.empty()) {
      f->dump_string("object_pool", object_pool_name);
    }
    f->close_section();
    f->flush(std::cout);
  } else {
    std::cout << "rbd journal '" << journal_id << "':" << std::endl;
    std::cout << "\theader_oid: " << header_oid << std::endl;
    std::cout << "\tobject_oid_prefix: " << object_oid_prefix << std::endl;
    std::cout << "\torder: " << static_cast<int>(order) << " ("
	      << prettybyte_t(1ull << order) << " objects)"<< std::endl;
    std::cout << "\tsplay_width: " << static_cast<int>(splay_width) << std::endl;
    if (!object_pool_name.empty()) {
      std::cout << "\tobject_pool: " << object_pool_name << std::endl;
    }
  }
  return 0;
}

static int do_show_journal_status(librados::IoCtx& io_ctx,
				  const std::string& journal_id, Formatter *f)
{
  int r;

  C_SaferCond cond;
  uint64_t minimum_set;
  uint64_t active_set;
  std::set<cls::journal::Client> registered_clients;
  std::string oid = ::journal::Journaler::header_oid(journal_id);

  cls::journal::client::get_mutable_metadata(io_ctx, oid, &minimum_set,
                                            &active_set, &registered_clients,
                                            &cond);
  r = cond.wait();
  if (r < 0) {
    std::cerr << "warning: failed to get journal metadata" << std::endl;
    return r;
  }

  if (f) {
    f->open_object_section("status");
    f->dump_unsigned("minimum_set", minimum_set);
    f->dump_unsigned("active_set", active_set);
    f->open_array_section("registered_clients");
    for (std::set<cls::journal::Client>::iterator c =
          registered_clients.begin(); c != registered_clients.end(); ++c) {
      f->open_object_section("client");
      c->dump(f);
      f->close_section();
    }
    f->close_section();
    f->close_section();
    f->flush(std::cout);
  } else {
    std::cout << "minimum_set: " << minimum_set << std::endl;
    std::cout << "active_set: " << active_set << std::endl;
    std::cout << "registered clients: " << std::endl;
    for (std::set<cls::journal::Client>::iterator c =
          registered_clients.begin(); c != registered_clients.end(); ++c) {
      std::cout << "\t" << *c << std::endl;
    }
  }
  return 0;
}

static int do_reset_journal(librados::IoCtx& io_ctx,
			    const std::string& journal_id)
{
  // disable/re-enable journaling to delete/re-create the journal
  // to properly handle mirroring constraints
  std::string image_name;
  int r = librbd::cls_client::dir_get_name(&io_ctx, RBD_DIRECTORY, journal_id,
                                           &image_name);
  if (r < 0) {
    std::cerr << "failed to locate journal's image: " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  librbd::Image image;
  r = utils::open_image(io_ctx, image_name, false, &image);
  if (r < 0) {
    std::cerr << "failed to open image: " << cpp_strerror(r) << std::endl;
    return r;
  }

  r = image.update_features(RBD_FEATURE_JOURNALING, false);
  if (r < 0) {
    std::cerr << "failed to disable image journaling: " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  r = image.update_features(RBD_FEATURE_JOURNALING, true);
  if (r < 0) {
    std::cerr << "failed to re-enable image journaling: " << cpp_strerror(r)
              << std::endl;
    return r;
  }
  return 0;
}

static int do_disconnect_journal_client(librados::IoCtx& io_ctx,
					const std::string& journal_id,
					const std::string& client_id)
{
  int r;

  C_SaferCond cond;
  uint64_t minimum_set;
  uint64_t active_set;
  std::set<cls::journal::Client> registered_clients;
  std::string oid = ::journal::Journaler::header_oid(journal_id);

  cls::journal::client::get_mutable_metadata(io_ctx, oid, &minimum_set,
                                            &active_set, &registered_clients,
                                            &cond);
  r = cond.wait();
  if (r < 0) {
    std::cerr << "warning: failed to get journal metadata" << std::endl;
    return r;
  }

  static const std::string IMAGE_CLIENT_ID("");

  bool found = false;
  for (auto &c : registered_clients) {
    if (c.id == IMAGE_CLIENT_ID || (!client_id.empty() && client_id != c.id)) {
      continue;
    }
    r = cls::journal::client::client_update_state(io_ctx, oid, c.id,
				  cls::journal::CLIENT_STATE_DISCONNECTED);
    if (r < 0) {
      std::cerr << "warning: failed to disconnect client " << c.id << ": "
		<< cpp_strerror(r) << std::endl;
      return r;
    }
    std::cout << "client " << c.id << " disconnected" << std::endl;
    found = true;
  }

  if (!found) {
    if (!client_id.empty()) {
      std::cerr << "warning: client " << client_id << " is not registered"
		<< std::endl;
    } else {
      std::cerr << "no registered clients to disconnect" << std::endl;
    }
    return -ENOENT;
  }

  bufferlist bl;
  r = io_ctx.notify2(oid, bl, 5000, NULL);
  if (r < 0) {
    std::cerr << "warning: failed to notify state change:" << ": "
	      << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

class Journaler : public ::journal::Journaler {
public:
  Journaler(librados::IoCtx& io_ctx, const std::string& journal_id,
	    const std::string &client_id) :
    ::journal::Journaler(io_ctx, journal_id, client_id, {}) {
  }

  int init() {
    int r;

    // TODO register with librbd payload
    r = register_client(bufferlist());
    if (r < 0) {
      std::cerr << "failed to register client: " << cpp_strerror(r)
		<< std::endl;
      return r;
    }

    C_SaferCond cond;

    ::journal::Journaler::init(&cond);
    r = cond.wait();
    if (r < 0) {
      std::cerr << "failed to initialize journal: " << cpp_strerror(r)
		<< std::endl;
      (void) unregister_client();
      return r;
    }

    return 0;
  }

  int shut_down() {
    int r = unregister_client();
    if (r < 0) {
      std::cerr << "rbd: failed to unregister journal client: "
		<< cpp_strerror(r) << std::endl;
    }
    ::journal::Journaler::shut_down();

    return r;
  }
};

class JournalPlayer {
public:
  JournalPlayer(librados::IoCtx& io_ctx, const std::string& journal_id,
		const std::string &client_id) :
    m_journaler(io_ctx, journal_id, client_id),
    m_cond(),
    m_r(0) {
  }

  virtual ~JournalPlayer() {}

  virtual int exec() {
    int r;

    r = m_journaler.init();
    if (r < 0) {
      return r;
    }

    ReplayHandler replay_handler(this);

    m_journaler.start_replay(&replay_handler);

    r = m_cond.wait();
    if (r < 0) {
      std::cerr << "rbd: failed to process journal: " << cpp_strerror(r)
		<< std::endl;
      if (m_r == 0) {
       m_r = r;
      }
    }
    return m_r;
  }

  int shut_down() {
    return m_journaler.shut_down();
  }

protected:
  struct ReplayHandler : public ::journal::ReplayHandler {
    JournalPlayer *journal;
    explicit ReplayHandler(JournalPlayer *_journal) : journal(_journal) {}

    virtual void get() {}
    virtual void put() {}

    virtual void handle_entries_available() {
      journal->handle_replay_ready();
    }
    virtual void handle_complete(int r) {
      journal->handle_replay_complete(r);
    }
  };

  void handle_replay_ready() {
    int r = 0;
    while (true) {
      ::journal::ReplayEntry replay_entry;
      uint64_t tag_id;
      if (!m_journaler.try_pop_front(&replay_entry, &tag_id)) {
	break;
      }

      r = process_entry(replay_entry, tag_id);
      if (r < 0) {
	break;
      }
    }
  }

  virtual int process_entry(::journal::ReplayEntry replay_entry,
			    uint64_t tag_id) = 0;

  void handle_replay_complete(int r) {
    if (m_r == 0 && r < 0) {
      m_r = r;
    }
    m_journaler.stop_replay(&m_cond);
  }

  Journaler m_journaler;
  C_SaferCond m_cond;
  int m_r;
};

static int inspect_entry(bufferlist& data,
			 librbd::journal::EventEntry& event_entry,
			 bool verbose) {
  try {
    bufferlist::iterator it = data.begin();
    ::decode(event_entry, it);
  } catch (const buffer::error &err) {
    std::cerr << "failed to decode event entry: " << err.what() << std::endl;
    return -EINVAL;
  }
  if (verbose) {
    JSONFormatter f(true);
    f.open_object_section("event_entry");
    event_entry.dump(&f);
    f.close_section();
    f.flush(std::cout);
  }
  return 0;
}

class JournalInspector : public JournalPlayer {
public:
  JournalInspector(librados::IoCtx& io_ctx, const std::string& journal_id,
		   bool verbose) :
    JournalPlayer(io_ctx, journal_id, "INSPECT"),
    m_verbose(verbose),
    m_s() {
  }

  int exec() {
    int r = JournalPlayer::exec();
    m_s.print();
    return r;
  }

private:
  struct Stats {
    Stats() : total(0), error(0) {}

    void print() {
      std::cout << "Summary:" << std::endl
		<< "  " << total << " entries inspected, " << error << " errors"
		<< std::endl;
    }

    int total;
    int error;
  };

  int process_entry(::journal::ReplayEntry replay_entry,
		    uint64_t tag_id) {
    m_s.total++;
    if (m_verbose) {
      std::cout << "Entry: tag_id=" << tag_id << ", commit_tid="
		<< replay_entry.get_commit_tid() << std::endl;
    }
    bufferlist data = replay_entry.get_data();
    librbd::journal::EventEntry event_entry;
    int r = inspect_entry(data, event_entry, m_verbose);
    if (r < 0) {
      m_r = r;
      m_s.error++;
    }
    return 0;
  }

  bool m_verbose;
  Stats m_s;
};

static int do_inspect_journal(librados::IoCtx& io_ctx,
			      const std::string& journal_id,
			      bool verbose) {
  JournalInspector inspector(io_ctx, journal_id, verbose);
  int r = inspector.exec();
  if (r < 0) {
    inspector.shut_down();
    return r;
  }

  r = inspector.shut_down();
  if (r < 0) {
    return r;
  }
  return 0;
}

struct ExportEntry {
  uint64_t tag_id;
  uint64_t commit_tid;
  int type;
  bufferlist entry;

  ExportEntry() : tag_id(0), commit_tid(0), type(0), entry() {}

  ExportEntry(uint64_t tag_id, uint64_t commit_tid, int type,
	      const bufferlist& entry)
    : tag_id(tag_id), commit_tid(commit_tid), type(type), entry(entry) {
  }

  void dump(Formatter *f) const {
    ::encode_json("tag_id", tag_id, f);
    ::encode_json("commit_tid", commit_tid, f);
    ::encode_json("type", type, f);
    ::encode_json("entry", entry, f);
  }

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("tag_id", tag_id, obj);
    JSONDecoder::decode_json("commit_tid", commit_tid, obj);
    JSONDecoder::decode_json("type", type, obj);
    JSONDecoder::decode_json("entry", entry, obj);
  }
};

class JournalExporter : public JournalPlayer {
public:
  JournalExporter(librados::IoCtx& io_ctx, const std::string& journal_id,
		  int fd, bool no_error, bool verbose) :
    JournalPlayer(io_ctx, journal_id, "EXPORT"),
    m_journal_id(journal_id),
    m_fd(fd),
    m_no_error(no_error),
    m_verbose(verbose),
    m_s() {
  }

  int exec() {
    std::string header("# journal_id: " + m_journal_id + "\n");
    int r;
    r = safe_write(m_fd, header.c_str(), header.size());
    if (r < 0) {
      std::cerr << "rbd: failed to write to export file: " << cpp_strerror(r)
		<< std::endl;
      return r;
    }
    r = JournalPlayer::exec();
    m_s.print();
    return r;
  }

private:
  struct Stats {
    Stats() : total(0), error(0) {}

    void print() {
      std::cout << total << " entries processed, " << error << " errors"
		<< std::endl;
    }

    int total;
    int error;
  };

  int process_entry(::journal::ReplayEntry replay_entry,
		    uint64_t tag_id) {
    m_s.total++;
    int type = -1;
    bufferlist entry = replay_entry.get_data();
    librbd::journal::EventEntry event_entry;
    int r = inspect_entry(entry, event_entry, m_verbose);
    if (r < 0) {
      m_s.error++;
      m_r = r;
      return m_no_error ? 0 : r;
    } else {
      type = event_entry.get_event_type();
    }
    ExportEntry export_entry(tag_id, replay_entry.get_commit_tid(), type,
                             entry);
    JSONFormatter f;
    ::encode_json("event_entry", export_entry, &f);
    std::ostringstream oss;
    f.flush(oss);
    std::string objstr = oss.str();
    std::string header = stringify(objstr.size()) + " ";
    r = safe_write(m_fd, header.c_str(), header.size());
    if (r == 0) {
      r = safe_write(m_fd, objstr.c_str(), objstr.size());
    }
    if (r == 0) {
      r = safe_write(m_fd, "\n", 1);
    }
    if (r < 0) {
      std::cerr << "rbd: failed to write to export file: " << cpp_strerror(r)
		<< std::endl;
      m_s.error++;
      return r;
    }
    return 0;
  }

  std::string m_journal_id;
  int m_fd;
  bool m_no_error;
  bool m_verbose;
  Stats m_s;
};

static int do_export_journal(librados::IoCtx& io_ctx,
			     const std::string& journal_id,
			     const std::string& path,
			     bool no_error, bool verbose) {
  int r;
  int fd;
  bool to_stdout = path == "-";
  if (to_stdout) {
    fd = STDOUT_FILENO;
  } else {
    fd = open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0) {
      r = -errno;
      std::cerr << "rbd: error creating " << path << std::endl;
      return r;
    }
    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
  }

  JournalExporter exporter(io_ctx, journal_id, fd, no_error, verbose);
  r = exporter.exec();

  if (!to_stdout) {
    close(fd);
  }

  int shut_down_r = exporter.shut_down();
  if (r == 0 && shut_down_r < 0) {
    r = shut_down_r;
  }

  return r;
}

class JournalImporter {
public:
  JournalImporter(librados::IoCtx& io_ctx, const std::string& journal_id,
		  int fd, bool no_error, bool verbose) :
    m_journaler(io_ctx, journal_id, "IMPORT"),
    m_fd(fd),
    m_no_error(no_error),
    m_verbose(verbose) {
  }

  bool read_entry(bufferlist& bl, int& r) {
    // Entries are storead in the file using the following format:
    //
    //   # Optional comments
    //   NNN {json encoded entry}
    //   ...
    //
    // Where NNN is the encoded entry size.
    bl.clear();
    char buf[80];
    // Skip line feed and comments (lines started with #).
    while ((r = safe_read_exact(m_fd, buf, 1)) == 0) {
      if (buf[0] == '\n') {
	continue;
      } else if (buf[0] == '#') {
	while ((r = safe_read_exact(m_fd, buf, 1)) == 0) {
	  if (buf[0] == '\n') {
	    break;
	  }
	}
      } else {
	break;
      }
    }
    if (r < 0) {
      if (r == -EDOM) {
	r = 0;
      }
      return false;
    }
    // Read entry size to buf.
    if (!isdigit(buf[0])) {
      r = -EINVAL;
      std::cerr << "rbd: import data invalid format (digit expected)"
		<< std::endl;
      return false;
    }
    for (size_t i = 1; i < sizeof(buf); i++) {
      r = safe_read_exact(m_fd, buf + i, 1);
      if (r < 0) {
	std::cerr << "rbd: error reading import data" << std::endl;
	return false;
      }
      if (!isdigit(buf[i])) {
	if (buf[i] != ' ') {
	  r = -EINVAL;
	  std::cerr << "rbd: import data invalid format (space expected)"
		    << std::endl;
	  return false;
	}
	buf[i] = '\0';
	break;
      }
    }
    int entry_size = atoi(buf);
    if (entry_size == 0) {
      r = -EINVAL;
      std::cerr << "rbd: import data invalid format (zero entry size)"
		<< std::endl;
      return false;
    }
    assert(entry_size > 0);
    // Read entry.
    r = bl.read_fd(m_fd, entry_size);
    if (r < 0) {
      std::cerr << "rbd: error reading from stdin: " << cpp_strerror(r)
		<< std::endl;
      return false;
    }
    if (r != entry_size) {
      std::cerr << "rbd: error reading from stdin: trucated"
		<< std::endl;
      r = -EINVAL;
      return false;
    }
    r = 0;
    return true;
  }

  int exec() {
    int r = m_journaler.init();
    if (r < 0) {
      return r;
    }
    m_journaler.start_append(0, 0, 0);

    int r1 = 0;
    bufferlist bl;
    int n = 0;
    int error_count = 0;
    while (read_entry(bl, r)) {
      n++;
      error_count++;
      JSONParser p;
      if (!p.parse(bl.c_str(), bl.length())) {
	std::cerr << "rbd: error parsing input (entry " << n << ")"
		  << std::endl;
	r = -EINVAL;
	if (m_no_error) {
	  r1 = r;
	  continue;
	} else {
	  break;
	}
      }
      ExportEntry e;
      try {
	decode_json_obj(e, &p);
      } catch (JSONDecoder::err& err) {
	std::cerr << "rbd: error json decoding import data (entry " << n << "):"
		  << err.message << std::endl;
	r = -EINVAL;
	if (m_no_error) {
	  r1 = r;
	  continue;
	} else {
	  break;
	}
      }
      librbd::journal::EventEntry event_entry;
      r = inspect_entry(e.entry, event_entry, m_verbose);
      if (r < 0) {
	std::cerr << "rbd: corrupted entry " << n << ": tag_tid=" << e.tag_id
		  << ", commit_tid=" << e.commit_tid << std::endl;
	if (m_no_error) {
	  r1 = r;
	  continue;
	} else {
	  break;
	}
      }
      m_journaler.append(e.tag_id, e.entry);
      error_count--;
    }

    std::cout << n << " entries processed, " << error_count << " errors"  << std::endl;

    std::cout << "Waiting for journal append to complete..."  << std::endl;

    C_SaferCond cond;
    m_journaler.stop_append(&cond);
    r = cond.wait();

    if (r < 0) {
      std::cerr << "failed to append journal: " << cpp_strerror(r) << std::endl;
    }

    if (r1 < 0 && r == 0) {
      r = r1;
    }
    return r;
  }

  int shut_down() {
    return m_journaler.shut_down();
  }

private:
  Journaler m_journaler;
  int m_fd;
  bool m_no_error;
  bool m_verbose;
};

static int do_import_journal(librados::IoCtx& io_ctx,
			     const std::string& journal_id,
			     const std::string& path,
			     bool no_error, bool verbose) {
  int r;

  int fd;
  bool from_stdin = path == "-";
  if (from_stdin) {
    fd = STDIN_FILENO;
  } else {
    if ((fd = open(path.c_str(), O_RDONLY)) < 0) {
      r = -errno;
      std::cerr << "rbd: error opening " << path << std::endl;
      return r;
    }
    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
  }

  JournalImporter importer(io_ctx, journal_id, fd, no_error, verbose);
  r = importer.exec();

  if (!from_stdin) {
    close(fd);
  }

  int shut_down_r = importer.shut_down();
  if (r == 0 && shut_down_r < 0) {
    r = shut_down_r;
  }

  return r;
}

void get_info_arguments(po::options_description *positional,
			po::options_description *options) {
  at::add_journal_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_format_options(options);
}

int execute_info(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string journal_name;
  int r = utils::get_pool_journal_names(vm, at::ARGUMENT_MODIFIER_NONE,
					&arg_index, &pool_name, &journal_name);
  if (r < 0) {
    return r;
  }

  at::Format::Formatter formatter;
  r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = do_show_journal_info(rados, io_ctx, journal_name, formatter.get());
  if (r < 0) {
    std::cerr << "rbd: journal info: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;

}

void get_status_arguments(po::options_description *positional,
			  po::options_description *options) {
  at::add_journal_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_format_options(options);
}

int execute_status(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string journal_name;
  int r = utils::get_pool_journal_names(vm, at::ARGUMENT_MODIFIER_NONE,
					&arg_index, &pool_name, &journal_name);
  if (r < 0) {
    return r;
  }

  at::Format::Formatter formatter;
  r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = do_show_journal_status(io_ctx, journal_name, formatter.get());
  if (r < 0) {
    std::cerr << "rbd: journal status: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

void get_reset_arguments(po::options_description *positional,
			 po::options_description *options) {
  at::add_journal_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
}

int execute_reset(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string journal_name;
  int r = utils::get_pool_journal_names(vm, at::ARGUMENT_MODIFIER_NONE,
					&arg_index, &pool_name, &journal_name);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = do_reset_journal(io_ctx, journal_name);
  if (r < 0) {
    std::cerr << "rbd: journal reset: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

void get_client_disconnect_arguments(po::options_description *positional,
				     po::options_description *options) {
  at::add_journal_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  options->add_options()
    ("client-id", po::value<std::string>(),
     "client ID (or leave unspecified to disconnect all)");
}

int execute_client_disconnect(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string journal_name;
  int r = utils::get_pool_journal_names(vm, at::ARGUMENT_MODIFIER_NONE,
					&arg_index, &pool_name, &journal_name);
  if (r < 0) {
    return r;
  }

  std::string client_id;
  if (vm.count("client-id")) {
    client_id = vm["client-id"].as<std::string>();
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = do_disconnect_journal_client(io_ctx, journal_name, client_id);
  if (r < 0) {
    std::cerr << "rbd: journal client disconnect: " << cpp_strerror(r)
	      << std::endl;
    return r;
  }
  return 0;
}

void get_inspect_arguments(po::options_description *positional,
			   po::options_description *options) {
  at::add_journal_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_verbose_option(options);
}

int execute_inspect(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string journal_name;
  int r = utils::get_pool_journal_names(vm, at::ARGUMENT_MODIFIER_NONE,
					&arg_index, &pool_name, &journal_name);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = do_inspect_journal(io_ctx, journal_name, vm[at::VERBOSE].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: journal inspect: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

void get_export_arguments(po::options_description *positional,
			  po::options_description *options) {
  at::add_journal_spec_options(positional, options,
			       at::ARGUMENT_MODIFIER_SOURCE);
  at::add_path_options(positional, options,
                       "export file (or '-' for stdout)");
  at::add_verbose_option(options);
  at::add_no_error_option(options);
}

int execute_export(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string journal_name;
  int r = utils::get_pool_journal_names(vm, at::ARGUMENT_MODIFIER_SOURCE,
					&arg_index, &pool_name, &journal_name);
  if (r < 0) {
    return r;
  }

  std::string path;
  r = utils::get_path(vm, utils::get_positional_argument(vm, 1), &path);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = do_export_journal(io_ctx, journal_name, path, vm[at::NO_ERROR].as<bool>(),
			vm[at::VERBOSE].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: journal export: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

void get_import_arguments(po::options_description *positional,
			  po::options_description *options) {
  at::add_path_options(positional, options,
                       "import file (or '-' for stdin)");
  at::add_journal_spec_options(positional, options, at::ARGUMENT_MODIFIER_DEST);
  at::add_verbose_option(options);
  at::add_no_error_option(options);
}

int execute_import(const po::variables_map &vm) {
  std::string path;
  int r = utils::get_path(vm, utils::get_positional_argument(vm, 0), &path);
  if (r < 0) {
    return r;
  }

  size_t arg_index = 1;
  std::string pool_name;
  std::string journal_name;
  r = utils::get_pool_journal_names(vm, at::ARGUMENT_MODIFIER_DEST,
				    &arg_index, &pool_name, &journal_name);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = do_import_journal(io_ctx, journal_name, path, vm[at::NO_ERROR].as<bool>(),
			vm[at::VERBOSE].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: journal export: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action_info(
  {"journal", "info"}, {}, "Show information about image journal.", "",
  &get_info_arguments, &execute_info);

Shell::Action action_status(
  {"journal", "status"}, {}, "Show status of image journal.", "",
  &get_status_arguments, &execute_status);

Shell::Action action_reset(
  {"journal", "reset"}, {}, "Reset image journal.", "",
  &get_reset_arguments, &execute_reset);

Shell::Action action_inspect(
  {"journal", "inspect"}, {}, "Inspect image journal for structural errors.", "",
  &get_inspect_arguments, &execute_inspect);

Shell::Action action_export(
  {"journal", "export"}, {}, "Export image journal.", "",
  &get_export_arguments, &execute_export);

Shell::Action action_import(
  {"journal", "import"}, {}, "Import image journal.", "",
  &get_import_arguments, &execute_import);

Shell::Action action_disconnect(
  {"journal", "client", "disconnect"}, {},
  "Flag image journal client as disconnected.", "",
  &get_client_disconnect_arguments, &execute_client_disconnect);

} // namespace journal
} // namespace action
} // namespace rbd
