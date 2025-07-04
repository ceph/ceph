#include "ceph_test_rados_io_sequence.h"

#include <boost/asio/io_context.hpp>
#include <iostream>
#include <vector>

#include "common/Formatter.h"
#include "common/Thread.h"
#include "common/ceph_argparse.h"
#include "common/ceph_json.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/split.h"
#include "common/strtol.h" // for strict_iecstrtoll()
#include "common/ceph_json.h"
#include "common/Formatter.h"

#include "common/io_exerciser/DataGenerator.h"
#include "common/io_exerciser/EcIoSequence.h"
#include "common/io_exerciser/IoOp.h"
#include "common/io_exerciser/IoSequence.h"
#include "common/io_exerciser/Model.h"
#include "common/io_exerciser/ObjectModel.h"
#include "common/io_exerciser/RadosIo.h"
#include "common/json/BalancerStructures.h"
#include "common/json/ConfigStructures.h"
#include "common/json/OSDStructures.h"
#include "fmt/format.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "include/interval_set.h"
#include "include/random.h"
#include "json_spirit/json_spirit.h"
#include "librados/librados_asio.h"

#define dout_subsys ceph_subsys_rados
#define dout_context g_ceph_context

using OpType = ceph::io_exerciser::OpType;

using DoneOp = ceph::io_exerciser::DoneOp;
using BarrierOp = ceph::io_exerciser::BarrierOp;
using CreateOp = ceph::io_exerciser::CreateOp;
using RemoveOp = ceph::io_exerciser::RemoveOp;
using SingleReadOp = ceph::io_exerciser::SingleReadOp;
using DoubleReadOp = ceph::io_exerciser::DoubleReadOp;
using TripleReadOp = ceph::io_exerciser::TripleReadOp;
using SingleWriteOp = ceph::io_exerciser::SingleWriteOp;
using DoubleWriteOp = ceph::io_exerciser::DoubleWriteOp;
using TripleWriteOp = ceph::io_exerciser::TripleWriteOp;
using SingleAppendOp = ceph::io_exerciser::SingleAppendOp;
using TruncateOp = ceph::io_exerciser::TruncateOp;
using SingleFailedWriteOp = ceph::io_exerciser::SingleFailedWriteOp;
using DoubleFailedWriteOp = ceph::io_exerciser::DoubleFailedWriteOp;
using TripleFailedWriteOp = ceph::io_exerciser::TripleFailedWriteOp;

namespace {
struct Size {};
void validate(boost::any& v, const std::vector<std::string>& values,
              Size* target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string& s = po::validators::get_single_string(values);

  std::string parse_error;
  uint64_t size = strict_iecstrtoll(s, &parse_error);
  if (!parse_error.empty()) {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }
  v = boost::any(size);
}

struct Pair {};
void validate(boost::any& v, const std::vector<std::string>& values,
              Pair* target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string& s = po::validators::get_single_string(values);
  auto part = ceph::split(s).begin();
  std::string parse_error;
  int first = strict_iecstrtoll(*part++, &parse_error);
  int second = strict_iecstrtoll(*part, &parse_error);
  if (!parse_error.empty()) {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }
  v = boost::any(std::pair<int, int>{first, second});
}

struct PluginString {};
void validate(boost::any& v, const std::vector<std::string>& values,
              PluginString* target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string& s = po::validators::get_single_string(values);

  const std::string_view* plugin_iter =
      std::find(ceph::io_sequence::tester::plugin_choices.begin(),
                ceph::io_sequence::tester::plugin_choices.end(), s);
  if (ceph::io_sequence::tester::plugin_choices.end() == plugin_iter) {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }

  v = boost::any(*plugin_iter);
}

constexpr std::string_view usage[] = {
    "Basic usage:",
    "",
    "ceph_test_rados_io_sequence",
    "\t Test I/O to a single object using default settings. Good for",
    "\t testing boundary conditions",
    "",
    "ceph_test_rados_io_sequence --parallel <n>",
    "\t Run parallel test to multiple objects. First object is tested with",
    "\t default settings, other objects are tested with random settings",
    "",
    "Advanced usage:",
    "",
    "ceph_test_rados_io_sequence --blocksize <b> --km <k,m> --plugin <p>",
    "                            --objectsize <min,max> --threads <t>",
    "ceph_test_rados_io_sequence --blocksize <b> --pool <p> --object <oid>",
    "                            --objectsize <min,max> --threads <t>",
    "\tCustomize the test, if a pool is specified then it defines the",
    "\tReplica/EC configuration",
    "",
    "ceph_test_rados_io_sequence --listsequence",
    "\t Display list of supported I/O sequences",
    "",
    "ceph_test_rados_io_sequence --dryrun --sequence <n>",
    "\t Show I/O that will be generated for a sequence, validate",
    "\t seqeunce has correct I/O barriers to restrict concurrency",
    "",
    "ceph_test_rados_io_sequence --seed <seed>",
    "\t Repeat a previous test with the same random numbers (seed is",
    "\t displayed at start of test), if threads = 1 then this will produce",
    "\t the exact same sequence of I/O, if threads > 1 then I/Os are issued",
    "\t in parallel so ordering might be slightly different",
    "",
    "ceph_test_rados_io_sequence --sequence <n> --seqseed <n>",
    "\t Repeat a sequence from a previous test with the same random",
    "\t numbers (seqseed is displayed at start of sequence)",
    "",
    "ceph_test_rados_io_sequence --pool <p> --object <oid> --interactive",
    "\t Execute sequence of I/O commands from stdin. Offset and length",
    "\t are specified with unit of blocksize. Supported commands:",
    "\t\t create <len>",
    "\t\t remove",
    "\t\t read|write|failedwrite <off> <len>",
    "\t\t read2|write2|failedwrite2 <off> <len> <off> <len>",
    "\t\t read3|write3|failedwrite3 <off> <len> <off> <len> <off> <len>",
    "\t\t injecterror <type> <shard> <good_count> <fail_count>",
    "\t\t clearinject <type> <shard>",
    "\t\t done"};

po::options_description get_options_description() {
  po::options_description desc("ceph_test_rados_io options");
  desc.add_options()("help,h", "show help message")("listsequence,l",
                                                    "show list of sequences")(
      "dryrun,d", "test sequence, do not issue any I/O")(
      "verbose", "more verbose output during test")(
      "sequence,s", po::value<int>(), "test specified sequence")(
      "seed", po::value<int>(), "seed for whole test")(
      "seqseed", po::value<int>(), "seed for sequence")(
      "blocksize,b", po::value<Size>(), "block size (default 2048)")(
      "pool,p", po::value<std::string>(), "existing pool name")(
      "profile", po::value<std::string>(), "existing profile name")(
      "object,o", po::value<std::string>()->default_value("test"),
      "object name")("plugin", po::value<PluginString>(), "EC plugin")(
      "chunksize,c", po::value<Size>(), "chunk size (default 4096)")(
      "km", po::value<Pair>(), "k,m EC pool profile (default 2,2)")(
      "technique", po::value<std::string>(), "EC profile technique")(
      "packetsize", po::value<uint64_t>(), "Jerasure EC profile packetsize")(
      "w", po::value<uint64_t>(), "Jerasure EC profile w value")(
      "c", po::value<uint64_t>(), "Shec EC profile c value")(
      "mapping", po::value<std::string>(), "LRC EC profile mapping")(
      "layers", po::value<std::string>(), "LRC EC profile layers")(
      "objectsize", po::value<Pair>(),
      "min,max object size in blocks (default 1,32)")(
      "threads,t", po::value<int>(),
      "number of threads of I/O per object (default 1)")(
      "parallel,p", po::value<int>()->default_value(1),
      "number of objects to exercise in parallel")(
      "testrecovery",
      "Inject errors during sequences to test recovery processes of OSDs")(
      "interactive", "interactive mode, execute IO commands from stdin")(
      "allow_pool_autoscaling",
      "Allows pool autoscaling. Disabled by default.")(
      "allow_pool_balancer", "Enables pool balancing. Disabled by default.")(
      "allow_pool_deep_scrubbing",
      "Enables pool deep scrub. Disabled by default.")(
      "allow_pool_scrubbing", "Enables pool scrubbing. Disabled by default.")(
      "disable_pool_ec_optimizations",
      "Disables EC optimizations. Enabled by default.")(
      "allow_unstable_pool_configs",
      "Permits pool configs that are known to be unstable. This option "
      " may be removed. at a later date. Disabled by default if ec optimized");

  return desc;
}

int parse_io_seq_options(po::variables_map& vm, int argc, char** argv) {
  std::vector<std::string> unrecognized_options;
  try {
    po::options_description desc = get_options_description();

    auto parsed = po::command_line_parser(argc, argv)
                      .options(desc)
                      .allow_unregistered()
                      .run();
    po::store(parsed, vm);
    po::notify(vm);
    unrecognized_options =
        po::collect_unrecognized(parsed.options, po::include_positional);

    if (!unrecognized_options.empty()) {
      std::stringstream ss;
      ss << "Unrecognised command options supplied: ";
      while (unrecognized_options.size() > 1) {
        ss << unrecognized_options.back().c_str() << ", ";
        unrecognized_options.pop_back();
      }
      ss << unrecognized_options.back();
      dout(0) << ss.str() << dendl;
      return 1;
    }
  } catch (const po::error& e) {
    std::cerr << "error: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}

template <typename S>
int send_mon_command(S& s, librados::Rados& rados, const char* name,
                     ceph::buffer::list& inbl, ceph::buffer::list* outbl,
                     Formatter* f) {
  std::ostringstream oss;
  encode_json(name, s, f);
  f->flush(oss);
  int rc = rados.mon_command(oss.str(), inbl, outbl, nullptr);
  return rc;
}

}  // namespace

ceph::io_sequence::tester::SelectSeqRange::SelectSeqRange(po::variables_map& vm)
    : ProgramOptionReader<std::pair<ceph::io_exerciser::Sequence,
                                    ceph::io_exerciser::Sequence>>(vm,
                                                                   "sequence") {
  if (vm.count(option_name)) {
    ceph::io_exerciser::Sequence s =
        static_cast<ceph::io_exerciser::Sequence>(vm["sequence"].as<int>());
    if (s < ceph::io_exerciser::Sequence::SEQUENCE_BEGIN ||
        s >= ceph::io_exerciser::Sequence::SEQUENCE_END) {
      dout(0) << "Sequence argument out of range" << dendl;
      throw po::validation_error(po::validation_error::invalid_option_value);
    }
    ceph::io_exerciser::Sequence e = s;
    force_value = std::make_optional<
        std::pair<ceph::io_exerciser::Sequence, ceph::io_exerciser::Sequence>>(
        std::make_pair(s, ++e));
  }
}

const std::pair<ceph::io_exerciser::Sequence, ceph::io_exerciser::Sequence>
ceph::io_sequence::tester::SelectSeqRange::select() {
  if (force_value.has_value()) {
    return *force_value;
  } else {
    return std::make_pair(ceph::io_exerciser::Sequence::SEQUENCE_BEGIN,
                          ceph::io_exerciser::Sequence::SEQUENCE_END);
  }
}

ceph::io_sequence::tester::SelectErasureTechnique::SelectErasureTechnique(
    ceph::util::random_number_generator<int>& rng,
    po::variables_map& vm,
    std::string_view plugin,
    bool first_use)
    : ProgramOptionGeneratedSelector<std::string>(rng, vm, "technique",
                                                  first_use),
      rng(rng),
      plugin(plugin),
      stable(!vm.contains("allow_unstable_pool_configs") ||
        vm.contains("disable_pool_ec_optimizations")) {}

const std::vector<std::string>
ceph::io_sequence::tester::SelectErasureTechnique::generate_selections() {
  std::vector<std::string> techniques = {};
  if (plugin == "jerasure") {
    techniques.push_back("reed_sol_van");
    if (!stable) {
      techniques.push_back("reed_sol_r6_op");
      techniques.push_back("cauchy_orig");
      techniques.push_back("cauchy_good");
      techniques.push_back("liberation");
      techniques.push_back("blaum_roth");
      techniques.push_back("liber8tion");
    }
  } else if (plugin == "isa") {
    techniques.push_back("reed_sol_van");
    if (!stable) {
      techniques.push_back("cauchy");
    }
  } else if (plugin == "shec") {
    techniques.push_back("single");
    techniques.push_back("multiple");
  }

  return techniques;
}

ceph::io_sequence::tester::lrc::SelectMappingAndLayers::SelectMappingAndLayers(
    ceph::util::random_number_generator<int>& rng,
    po::variables_map& vm,
    bool first_use)
    : rng_seed(rng()),
      mapping_rng{rng_seed},
      layers_rng{rng_seed},
      sma{mapping_rng, vm, "mapping", first_use},
      sly{layers_rng, vm, "layers", first_use} {
  if (sma.isForced() != sly.isForced()) {
    std::string forced_parameter = "mapping";
    std::string unforced_parameter = "layers";
    if (sly.isForced()) {
      std::swap(forced_parameter, unforced_parameter);
    }

    throw std::invalid_argument(fmt::format("The parameter --{} can only be used"
                                " when a --{} parameter is also supplied.",
                                forced_parameter, unforced_parameter));
  }
}

const std::pair<std::string, std::string>
ceph::io_sequence::tester::lrc::SelectMappingAndLayers::select() {
  return std::pair<std::string, std::string>(sma.select(), sly.select());
}

ceph::io_sequence::tester::SelectErasureKM::SelectErasureKM(
    ceph::util::random_number_generator<int>& rng,
    po::variables_map& vm,
    std::string_view plugin,
    const std::optional<std::string>& technique,
    bool first_use)
    : ProgramOptionGeneratedSelector<std::pair<int, int>>(rng, vm, "km",
                                                          first_use),
      rng(rng),
      plugin(plugin),
      technique(technique) {}

const std::vector<std::pair<int, int>>
ceph::io_sequence::tester::SelectErasureKM::generate_selections() {
  std::vector<std::pair<int, int>> selection;

  // Gives different spreads of k and m depending on the plugin and technique
  if (plugin == "isa" || plugin == "clay" ||
      (plugin == "jerasure" &&
       (technique == "reed_sol_van" || technique == "cauchy_orig" ||
        technique == "cauchy_good" || technique == std::nullopt))) {
    for (int m = 1; m <= 3; m++)
      for (int k = 2; k <= 4; k++) selection.push_back({k, m});
  } else if (plugin == "shec" ||
             (plugin == "jerasure" &&
              (technique == "liberation" || technique == "blaum_roth"))) {
    for (int m = 1; m <= 2; m++)
      for (int k = 2; k <= 4; k++) selection.push_back({k, m});
  } else if (plugin == "jerasure" &&
             (technique == "reed_sol_r6_op" || technique == "liber8tion")) {
    for (int k = 2; k <= 4; k++) selection.push_back({k, 2});
  }

  // We want increased chances of these as we will test with c=1 and c=2
  if (plugin == "shec")
    for (int i = 0; i < 2; i++)
      for (int k = 3; k <= 4; k++) selection.push_back({k, 3});

  // Add extra miscelaneous interesting options for testing w values
  if (plugin == "jerasure") {
    if (technique == "liberation" || technique == "blaum_roth")
      // Double chance of chosing to test more different w values
      for (int i = 0; i < 2; i++) selection.push_back({6, 2});

    if (technique == "liber8tion") selection.push_back({2, 2});
  }

  return selection;
}

ceph::io_sequence::tester::jerasure::SelectErasureW::SelectErasureW(
    ceph::util::random_number_generator<int>& rng,
    po::variables_map& vm,
    std::string_view plugin,
    const std::optional<std::string_view>& technique,
    const std::optional<std::pair<int, int>>& km,
    const std::optional<uint64_t>& packetsize,
    bool first_use)
    : ProgramOptionGeneratedSelector<uint64_t>(rng, vm, "w", first_use),
      rng(rng),
      plugin(plugin),
      km(km),
      packetsize(packetsize) {}

const std::vector<uint64_t>
ceph::io_sequence::tester::jerasure::SelectErasureW::generate_selections() {
  std::vector<uint64_t> selection = {};

  if (plugin != "jerasure") {
    return selection;
  }

  if (technique && km && technique == "reed_sol_van" && km->first == 6 &&
      km->second == 3) {
    selection.push_back(16);
    selection.push_back(32);
  }

  if (km && km->first == 6 && km->second == 2) {
    if (technique && technique == "liberation") {
      if (packetsize == 32) selection.push_back(11);
      if (packetsize == 36) selection.push_back(13);
    } else if (technique && technique == "blaum_roth") {
      if (packetsize == 44) selection.push_back(7);
      if (packetsize == 60) selection.push_back(10);
    }
  }

  return selection;
}

ceph::io_sequence::tester::shec::SelectErasureC::SelectErasureC(
    ceph::util::random_number_generator<int>& rng,
    po::variables_map& vm,
    std::string_view plugin,
    const std::optional<std::pair<int, int>>& km,
    bool first_use)
    : ProgramOptionGeneratedSelector<uint64_t>(rng, vm, "c", first_use),
      rng(rng),
      plugin(plugin),
      km(km) {}

const std::vector<uint64_t>
ceph::io_sequence::tester::shec::SelectErasureC::generate_selections() {
  if (plugin != "shec") {
    return {};
  }

  std::vector<uint64_t> selection = {};
  selection.push_back(1);

  if (km && km->first == 3 && km->second >= 3) {
    selection.push_back(2);
  }

  return selection;
}

ceph::io_sequence::tester::jerasure::SelectErasurePacketSize::
    SelectErasurePacketSize(ceph::util::random_number_generator<int>& rng,
                            po::variables_map& vm,
                            std::string_view plugin,
                            const std::optional<std::string_view>& technique,
                            const std::optional<std::pair<int, int>>& km,
                            bool first_use)
    : ProgramOptionGeneratedSelector<uint64_t>(rng, vm, "packetsize",
                                               first_use),
      rng(rng),
      plugin(plugin),
      technique(technique),
      km(km) {}

const std::vector<uint64_t> ceph::io_sequence::tester::jerasure::
    SelectErasurePacketSize::generate_selections() {
  std::vector<uint64_t> selection = {};

  if (plugin != "jerasure") {
    return selection;
  }

  if (technique == "cauchy_orig" ||technique == "cauchy_good" ||
      technique == "liberation" || technique == "blaum_roth" ||
      technique == "liber8tion") {
    selection.push_back(32);
  }

  if (km && technique && technique == "liberation" && km->first == 6 &&
      km->second == 2) {
    selection.push_back(32);
    selection.push_back(36);
  }

  if (km && technique && technique == "blaum_roth" && km->first == 6 &&
      km->second == 2) {
    selection.push_back(44);
    selection.push_back(60);
  }

  if (km && technique && technique == "liber8tion" && km->first == 6 &&
      km->second == 2) {
    selection.push_back(92);
  }

  return selection;
}

ceph::io_sequence::tester::SelectErasureChunkSize::SelectErasureChunkSize(
    ceph::util::random_number_generator<int>& rng,
    po::variables_map& vm,
    ErasureCodeInterfaceRef ec_impl,
    bool first_use)
    : ProgramOptionGeneratedSelector(rng, vm, "chunksize", first_use),
      rng(rng),
      ec_impl(ec_impl) {}

const std::vector<uint64_t>
ceph::io_sequence::tester::SelectErasureChunkSize::generate_selections() {
  int minimum_granularity = ec_impl->get_minimum_granularity();
  int data_chunks = ec_impl->get_data_chunk_count();
  int minimum_chunksize =
      ec_impl->get_chunk_size(minimum_granularity * data_chunks);

  std::vector<uint64_t> choices = {};

  if (4096 % minimum_chunksize == 0) {
    choices.push_back(4096);
  } else {
    choices.push_back(minimum_chunksize * rng(4));
  }

  if ((64 * 1024) % minimum_chunksize == 0) {
    choices.push_back(64 * 1024);
  } else {
    choices.push_back(minimum_chunksize * rng(64));
  }

  if ((256 * 1024) % minimum_chunksize == 0) {
    choices.push_back(256 * 1024);
  } else {
    choices.push_back(minimum_chunksize * rng(256));
  }

  return choices;
}

ceph::io_sequence::tester::SelectErasureProfile::SelectErasureProfile(
    boost::intrusive_ptr<CephContext> cct,
    ceph::util::random_number_generator<int>& rng,
    po::variables_map& vm,
    librados::Rados& rados,
    bool dry_run,
    bool first_use)
    : ProgramOptionReader(vm, "profile"),
      cct(cct),
      rados(rados),
      dry_run(dry_run),
      rng(rng),
      vm(vm),
      first_use(first_use),
      spl{rng, vm, "plugin", true},
      sml{rng, vm, true} {
  if (isForced()) {
    std::array<std::string, 9> disallowed_options = {
        "pool", "km",      "technique", "packetsize", "c",
        "w",    "mapping", "layers",    "chunksize"};

    for (std::string& option : disallowed_options) {
      if (vm.count(option) > 0) {
        throw std::invalid_argument(fmt::format("{} option not allowed "
                                                "if profile is specified",
                                                option));
      }
    }
  }
}

const ceph::io_sequence::tester::Profile
ceph::io_sequence::tester::SelectErasureProfile::select() {
  ceph::io_sequence::tester::Profile profile;

  if (force_value) {
    if (!dry_run) {
      profile = selectExistingProfile(force_value->name);
    }
  } else {
    profile.plugin = spl.select();

    SelectErasureTechnique set{rng, vm, profile.plugin, first_use};
    profile.technique = set.select();

    SelectErasureKM skm{rng, vm, profile.plugin, profile.technique, first_use};
    profile.km = skm.select();

    jerasure::SelectErasurePacketSize sps{
        rng, vm, profile.plugin, profile.technique, profile.km, first_use};
    profile.packet_size = sps.select();

    if (profile.plugin == "jerasure") {
      jerasure::SelectErasureW ssw{rng,
                                   vm,
                                   profile.plugin,
                                   profile.technique,
                                   profile.km,
                                   profile.packet_size,
                                   first_use};
      profile.w = ssw.select();
    } else if (profile.plugin == "shec") {
      shec::SelectErasureC ssc{rng, vm, profile.plugin, profile.km, first_use};
      profile.c = ssc.select();
    } else if (profile.plugin == "lrc") {
      std::pair<std::string, std::string> mappinglayers = sml.select();
      profile.mapping = mappinglayers.first;
      profile.layers = mappinglayers.second;
    }

    ErasureCodeProfile erasure_code_profile;
    erasure_code_profile["plugin"] = std::string(profile.plugin);
    if (profile.km) {
      erasure_code_profile["k"] = std::to_string(profile.km->first);
      erasure_code_profile["m"] = std::to_string(profile.km->second);
    }
    if (profile.technique) {
      erasure_code_profile["technique"] = *profile.technique;
    }
    if (profile.packet_size) {
      erasure_code_profile["packetsize"] = std::to_string(*profile.packet_size);
    }
    if (profile.c) {
      erasure_code_profile["c"] = std::to_string(*profile.c);
    }
    if (profile.w) {
      erasure_code_profile["packetsize"] = std::to_string(*profile.w);
    }
    if (profile.jerasure_per_chunk_alignment) {
      erasure_code_profile["jerasure_per_chunk_alignment"] =
          std::to_string(*profile.jerasure_per_chunk_alignment);
    }
    if (profile.mapping) {
      erasure_code_profile["mapping"] = *profile.mapping;
    }
    if (profile.layers) {
      erasure_code_profile["layers"] = *profile.layers;
    }

    ErasureCodePluginRegistry& instance = ErasureCodePluginRegistry::instance();
    ErasureCodeInterfaceRef ec_impl;
    std::stringstream ss;
    instance.factory(std::string(profile.plugin),
                     cct->_conf.get_val<std::string>("erasure_code_dir"),
                     erasure_code_profile, &ec_impl, &ss);
    if (!ec_impl) {
      throw std::runtime_error(ss.str());
    }

    SelectErasureChunkSize scs{rng, vm, ec_impl, first_use};
    profile.chunk_size = scs.select();

    profile.name = fmt::format("testprofile_pl{}", profile.plugin);
    if (profile.technique) {
      profile.name += fmt::format("_t{}", profile.technique);
    }
    if (profile.km) {
      profile.name +=
          fmt::format("_k{}_m{}", profile.km->first, profile.km->second);
    }
    if (profile.packet_size) {
      profile.name += fmt::format("_ps{}", *profile.packet_size);
    }
    if (profile.c) {
      profile.name += fmt::format("_c{}", *profile.c);
    }
    if (profile.w) {
      profile.name += fmt::format("_w{}", *profile.w);
    }
    if (profile.chunk_size) {
      profile.name += fmt::format("_cs{}", *profile.chunk_size);
    }
    if (profile.mapping) {
      profile.name += fmt::format("_ma{}", *profile.mapping);
    }

    if (!dry_run) {
      create(profile);
    }
  }

  first_use = false;

  return profile;
}

void ceph::io_sequence::tester::SelectErasureProfile::create(
    const ceph::io_sequence::tester::Profile& profile) {
  bufferlist inbl, outbl;
  auto formatter = std::make_unique<JSONFormatter>(false);

  std::vector<std::string> profile_values = {
      fmt::format("plugin={}", profile.plugin)};

  if (profile.km) {
    profile_values.push_back(fmt::format("k={}", profile.km->first));
    profile_values.push_back(fmt::format("m={}", profile.km->second));
  }
  if (profile.technique)
    profile_values.push_back(fmt::format("technique={}", profile.technique));
  if (profile.packet_size)
    profile_values.push_back(fmt::format("packetsize={}", profile.packet_size));
  if (profile.c) profile_values.push_back(fmt::format("c={}", profile.c));
  if (profile.w) profile_values.push_back(fmt::format("w={}", profile.w));
  if (profile.mapping)
    profile_values.push_back(fmt::format("mapping={}", profile.mapping));
  if (profile.layers)
    profile_values.push_back(fmt::format("layers={}", profile.layers));
  if (profile.chunk_size)
    profile_values.push_back(fmt::format("stripe_unit={}", profile.chunk_size));

  // Crush-failure-domain only seems to be taken into account when specifying
  // k and m values in LRC, so we set a crush step to do the same, which is
  // what LRC does under the covers
  if (profile.plugin == "lrc")
    profile_values.push_back("crush-steps=[[\"chooseleaf\",\"osd\",0]]");
  else
    profile_values.push_back("crush-failure-domain=osd");

  bool force =
      profile.chunk_size.has_value() && (*(profile.chunk_size) % 4096 != 0);
  ceph::messaging::osd::OSDECProfileSetRequest ec_profile_set_request{
      profile.name, profile_values, force};
  int rc =
      send_mon_command(ec_profile_set_request, rados, "OSDECProfileSetRequest",
                       inbl, &outbl, formatter.get());
  ceph_assert(rc == 0);
}

const ceph::io_sequence::tester::Profile
ceph::io_sequence::tester::SelectErasureProfile::selectExistingProfile(
    const std::string& profile_name) {
  int rc;
  bufferlist inbl, outbl;
  auto formatter = std::make_shared<JSONFormatter>(false);

  ceph::messaging::osd::OSDECProfileGetRequest ec_profile_get_request{
      profile_name};
  rc = send_mon_command(ec_profile_get_request, rados, "OSDECProfileGetRequest",
                        inbl, &outbl, formatter.get());
  ceph_assert(rc == 0);

  JSONParser p;
  bool success = p.parse(outbl.c_str(), outbl.length());
  ceph_assert(success);

  ceph::messaging::osd::OSDECProfileGetReply reply;
  reply.decode_json(&p);

  ceph::io_sequence::tester::Profile profile{};
  profile.name = profile_name;
  profile.plugin = reply.plugin;
  profile.km = {reply.k, reply.m};
  profile.technique = reply.technique->c_str();
  profile.packet_size = reply.packetsize;
  profile.c = reply.c;
  profile.w = reply.w;
  profile.mapping = reply.mapping;
  profile.layers = reply.layers;

  return profile;
}

ceph::io_sequence::tester::SelectErasurePool::SelectErasurePool(
    boost::intrusive_ptr<CephContext> cct,
    ceph::util::random_number_generator<int>& rng,
    po::variables_map& vm,
    librados::Rados& rados,
    bool dry_run,
    bool allow_pool_autoscaling,
    bool allow_pool_balancer,
    bool allow_pool_deep_scrubbing,
    bool allow_pool_scrubbing,
    bool test_recovery,
    bool disable_pool_ec_optimizations)
    : ProgramOptionReader<std::string>(vm, "pool"),
      rados(rados),
      dry_run(dry_run),
      allow_pool_autoscaling(allow_pool_autoscaling),
      allow_pool_balancer(allow_pool_balancer),
      allow_pool_deep_scrubbing(allow_pool_deep_scrubbing),
      allow_pool_scrubbing(allow_pool_scrubbing),
      test_recovery(test_recovery),
      disable_pool_ec_optimizations(disable_pool_ec_optimizations),
      first_use(true),
      sep{cct, rng, vm, rados, dry_run, first_use} {
  if (isForced()) {
    std::array<std::string, 9> disallowed_options = {
        "profile", "km",      "technique", "packetsize", "c",
        "w",       "mapping", "layers",    "chunksize"};

    for (std::string& option : disallowed_options) {
      if (vm.count(option) > 0) {
        throw std::invalid_argument(fmt::format("{} option not allowed "
                                    "if pool is specified",
                                    option));
      }
    }
  }
}

const std::string ceph::io_sequence::tester::SelectErasurePool::select() {
  first_use = true;

  std::string created_pool_name = "";
  if (!dry_run) {
    if (isForced()) {
      int rc;
      bufferlist inbl, outbl;
      auto formatter = std::make_shared<JSONFormatter>(false);

      ceph::messaging::osd::OSDPoolGetRequest osdPoolGetRequest{*force_value};
      rc = send_mon_command(osdPoolGetRequest, rados, "OSDPoolGetRequest", inbl,
                            &outbl, formatter.get());
      ceph_assert(rc == 0);

      JSONParser p;
      bool success = p.parse(outbl.c_str(), outbl.length());
      ceph_assert(success);

      ceph::messaging::osd::OSDPoolGetReply pool_get_reply;
      pool_get_reply.decode_json(&p);

      profile = sep.selectExistingProfile(pool_get_reply.erasure_code_profile);
    } else {
      created_pool_name = create();
    }

    if (!dry_run) {
      configureServices(allow_pool_autoscaling, allow_pool_balancer,
                        allow_pool_deep_scrubbing, allow_pool_scrubbing,
                        test_recovery);
    }
  }

  return force_value.value_or(created_pool_name);
}

std::string ceph::io_sequence::tester::SelectErasurePool::create() {
  int rc;
  bufferlist inbl, outbl;
  auto formatter = std::make_shared<JSONFormatter>(false);

  std::string pool_name;
  profile = sep.select();
  pool_name = fmt::format("testpool-pr{}{}", profile->name,
    disable_pool_ec_optimizations?"_no_ec_opt":"");

  ceph::messaging::osd::OSDECPoolCreateRequest pool_create_request{
      pool_name, "erasure", 8, 8, profile->name};
  rc = send_mon_command(pool_create_request, rados, "OSDECPoolCreateRequest",
                        inbl, &outbl, formatter.get());
  ceph_assert(rc == 0);

  return pool_name;
}

void ceph::io_sequence::tester::SelectErasurePool::configureServices(
    bool allow_pool_autoscaling,
    bool allow_pool_balancer,
    bool allow_pool_deep_scrubbing,
    bool allow_pool_scrubbing,
    bool test_recovery) {
  int rc;
  bufferlist inbl, outbl;
  auto formatter = std::make_shared<JSONFormatter>(false);

  if (!allow_pool_autoscaling) {
    ceph::messaging::osd::OSDSetRequest no_autoscale_request{"noautoscale",
                                                              std::nullopt};
    rc = send_mon_command(no_autoscale_request, rados, "OSDSetRequest", inbl,
                          &outbl, formatter.get());
    ceph_assert(rc == 0);
  }

  if (!allow_pool_balancer) {
    ceph::messaging::balancer::BalancerOffRequest balancer_off_request;
    rc = send_mon_command(balancer_off_request, rados, "BalancerOffRequest", inbl,
                          &outbl, formatter.get());
    ceph_assert(rc == 0);

    ceph::messaging::balancer::BalancerStatusRequest balancer_status_request;
    rc = send_mon_command(balancer_status_request, rados, "BalancerStatusRequest",
                          inbl, &outbl, formatter.get());
    ceph_assert(rc == 0);

    JSONParser p;
    bool success = p.parse(outbl.c_str(), outbl.length());
    ceph_assert(success);

    ceph::messaging::balancer::BalancerStatusReply balancer_status_reply;
    balancer_status_reply.decode_json(&p);
    ceph_assert(!balancer_status_reply.active);
  }

  if (!allow_pool_deep_scrubbing) {
    ceph::messaging::osd::OSDSetRequest no_deep_scrub_request{"nodeep-scrub",
                                                              std::nullopt};
    rc = send_mon_command(no_deep_scrub_request, rados, "setNoDeepScrubRequest",
                          inbl, &outbl, formatter.get());
    ceph_assert(rc == 0);
  }

  if (!allow_pool_scrubbing) {
    ceph::messaging::osd::OSDSetRequest no_scrub_request{"noscrub",
                                                          std::nullopt};
    rc = send_mon_command(no_scrub_request, rados, "OSDSetRequest", inbl,
                          &outbl, formatter.get());
    ceph_assert(rc == 0);
  }

  if (test_recovery) {
    ceph::messaging::config::ConfigSetRequest bluestore_debug_request{
        "global", "bluestore_debug_inject_read_err", "true", std::nullopt};
    rc = send_mon_command(bluestore_debug_request, rados,
                          "ConfigSetRequest", inbl, &outbl, formatter.get());
    ceph_assert(rc == 0);

    ceph::messaging::config::ConfigSetRequest max_markdown_request{
        "global", "osd_max_markdown_count", "99999999", std::nullopt};
    rc = send_mon_command(max_markdown_request, rados,
                          "ConfigSetRequest", inbl, &outbl, formatter.get());
    ceph_assert(rc == 0);
  }
}

ceph::io_sequence::tester::TestObject::TestObject(
    const std::string oid, librados::Rados& rados,
    boost::asio::io_context& asio, SelectBlockSize& sbs, SelectErasurePool& spo,
    SelectObjectSize& sos, SelectNumThreads& snt, SelectSeqRange& ssr,
    ceph::util::random_number_generator<int>& rng, ceph::mutex& lock,
    ceph::condition_variable& cond, bool dryrun, bool verbose,
    std::optional<int> seqseed, bool testrecovery)
    : rng(rng), verbose(verbose), seqseed(seqseed), testrecovery(testrecovery) {
  if (dryrun) {
    exerciser_model = std::make_unique<ceph::io_exerciser::ObjectModel>(
        oid, sbs.select(), rng());
  } else {
    const std::string pool = spo.select();
    if (!dryrun) {
      ceph_assert(spo.getProfile());
      pool_km = spo.getProfile()->km;
      if (spo.getProfile()->mapping && spo.getProfile()->layers) {
        pool_mappinglayers = {*spo.getProfile()->mapping,
                             *spo.getProfile()->layers};
      }
    }

    int threads = snt.select();

    bufferlist inbl, outbl;
    auto formatter = std::make_unique<JSONFormatter>(false);

    std::optional<std::vector<int>> cached_shard_order = std::nullopt;

    if (!spo.get_allow_pool_autoscaling() && !spo.get_allow_pool_balancer() &&
        !spo.get_allow_pool_deep_scrubbing() &&
        !spo.get_allow_pool_scrubbing()) {
      ceph::messaging::osd::OSDMapRequest osdMapRequest{pool, oid, ""};
      int rc = send_mon_command(osdMapRequest, rados, "OSDMapRequest", inbl,
                                &outbl, formatter.get());
      ceph_assert(rc == 0);

      JSONParser p;
      bool success = p.parse(outbl.c_str(), outbl.length());
      ceph_assert(success);

      ceph::messaging::osd::OSDMapReply reply{};
      reply.decode_json(&p);
      cached_shard_order = reply.acting;
    }

    exerciser_model = std::make_unique<ceph::io_exerciser::RadosIo>(
        rados, asio, pool, oid, cached_shard_order, sbs.select(), rng(),
        threads, lock, cond, spo.get_allow_pool_ec_optimizations());
    dout(0) << "= " << oid << " pool=" << pool << " threads=" << threads
            << " blocksize=" << exerciser_model->get_block_size() << " ="
            << dendl;
  }
  obj_size_range = sos.select();
  seq_range = ssr.select();
  curseq = seq_range.first;

  if (testrecovery) {
    seq = ceph::io_exerciser::EcIoSequence::generate_sequence(
        curseq, obj_size_range, pool_km, pool_mappinglayers,
        seqseed.value_or(rng()));
  } else {
    seq = ceph::io_exerciser::IoSequence::generate_sequence(
        curseq, obj_size_range, seqseed.value_or(rng()));
  }

  op = seq->next();
  done = false;
  dout(0) << "== " << exerciser_model->get_oid() << " " << curseq << " "
          << seq->get_name_with_seqseed() << " ==" << dendl;
}

bool ceph::io_sequence::tester::TestObject::readyForIo() {
  return exerciser_model->readyForIoOp(*op);
}

bool ceph::io_sequence::tester::TestObject::next() {
  if (!done) {
    if (verbose) {
      dout(0) << exerciser_model->get_oid() << " Step " << seq->get_step()
              << ": " << op->to_string(exerciser_model->get_block_size())
              << dendl;
    } else {
      dout(5) << exerciser_model->get_oid() << " Step " << seq->get_step()
              << ": " << op->to_string(exerciser_model->get_block_size())
              << dendl;
    }
    exerciser_model->applyIoOp(*op);
    if (op->getOpType() == ceph::io_exerciser::OpType::Done) {
      curseq = seq->getNextSupportedSequenceId();
      if (curseq >= seq_range.second) {
        done = true;
        dout(0) << exerciser_model->get_oid()
                << " Number of IOs = " << exerciser_model->get_num_io()
                << dendl;
      } else {
        if (testrecovery) {
          seq = ceph::io_exerciser::EcIoSequence::generate_sequence(
              curseq, obj_size_range, pool_km, pool_mappinglayers,
              seqseed.value_or(rng()));
        } else {
          seq = ceph::io_exerciser::IoSequence::generate_sequence(
              curseq, obj_size_range, seqseed.value_or(rng()));
        }

        dout(0) << "== " << exerciser_model->get_oid() << " " << curseq << " "
                << seq->get_name_with_seqseed() << " ==" << dendl;
        op = seq->next();
      }
    } else {
      op = seq->next();
    }
  }
  return done;
}

bool ceph::io_sequence::tester::TestObject::finished() { return done; }

int ceph::io_sequence::tester::TestObject::get_num_io() {
  return exerciser_model->get_num_io();
}

ceph::io_sequence::tester::TestRunner::TestRunner(
    boost::intrusive_ptr<CephContext> cct,
    po::variables_map& vm,
    librados::Rados& rados)
    : rados(rados),
      seed(vm.contains("seed") ? vm["seed"].as<int>() : time(nullptr)),
      rng(ceph::util::random_number_generator<int>(seed)),
      sbs{rng, vm, "blocksize", true},
      sos{rng, vm, "objectsize", true},
      spo{cct,
          rng,
          vm,
          rados,
          vm.contains("dryrun"),
          vm.contains("allow_pool_autoscaling"),
          vm.contains("allow_pool_balancer"),
          vm.contains("allow_pool_deep_scrubbing"),
          vm.contains("allow_pool_scrubbing"),
          vm.contains("test_recovery"),
          vm.contains("disable_pool_ec_optimizations")},
      snt{rng, vm, "threads", true},
      ssr{vm} {
  dout(0) << "Test using seed " << seed << dendl;

  verbose = vm.contains("verbose");
  dryrun = vm.contains("dryrun");

  seqseed = std::nullopt;
  if (vm.contains("seqseed")) {
    seqseed = vm["seqseed"].as<int>();
  }
  num_objects = vm["parallel"].as<int>();
  object_name = vm["object"].as<std::string>();
  interactive = vm.contains("interactive");
  testrecovery = vm.contains("testrecovery");

  allow_pool_autoscaling = vm.contains("allow_pool_autoscaling");
  allow_pool_balancer = vm.contains("allow_pool_balancer");
  allow_pool_deep_scrubbing = vm.contains("allow_pool_deep_scrubbing");
  allow_pool_scrubbing = vm.contains("allow_pool_scrubbing");
  disable_pool_ec_optimizations = vm.contains("disable_pool_ec_optimizations");

  if (!dryrun) {
    guard.emplace(boost::asio::make_work_guard(asio));
    thread = make_named_thread("io_thread", [&asio = asio] { asio.run(); });
  }

  show_help = vm.contains("help");
  show_sequence = vm.contains("listsequence");
}

ceph::io_sequence::tester::TestRunner::~TestRunner() {
  if (!dryrun) {
    guard = std::nullopt;
    asio.stop();
    thread.join();
    rados.shutdown();
  }
}

void ceph::io_sequence::tester::TestRunner::help() {
  std::cout << get_options_description() << std::endl;
  for (auto line : usage) {
    std::cout << line << std::endl;
  }
}

void ceph::io_sequence::tester::TestRunner::list_sequence(bool testrecovery) {
  // List seqeunces
  std::pair<int, int> obj_size_range = sos.select();
  ceph::io_exerciser::Sequence s = ceph::io_exerciser::Sequence::SEQUENCE_BEGIN;
  std::unique_ptr<ceph::io_exerciser::IoSequence> seq;
  if (testrecovery) {
    std::optional<ceph::io_sequence::tester::Profile> profile =
        spo.getProfile();
    std::optional<std::pair<int, int>> km;
    std::optional<std::pair<std::string_view, std::string_view>> mappinglayers;
    if (profile) {
      km = profile->km;
      if (profile->mapping && profile->layers) {
        mappinglayers = {*spo.getProfile()->mapping, *spo.getProfile()->layers};
      }
    }
    seq = ceph::io_exerciser::EcIoSequence::generate_sequence(
        s, obj_size_range, km, mappinglayers, seqseed.value_or(rng()));
  } else {
    seq = ceph::io_exerciser::IoSequence::generate_sequence(
        s, obj_size_range, seqseed.value_or(rng()));
  }

  do {
    dout(0) << s << " " << seq->get_name_with_seqseed() << dendl;
    s = seq->getNextSupportedSequenceId();
  } while (s != ceph::io_exerciser::Sequence::SEQUENCE_END);
}

void ceph::io_sequence::tester::TestRunner::clear_tokens() {
  tokens = split.end();
}

std::string ceph::io_sequence::tester::TestRunner::get_token(bool allow_eof) {
  while (line.empty() || tokens == split.end()) {
    if (!std::getline(std::cin, line)) {
      if (allow_eof) {
        return "done";
      }
      throw std::runtime_error("End of input");
    }
    if (line.starts_with('#')) {
      dout(0) << line << dendl;
      continue;
    }
    split = ceph::split(line);
    tokens = split.begin();
  }
  return std::string(*tokens++);
}

std::optional<std::string>
ceph::io_sequence::tester::TestRunner::get_optional_token() {
  std::optional<std::string> ret = std::nullopt;
  if (tokens != split.end()) {
    ret = std::string(*tokens++);
  }
  return ret;
}

uint64_t ceph::io_sequence::tester::TestRunner::get_numeric_token() {
  std::string parse_error;
  std::string token = get_token();
  uint64_t num = strict_iecstrtoll(token, &parse_error);
  if (!parse_error.empty()) {
    throw std::runtime_error("Invalid number " + token);
  }
  return num;
}

std::optional<uint64_t>
ceph::io_sequence::tester::TestRunner::get_optional_numeric_token() {
  std::string parse_error;
  std::optional<std::string> token = get_optional_token();
  if (token) {
    uint64_t num = strict_iecstrtoll(*token, &parse_error);
    if (!parse_error.empty()) {
      throw std::runtime_error("Invalid number " + *token);
    }
    return num;
  }

  return std::optional<uint64_t>(std::nullopt);
}

bool ceph::io_sequence::tester::TestRunner::run_test() {
  if (show_help) {
    help();
    return true;
  } else if (show_sequence) {
    list_sequence(testrecovery);
    return true;
  } else if (interactive) {
    return run_interactive_test();
  } else {
    return run_automated_test();
  }
}

bool ceph::io_sequence::tester::TestRunner::run_interactive_test() {
  bool done = false;
  std::unique_ptr<ceph::io_exerciser::IoOp> ioop;
  std::unique_ptr<ceph::io_exerciser::Model> model;

  if (dryrun) {
    model = std::make_unique<ceph::io_exerciser::ObjectModel>(
        object_name, sbs.select(), rng());
  } else {
    const std::string pool = spo.select();

    bufferlist inbl, outbl;
    auto formatter = std::make_unique<JSONFormatter>(false);

    ceph::messaging::osd::OSDMapRequest osd_map_request{pool, object_name, ""};
    int rc = send_mon_command(osd_map_request, rados, "OSDMapRequest", inbl,
                              &outbl, formatter.get());
    ceph_assert(rc == 0);

    JSONParser p;
    bool success = p.parse(outbl.c_str(), outbl.length());
    ceph_assert(success);

    ceph::messaging::osd::OSDMapReply osd_map_reply{};
    osd_map_reply.decode_json(&p);

    model = std::make_unique<ceph::io_exerciser::RadosIo>(
        rados, asio, pool, object_name, osd_map_reply.acting, sbs.select(), rng(),
        1,  // 1 thread
        lock, cond,
        spo.get_allow_pool_ec_optimizations());
  }

  while (!done) {
    const std::string op = get_token(true);
    if (op == "done" || op == "q" || op == "quit") {
      ioop = ceph::io_exerciser::DoneOp::generate();
    } else if (op == "sleep") {
      uint64_t duration = get_numeric_token();
      dout(0) << "Sleep " << duration << dendl;
      sleep(duration);
    } else if (op == "create") {
      ioop = ceph::io_exerciser::CreateOp::generate(get_numeric_token());
    } else if (op == "remove" || op == "delete") {
      ioop = ceph::io_exerciser::RemoveOp::generate();
    } else if (op == "read") {
      uint64_t offset = get_numeric_token();
      uint64_t length = get_numeric_token();
      ioop = ceph::io_exerciser::SingleReadOp::generate(offset, length);
    } else if (op == "read2") {
      uint64_t offset1 = get_numeric_token();
      uint64_t length1 = get_numeric_token();
      uint64_t offset2 = get_numeric_token();
      uint64_t length2 = get_numeric_token();
      ioop = DoubleReadOp::generate(offset1, length1, offset2, length2);
    } else if (op == "read3") {
      uint64_t offset1 = get_numeric_token();
      uint64_t length1 = get_numeric_token();
      uint64_t offset2 = get_numeric_token();
      uint64_t length2 = get_numeric_token();
      uint64_t offset3 = get_numeric_token();
      uint64_t length3 = get_numeric_token();
      ioop = TripleReadOp::generate(offset1, length1, offset2, length2, offset3,
                                    length3);
    } else if (op == "write") {
      uint64_t offset = get_numeric_token();
      uint64_t length = get_numeric_token();
      ioop = SingleWriteOp::generate(offset, length);
    } else if (op == "write2") {
      uint64_t offset1 = get_numeric_token();
      uint64_t length1 = get_numeric_token();
      uint64_t offset2 = get_numeric_token();
      uint64_t length2 = get_numeric_token();
      ioop = DoubleWriteOp::generate(offset1, length1, offset2, length2);
    } else if (op == "write3") {
      uint64_t offset1 = get_numeric_token();
      uint64_t length1 = get_numeric_token();
      uint64_t offset2 = get_numeric_token();
      uint64_t length2 = get_numeric_token();
      uint64_t offset3 = get_numeric_token();
      uint64_t length3 = get_numeric_token();
      ioop = TripleWriteOp::generate(offset1, length1, offset2, length2,
                                     offset3, length3);
    } else if (op == "append") {
      uint64_t length = get_numeric_token();
      ioop = SingleAppendOp::generate(length);
    } else if (op == "truncate") {
      ioop = TruncateOp::generate(get_numeric_token());
    } else if (op == "failedwrite") {
      uint64_t offset = get_numeric_token();
      uint64_t length = get_numeric_token();
      ioop = SingleFailedWriteOp::generate(offset, length);
    } else if (op == "failedwrite2") {
      uint64_t offset1 = get_numeric_token();
      uint64_t length1 = get_numeric_token();
      uint64_t offset2 = get_numeric_token();
      uint64_t length2 = get_numeric_token();
      ioop = DoubleFailedWriteOp::generate(offset1, length1, offset2, length2);
    } else if (op == "failedwrite3") {
      uint64_t offset1 = get_numeric_token();
      uint64_t length1 = get_numeric_token();
      uint64_t offset2 = get_numeric_token();
      uint64_t length2 = get_numeric_token();
      uint64_t offset3 = get_numeric_token();
      uint64_t length3 = get_numeric_token();
      ioop = TripleFailedWriteOp::generate(offset1, length1, offset2, length2,
                                           offset3, length3);
    } else if (op == "injecterror") {
      std::string inject_type = get_token();
      int shard = get_numeric_token();
      std::optional<int> type = get_optional_numeric_token();
      std::optional<int> when = get_optional_numeric_token();
      std::optional<int> duration = get_optional_numeric_token();
      if (inject_type == "read") {
        ioop = ceph::io_exerciser::InjectReadErrorOp::generate(shard, type,
                                                               when, duration);
      } else if (inject_type == "write") {
        ioop = ceph::io_exerciser::InjectWriteErrorOp::generate(shard, type,
                                                                when, duration);
      } else {
        clear_tokens();
        ioop.reset();
        dout(0) << fmt::format("Invalid error inject {}. No action performed.",
                               inject_type)
                << dendl;
      }
    } else if (op == "clearinject") {
      std::string inject_type = get_token();
      int shard = get_numeric_token();
      std::optional<int> type = get_optional_numeric_token();
      if (inject_type == "read") {
        ioop =
            ceph::io_exerciser::ClearReadErrorInjectOp::generate(shard, type);
      } else if (inject_type == "write") {
        ioop =
            ceph::io_exerciser::ClearWriteErrorInjectOp::generate(shard, type);
      } else {
        clear_tokens();
        ioop.reset();
        dout(0) << fmt::format("Invalid error inject {}. No action performed.",
                               inject_type)
                << dendl;
      }
    } else {
      clear_tokens();
      ioop.reset();
      dout(0) << fmt::format("Invalid op {}. No action performed.", op)
              << dendl;
    }
    if (ioop) {
      dout(0) << ioop->to_string(model->get_block_size()) << dendl;
      model->applyIoOp(*ioop);
      done = ioop->getOpType() == ceph::io_exerciser::OpType::Done;
      if (!done) {
        ioop = ceph::io_exerciser::BarrierOp::generate();
        model->applyIoOp(*ioop);
      }
    }
  }

  return true;
}

bool ceph::io_sequence::tester::TestRunner::run_automated_test() {
  // Create a test for each object
  std::vector<std::shared_ptr<ceph::io_sequence::tester::TestObject>>
      test_objects;

  for (int obj = 0; obj < num_objects; obj++) {
    std::string name;
    if (obj == 0) {
      name = object_name;
    } else {
      name = object_name + std::to_string(obj);
    }
    try {
      test_objects.push_back(
          std::make_shared<ceph::io_sequence::tester::TestObject>(
              name, rados, asio, sbs, spo, sos, snt, ssr, rng, lock, cond,
              dryrun, verbose, seqseed, testrecovery));
    }
    catch (const std::runtime_error &e) {
      std::cerr << "Error: " << e.what() << std::endl;
      return false;
    }
  }
  if (!dryrun) {
    rados.wait_for_latest_osdmap();
  }

  // Main loop of test - while not all test objects have finished
  // check to see if any are able to start a new I/O. If all test
  // objects are waiting for I/O to complete then wait on a cond
  // that is signalled each time an I/O completes

  bool started_io = true;
  bool need_wait = true;
  while (started_io || need_wait) {
    started_io = false;
    need_wait = false;
    for (auto obj = test_objects.begin(); obj != test_objects.end(); ++obj) {
      std::shared_ptr<ceph::io_sequence::tester::TestObject> to = *obj;
      if (!to->finished()) {
        lock.lock();
        bool ready = to->readyForIo();
        lock.unlock();
        if (ready) {
          to->next();
          started_io = true;
        } else {
          need_wait = true;
        }
      }
    }
    if (!started_io && need_wait) {
      std::unique_lock l(lock);
      // Recheck with lock incase anything has changed
      for (auto obj = test_objects.begin(); obj != test_objects.end(); ++obj) {
        std::shared_ptr<ceph::io_sequence::tester::TestObject> to = *obj;
        if (!to->finished()) {
          need_wait = !to->readyForIo();
          if (!need_wait) {
            break;
          }
        }
      }
      need_wait = true;
    }
  }

  int total_io = 0;
  for (auto obj = test_objects.begin(); obj != test_objects.end(); ++obj) {
    std::shared_ptr<ceph::io_sequence::tester::TestObject> to = *obj;
    total_io += to->get_num_io();
    ceph_assert(to->finished());
  }
  dout(0) << "Total number of IOs = " << total_io << dendl;

  return true;
}

int main(int argc, char** argv) {
  auto args = argv_to_vec(argc, argv);
  env_to_vec(args);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct.get());

  po::variables_map vm;
  int rc = parse_io_seq_options(vm, argc, argv);
  if (rc != 0) {
    return rc;
  }

  librados::Rados rados;
  if (!vm.contains("dryrun")) {
    rc = rados.init_with_context(g_ceph_context);
    ceph_assert(rc == 0);
    rc = rados.connect();
    ceph_assert(rc == 0);
  }

  std::unique_ptr<ceph::io_sequence::tester::TestRunner> runner;
  try {
    runner =
        std::make_unique<ceph::io_sequence::tester::TestRunner>(cct, vm, rados);
  } catch (const po::error& e) {
    return 1;
  } catch (const std::invalid_argument& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }

  if (!runner->run_test()) {
    return 1;
  }

  return 0;
}
