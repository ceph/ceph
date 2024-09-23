#include <utility>

#include "include/random.h"

#include "global/global_init.h"
#include "global/global_context.h"

#include "common/io_exerciser/IoOp.h"
#include "common/io_exerciser/IoSequence.h"
#include "common/io_exerciser/Model.h"

#include "librados/librados_asio.h"

#include <boost/program_options.hpp>

/* Overview
 *
 * class ProgramOptionSelector
 *   Base class for selector objects below with common code for 
 *   selecting options
 * 
 * class SelectObjectSize
 *   Selects min and max object sizes for a test
 *
 * class SelectErasureKM
 *   Selects an EC k and m value for a test
 * 
 * class SelectErasurePlugin
 *   Selects an plugin for a test
 * 
 * class SelectECPool
 *   Selects an EC pool (plugin,k and m) for a test. Also creates the
 *   pool as well.
 *
 * class SelectBlockSize
 *   Selects a block size for a test
 *
 * class SelectNumThreads
 *   Selects number of threads for a test
 *
 * class SelectSeqRange
 *   Selects a sequence or range of sequences for a test
 *
 * class TestObject
 *   Runs a test against an object, generating IOSequence
 *   and applying them to an IoExerciser
 *
 * main
 *   Run sequences of I/O with data integrity checking to
 *   one or more objects in parallel. Without arguments
 *   runs a default configuration against one object.
 *   Command arguments can select alternative
 *   configurations. Alternatively running against
 *   multiple objects with --objects <n> will select a
 *   random configuration for all but the first object.
 */

namespace po = boost::program_options;

namespace ceph
{
  namespace io_sequence::tester
  {
    // Choices for min and max object size
    inline constexpr size_t objectSizeSize = 10;
    inline constexpr std::array<std::pair<int,int>,objectSizeSize> 
                        objectSizeChoices = {{
      {1,32},  // Default - best for boundary checking
      {12,14},
      {28,30},
      {36,38},
      {42,44},
      {52,54},
      {66,68},
      {72,74},
      {83,83},
      {97,97}
    }};

    // Choices for block size
    inline constexpr int blockSizeSize = 5;
    inline constexpr std::array<uint64_t, blockSizeSize> blockSizeChoices = {{
      2048, // Default - test boundaries for EC 4K chunk size
      512,
      3767,
      4096,
      32768
    }};

    // Choices for number of threads
    inline constexpr int threadArraySize = 4;
    inline constexpr std::array<int, threadArraySize> threadCountChoices = {{
      1, // Default
      2,
      4,
      8
    }};

    // Choices for EC k+m profile
    inline constexpr int kmSize = 6;
    inline constexpr std::array<std::pair<int,int>, kmSize> kmChoices = {{
      {2,2}, // Default - reasonable coverage
      {2,1},
      {2,3},
      {3,2},
      {4,2},
      {5,1}
    }};

    // Choices for EC chunk size
    inline constexpr int chunkSizeSize = 3;
    inline constexpr std::array<uint64_t, chunkSizeSize> chunkSizeChoices = {{
      4*1024,
      64*1024,
      256*1024
    }};

    // Choices for plugin
    inline constexpr int pluginListSize = 2;
    inline constexpr std::array<std::string_view,
                                pluginListSize> pluginChoices = {{
      "jerasure",
      "isa"
    }};

    inline constexpr std::array<std::pair<ceph::io_exerciser::Sequence,
                                          ceph::io_exerciser::Sequence>, 
                                0> sequencePairs = {{}};

    inline constexpr std::array<std::string, 0> poolChoices = {{}};

    template <typename T, int N, const std::array<T, N>& Ts>
    class ProgramOptionSelector
    {
    public:
      ProgramOptionSelector(ceph::util::random_number_generator<int>& rng,
                            po::variables_map vm,
                            const std::string& option_name,
                            bool set_forced,
                            bool select_first
                           );
      virtual ~ProgramOptionSelector() = default;
      bool isForced();
      virtual const T choose();

    protected:
      ceph::util::random_number_generator<int>& rng;
      static constexpr std::array<T, N> choices = Ts;

      std::optional<T> force_value;
      std::optional<T> first_value;

      std::string option_name;
    };

    class SelectObjectSize
      : public ProgramOptionSelector<std::pair<int, int>,
                                     io_sequence::tester::objectSizeSize,
                                     io_sequence::tester::objectSizeChoices>
    {
    public:
      SelectObjectSize(ceph::util::random_number_generator<int>& rng,
                      po::variables_map vm);  
    };

    class SelectBlockSize
      : public ProgramOptionSelector<uint64_t, 
                                     io_sequence::tester::blockSizeSize,
                                     io_sequence::tester::blockSizeChoices>
    {
    public:
      SelectBlockSize(ceph::util::random_number_generator<int>& rng,
                      po::variables_map vm);
    };

    class SelectNumThreads
      : public ProgramOptionSelector<int, 
                                     io_sequence::tester::threadArraySize,
                                     io_sequence::tester::threadCountChoices>
    {
    public:
      SelectNumThreads(ceph::util::random_number_generator<int>& rng,
                       po::variables_map vm);
    };

    class SelectSeqRange
      : public ProgramOptionSelector<std::pair<ceph::io_exerciser::Sequence,
                                               ceph::io_exerciser::Sequence>,
                                     0, io_sequence::tester::sequencePairs>
    {
    public:
      SelectSeqRange(ceph::util::random_number_generator<int>& rng,
                     po::variables_map vm);

      const std::pair<ceph::io_exerciser::Sequence,
                      ceph::io_exerciser::Sequence> choose() override;
    };

    class SelectErasureKM
      : public ProgramOptionSelector<std::pair<int,int>,
                                     io_sequence::tester::kmSize,
                                     io_sequence::tester::kmChoices>
    {
    public:
      SelectErasureKM(ceph::util::random_number_generator<int>& rng,
                      po::variables_map vm);
    };

    class SelectErasurePlugin
      : public ProgramOptionSelector<std::string_view,
                                     io_sequence::tester::pluginListSize,
                                     io_sequence::tester::pluginChoices>
        {
    public:
      SelectErasurePlugin(ceph::util::random_number_generator<int>& rng,
                          po::variables_map vm);
    };

    class SelectErasureChunkSize 
      : public ProgramOptionSelector<uint64_t, 
                                     io_sequence::tester::chunkSizeSize,
                                     io_sequence::tester::chunkSizeChoices>
    {
    public:
      SelectErasureChunkSize(ceph::util::random_number_generator<int>& rng, po::variables_map vm);
    };

    class SelectECPool
      : public ProgramOptionSelector<std::string,
                                     0,
                                     io_sequence::tester::poolChoices>
    { 
    public:
      SelectECPool(ceph::util::random_number_generator<int>& rng,
                   po::variables_map vm,
                   librados::Rados& rados,
                   bool dry_run);
      const std::string choose() override;

    private:
      void create_pool(librados::Rados& rados,
                       const std::string& pool_name,
                       const std::string& plugin,
                       uint64_t chunk_size,
                       int k, int m);

    protected:
      librados::Rados& rados;
      bool dry_run;
      
      SelectErasureKM skm;
      SelectErasurePlugin spl;
      SelectErasureChunkSize scs;
    };

    class TestObject
    {
    public:
      TestObject( const std::string oid,
                  librados::Rados& rados,
                  boost::asio::io_context& asio,
                  ceph::io_sequence::tester::SelectBlockSize& sbs,
                  ceph::io_sequence::tester::SelectECPool& spl,
                  ceph::io_sequence::tester::SelectObjectSize& sos,
                  ceph::io_sequence::tester::SelectNumThreads& snt,
                  ceph::io_sequence::tester::SelectSeqRange& ssr,
                  ceph::util::random_number_generator<int>& rng,
                  ceph::mutex& lock,
                  ceph::condition_variable& cond,
                  bool dryrun,
                  bool verbose,
                  std::optional<int>  seqseed);
      
      int get_num_io();
      bool readyForIo();
      bool next();
      bool finished();

    protected:
      std::unique_ptr<ceph::io_exerciser::Model> exerciser_model;
      std::pair<int,int> obj_size_range;
      std::pair<ceph::io_exerciser::Sequence,
                ceph::io_exerciser::Sequence> seq_range;
      ceph::io_exerciser::Sequence curseq;
      std::unique_ptr<ceph::io_exerciser::IoSequence> seq;
      std::unique_ptr<ceph::io_exerciser::IoOp> op;
      bool done;
      ceph::util::random_number_generator<int>& rng;
      bool verbose;
      std::optional<int> seqseed;
    };

    class TestRunner
    {
    public:
      TestRunner(po::variables_map& vm, librados::Rados& rados);
      ~TestRunner();

      bool run_test();

    private:
      librados::Rados& rados;
      int seed;
      ceph::util::random_number_generator<int> rng;

      ceph::io_sequence::tester::SelectBlockSize sbs;
      ceph::io_sequence::tester::SelectObjectSize sos;
      ceph::io_sequence::tester::SelectECPool spo;
      ceph::io_sequence::tester::SelectNumThreads snt;
      ceph::io_sequence::tester::SelectSeqRange ssr;

      boost::asio::io_context asio;
      std::thread thread;
      std::optional<boost::asio::executor_work_guard<
                    boost::asio::io_context::executor_type>> guard;
      ceph::mutex lock = ceph::make_mutex("RadosIo::lock");
      ceph::condition_variable cond;

      bool input_valid;

      bool verbose;
      bool dryrun;
      std::optional<int> seqseed;
      bool interactive;

      bool show_sequence;
      bool show_help;

      int num_objects;
      std::string object_name;

      std::string get_token();
      uint64_t get_numeric_token();

      bool run_automated_test();

      bool run_interactive_test();

      void help();
      void list_sequence();
    };
  }
}