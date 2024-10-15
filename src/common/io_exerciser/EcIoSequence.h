#include "IoSequence.h"

namespace ceph
{
  namespace io_exerciser
  {
    class EcIoSequence : public IoSequence
    {
      public:
        virtual bool is_supported(Sequence sequence) const override;
        static std::unique_ptr<IoSequence>
          generate_sequence(Sequence s,
                            std::pair<int,int> obj_size_range,
                            int k,
                            int m,
                            int seed );

      protected:
        bool setup_inject;
        bool clear_inject;
        std::optional<uint64_t> shard_to_inject;
        InjectOpType inject_op_type;

        EcIoSequence(std::pair<int,int> obj_size_range, int seed);

        // Writes cannot be sent to injected on shard zero, so selections seperated out
        void select_random_data_shard_to_inject_read_error(int k, int m);
        void select_random_data_shard_to_inject_write_error(int k, int m);
        void select_random_shard_to_inject_read_error(int k, int m);
        void select_random_shard_to_inject_write_error(int k, int m);
        void generate_random_read_inject_type();
        void generate_random_write_inject_type();
    };

    class ReadInjectSequence : public EcIoSequence
    {
    public:
      ReadInjectSequence(std::pair<int,int> obj_size_range, int seed, Sequence s, int k, int m);

      Sequence get_id() const override;
      std::string get_name() const override;
      virtual std::unique_ptr<IoOp> next() override;
      std::unique_ptr<IoOp> _next() override;

      private:
        std::unique_ptr<IoSequence> child_sequence;
        std::unique_ptr<IoOp> nextOp;
    };

    class Seq10: public EcIoSequence {
    public:
      Seq10(std::pair<int,int> obj_size_range, int seed, int k, int m);

      Sequence get_id() const override;
      std::string get_name() const override;
      std::unique_ptr<IoOp> _next() override;

    private:
      uint64_t offset;
      uint64_t length;

      bool inject_error_done;
      bool failed_write_done;
      bool read_done;
      bool clear_inject_done;
      bool successful_write_done;
      bool test_all_lengths;
      bool test_all_sizes;
    };
  }
}