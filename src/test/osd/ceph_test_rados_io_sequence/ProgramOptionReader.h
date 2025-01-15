#pragma once

#include <boost/program_options.hpp>
#include <optional>
#include <string>

#include "include/ceph_assert.h"
#include "include/random.h"

/* Overview
 *
 * class ProgramOptionReader
 *    Base class that is constructed using a variable_map from
 *    boost::program_options and the name of an option.
 *    This class extracts the named option from the variable map into the
 *    force_value variable.
 *    It is necissairy to override this class and implement the pure virtual
 *    select() function to convert the input type to the return type as well as
 *    implement how to calculate a default value if no input is given.
 *
 * class OptionalProgramOptionReader
 *   Redefinition of ProgramOptionalReader that returns the result as a
 *   std::optional of the return type. Shorthand for specifying the above
 *   with a second return type of optional<return_type> for
 *   all optional implementations
 *
 * ProgramOptionSelector
 *    Implementation of the above ProgramOptionReader.
 *    This is constructed with a list of options and implements the select
 *    function to return randomly from this list when called and no argument is
 *    given in the variables_map.
 *    The first time this is called, we will always return the first item of the
 *    list when select_first is true and no value is found in the variables_map.
 *    This takes an random_number_generator so that randomness can be controlled
 *    and reproduced if desired.
 *
 * ProgramOptionGeneratedSelector
 *   A class for selecting an option from a list of options, similar to above.
 *   This implements a pure virtual method generate_selections() which should be
 *   overriden so generation code can be added to generate the selection list at
 *   runtime, whereas ProgramOptionSelector relies on the list existing at
 *   compile time.
 *
 */

namespace po = boost::program_options;

namespace ceph {
namespace io_sequence {
namespace tester {
template <typename option_type,
          typename return_type = option_type>
class ProgramOptionReader {
 public:
  ProgramOptionReader(po::variables_map& vm,
                      const std::string& option_name)
      : option_name(option_name) {
    if (vm.count(option_name) > 0) {
      force_value = vm[option_name].as<option_type>();
    }
  }

  virtual ~ProgramOptionReader() = default;

  bool isForced() { return force_value.has_value(); }

  virtual const return_type select() = 0;

 protected:
  std::optional<option_type> force_value;

  std::string option_name;
};

template <typename option_type>
class OptionalProgramOptionReader
    : public ProgramOptionReader<option_type, std::optional<option_type>> {
 public:
  using ProgramOptionReader<option_type,
                            std::optional<option_type>>::ProgramOptionReader;
};

template <typename option_type,
          int num_selections,
          const std::array< option_type,
                            num_selections>& selections_array>
class ProgramOptionSelector : public ProgramOptionReader<option_type> {
 public:
  ProgramOptionSelector(ceph::util::random_number_generator<int>& rng,
                        po::variables_map& vm,
                        const std::string& option_name,
                        bool select_first)
      : ProgramOptionReader<option_type>(vm, option_name), rng(rng) {
    if (select_first) {
      ceph_assert(selections_array.size() > 0);
      first_value = selections_array[0];
    }
  }

  virtual ~ProgramOptionSelector() = default;

  virtual const option_type select() override {
    if (this->force_value.has_value()) {
      return *this->force_value;
    } else if (first_value.has_value()) {
      return *std::exchange(first_value, std::nullopt);
    } else {
      return selections_array[rng(num_selections - 1)];
    }
  }

 protected:
  ceph::util::random_number_generator<int>& rng;

  std::optional<option_type> first_value;
};

template <typename option_type>
class ProgramOptionGeneratedSelector
    : public OptionalProgramOptionReader<option_type> {
 public:
  ProgramOptionGeneratedSelector(ceph::util::random_number_generator<int>& rng,
                                 po::variables_map& vm,
                                 const std::string& option_name,
                                 bool first_use)
      : OptionalProgramOptionReader<option_type>(vm, option_name),
        rng(rng),
        first_use(first_use) {}

  const std::optional<option_type> select() final {
    if (this->force_value.has_value())
      return *this->force_value;
    else if (first_use)
      return selectFirst();
    else
      return selectRandom();
  }

 protected:
  virtual const std::vector<option_type> generate_selections() = 0;
  virtual const std::optional<option_type> selectFirst() {
    first_use = false;
    std::vector<option_type> selection = generate_selections();
    if (selection.size() > 0)
      return selection[0];
    else
      return std::nullopt;
  }

  virtual const std::optional<option_type> selectRandom() {
    std::vector<option_type> selection = generate_selections();
    if (selection.size() > 0)
      return selection[rng(selection.size() - 1)];
    else
      return std::nullopt;
  }

  bool is_first_use() { return first_use; }

 private:
  ceph::util::random_number_generator<int>& rng;

  bool first_use;
};
}  // namespace tester
}  // namespace io_sequence
}  // namespace ceph