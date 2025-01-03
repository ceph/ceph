// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_SHELL_H
#define CEPH_RBD_SHELL_H

#include "include/int_types.h"
#include <set>
#include <string>
#include <vector>
#include <boost/program_options.hpp>

namespace rbd {

class Shell {
public:
  typedef std::vector<std::string> CommandSpec;

  struct Action {
    typedef void (*GetArguments)(boost::program_options::options_description *,
                                 boost::program_options::options_description *);
    typedef int (*Execute)(const boost::program_options::variables_map &,
                           const std::vector<std::string> &);

    CommandSpec command_spec;
    CommandSpec alias_command_spec;
    const std::string description;
    const std::string help;
    GetArguments get_arguments;
    Execute execute;
    bool visible;

    template <typename Args, typename Execute>
    Action(const std::initializer_list<std::string> &command_spec,
           const std::initializer_list<std::string> &alias_command_spec,
           const std::string &description, const std::string &help,
           Args args, Execute execute, bool visible = true)
        : command_spec(command_spec), alias_command_spec(alias_command_spec),
          description(description), help(help), get_arguments(args),
          execute(execute), visible(visible) {
      Shell::get_actions().push_back(this);
    }

  };

  struct SwitchArguments {
    SwitchArguments(const std::initializer_list<std::string> &arguments) {
      Shell::get_switch_arguments().insert(arguments.begin(), arguments.end());
    }
  };

  int execute(int argc, const char **argv);

private:
  static std::vector<Action *>& get_actions();
  static std::set<std::string>& get_switch_arguments();

  void get_command_spec(const std::vector<std::string> &arguments,
                        std::vector<std::string> *command_spec);
  Action *find_action(const CommandSpec &command_spec,
                      CommandSpec **matching_spec, bool *is_alias);

  void get_global_options(boost::program_options::options_description *opts);

  void print_help();
  void print_action_help(Action *action, bool is_alias);
  void print_unknown_action(const CommandSpec &command_spec);

  void print_bash_completion(const CommandSpec &command_spec);
  void print_bash_completion_options(
    const boost::program_options::options_description &ops);
};

} // namespace rbd

#endif // CEPH_RBD_SHELL_H
