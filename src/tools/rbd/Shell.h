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
    typedef int (*Execute)(const boost::program_options::variables_map &);

    CommandSpec command_spec;
    CommandSpec alias_command_spec;
    const std::string description;
    const std::string help;
    GetArguments get_arguments;
    Execute execute;

    template <typename Args, typename Execute>
    Action(const std::initializer_list<std::string> &command_spec,
           const std::initializer_list<std::string> &alias_command_spec,
           const std::string &description, const std::string &help,
           Args args, Execute execute)
        : command_spec(command_spec), alias_command_spec(alias_command_spec),
          description(description), help(help), get_arguments(args),
          execute(execute) {
      Shell::s_actions.push_back(this);
    }

  };

  struct SwitchArguments {
    SwitchArguments(const std::initializer_list<std::string> &arguments) {
      Shell::s_switch_arguments.insert(arguments.begin(), arguments.end());
    }
  };

  int execute(int arg_count, const char **arg_values);

private:
  static std::vector<Action *> s_actions;
  static std::set<std::string> s_switch_arguments;

  void get_command_spec(const std::vector<std::string> &arguments,
                        std::vector<std::string> *command_spec);
  Action *find_action(const CommandSpec &command_spec,
                      CommandSpec **matching_spec);

  void get_global_options(boost::program_options::options_description *opts);
  void prune_command_line_arguments(int arg_count, const char **arg_values,
                                    std::vector<std::string> *args);

  void print_help(const std::string &app_name);
  void print_action_help(const std::string &app_name, Action *action);
  void print_unknown_action(const std::string &app_name,
                            const std::vector<std::string> &command_spec);
};

} // namespace rbd

#endif // CEPH_RBD_SHELL_H
