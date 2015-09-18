// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/Shell.h"
#include "tools/rbd/IndentStream.h"
#include "tools/rbd/OptionPrinter.h"
#include "common/config.h"
#include "global/global_context.h"
#include "include/stringify.h"
#include <algorithm>
#include <iostream>
#include <set>

namespace rbd {

namespace po = boost::program_options;

namespace {

struct Secret {};

void validate(boost::any& v, const std::vector<std::string>& values,
              Secret *target_type, int) {
  std::cerr << "rbd: --secret is deprecated, use --keyfile" << std::endl;

  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);
  int r = g_conf->set_val("keyfile", s.c_str());
  assert(r == 0);
  v = boost::any(s);
}

std::string base_name(const std::string &path,
                      const std::string &delims = "/\\") {
  return path.substr(path.find_last_of(delims) + 1);
}

std::string format_command_spec(const Shell::CommandSpec &spec) {
  return joinify<std::string>(spec.begin(), spec.end(), " ");
}

std::string format_command_name(const Shell::CommandSpec &spec,
                                const Shell::CommandSpec &alias_spec) {
  std::string name = format_command_spec(spec);
  if (!alias_spec.empty()) {
    name += " (" + format_command_spec(alias_spec) + ")";
  }
  return name;
}

} // anonymous namespace

std::vector<Shell::Action *> Shell::s_actions;
std::set<std::string> Shell::s_switch_arguments;

int Shell::execute(int arg_count, const char **arg_values) {
  std::string app_name(base_name(arg_values[0]));
  std::vector<std::string> command_spec;
  get_command_spec(arg_count, arg_values, &command_spec);

  if (command_spec.empty() || command_spec == CommandSpec({"help"})) {
    // list all available actions
    print_help(app_name);
    return 0;
  } else if (command_spec[0] == "help") {
    // list help for specific action
    command_spec.erase(command_spec.begin());
    Action *action = find_action(command_spec, NULL);
    if (action == NULL) {
      print_unknown_action(app_name, command_spec);
      return EXIT_FAILURE;
    } else {
      print_action_help(app_name, action);
      return 0;
    }
  }

  CommandSpec *matching_spec;
  Action *action = find_action(command_spec, &matching_spec);
  if (action == NULL) {
    print_unknown_action(app_name, command_spec);
    return EXIT_FAILURE;
  }

  po::variables_map vm;
  try {
    po::options_description positional;
    po::options_description options;
    (*action->get_arguments)(&positional, &options);

    // dynamically allocate options for our command (e.g. snap list) and
    // its associated positional arguments
    po::options_description arguments;
    arguments.add_options()
      ("positional-command-spec",
       po::value<std::vector<std::string> >()->required(), "")
      ("positional-arguments",
       po::value<std::vector<std::string> >(), "");

    po::positional_options_description positional_options;
    positional_options.add("positional-command-spec",
                           matching_spec->size());
    if (command_spec.size() > matching_spec->size()) {
      positional_options.add("positional-arguments", -1);
    }

    po::options_description global;
    get_global_options(&global);

    po::options_description group;
    group.add(options).add(arguments).add(global);

    po::store(po::command_line_parser(arg_count, arg_values)
      .style(po::command_line_style::default_style &
        ~po::command_line_style::allow_guessing)
      .options(group)
      .positional(positional_options)
      .allow_unregistered()
      .run(), vm);

    if (vm["positional-command-spec"].as<std::vector<std::string> >() !=
          *matching_spec) {
      std::cerr << "rbd: failed to parse command" << std::endl;
      return EXIT_FAILURE;
    }

    po::notify(vm);

    int r = (*action->execute)(vm);
    if (r != 0) {
      return std::abs(r);
    }
  } catch (po::required_option& e) {
    std::cerr << "rbd: " << e.what() << std::endl << std::endl;
    return EXIT_FAILURE;
  } catch (po::too_many_positional_options_error& e) {
    std::cerr << "rbd: too many positional arguments or unrecognized optional "
              << "argument" << std::endl;
  } catch (po::error& e) {
    std::cerr << "rbd: " << e.what() << std::endl << std::endl;
    return EXIT_FAILURE;
  }

  return 0;
}

void Shell::get_command_spec(int arg_count, const char **arg_values,
                             std::vector<std::string> *command_spec) {
  for (int i = 1; i < arg_count; ++i) {
    std::string arg(arg_values[i]);
    if (arg == "-h" || arg == "--help") {
      *command_spec = {"help"};
      return;
    } else if (arg[0] == '-') {
      // if the option is not a switch, skip its value
      if (arg.size() >= 2 &&
          (arg[1] == '-' || s_switch_arguments.count(arg.substr(1, 1)) == 0) &&
          (arg[1] != '-' ||
             s_switch_arguments.count(arg.substr(2, std::string::npos)) == 0) &&
          arg.find('=') == std::string::npos) {
        ++i;
      }
    } else {
      command_spec->push_back(arg);
    }
  }
}

Shell::Action *Shell::find_action(const CommandSpec &command_spec,
                                  CommandSpec **matching_spec) {
  for (size_t i = 0; i < s_actions.size(); ++i) {
    Action *action = s_actions[i];
    if (action->command_spec.size() <= command_spec.size()) {
      if (std::includes(action->command_spec.begin(),
                        action->command_spec.end(),
                        command_spec.begin(),
                        command_spec.begin() + action->command_spec.size())) {
        if (matching_spec != NULL) {
          *matching_spec = &action->command_spec;
        }
        return action;
      }
    }
    if (!action->alias_command_spec.empty() &&
        action->alias_command_spec.size() <= command_spec.size()) {
      if (std::includes(action->alias_command_spec.begin(),
                        action->alias_command_spec.end(),
                        command_spec.begin(),
                        command_spec.begin() +
                          action->alias_command_spec.size())) {
        if (matching_spec != NULL) {
          *matching_spec = &action->alias_command_spec;
        }
        return action;
      }
    }
  }
  return NULL;
}

void Shell::get_global_options(po::options_description *opts) {
  opts->add_options()
    ("conf,c", "path to cluster configuration")
    ("cluster", "cluster name")
    ("id,i", "client id (without 'client.' prefix)")
    ("name,n", "client name")
    ("secret", po::value<Secret>(), "path to secret key (deprecated)")
    ("keyfile", "path to secret key")
    ("keyring", "path to keyring");
}

void Shell::print_help(const std::string &app_name) {
  std::cout << "usage: " << app_name << " <command> ..."
            << std::endl << std::endl
            << "Command-line interface for managing Ceph RBD images."
            << std::endl << std::endl;

  std::vector<Action *> actions(s_actions);
  std::sort(actions.begin(), actions.end(),
            [](Action *lhs, Action *rhs) { return lhs->command_spec <
                                                    rhs->command_spec; });

  std::cout << OptionPrinter::POSITIONAL_ARGUMENTS << ":" << std::endl
            << "  <command>" << std::endl;

  // since the commands have spaces, we have to build our own formatter
  std::string indent(4, ' ');
  size_t name_width = OptionPrinter::MIN_NAME_WIDTH;
  for (size_t i = 0; i < actions.size(); ++i) {
    Action *action = actions[i];
    std::string name = format_command_name(action->command_spec,
                                           action->alias_command_spec);
    name_width = std::max(name_width, name.size());
  }
  name_width += indent.size();
  name_width = std::min(name_width, OptionPrinter::MAX_DESCRIPTION_OFFSET) + 1;

  for (size_t i = 0; i < actions.size(); ++i) {
    Action *action = actions[i];
    std::stringstream ss;
    ss << indent
       << format_command_name(action->command_spec, action->alias_command_spec);

    std::cout << ss.str();
    if (!action->description.empty()) {
      IndentStream indent_stream(name_width, ss.str().size(),
                                 OptionPrinter::LINE_WIDTH,
                                 std::cout);
      indent_stream << action->description << std::endl;
    } else {
      std::cout << std::endl;
    }
  }

  po::options_description global_opts(OptionPrinter::OPTIONAL_ARGUMENTS);
  get_global_options(&global_opts);
  std::cout << std::endl << global_opts << std::endl
            << "See '" << app_name << " help <command>' for help on a specific "
            << "command." << std::endl;
}

void Shell::print_action_help(const std::string &app_name, Action *action) {

  std::stringstream ss;
  ss << "usage: " << app_name << " "
     << format_command_spec(action->command_spec);
  std::cout << ss.str();

  po::options_description positional;
  po::options_description options;
  (*action->get_arguments)(&positional, &options);

  OptionPrinter option_printer(positional, options);
  option_printer.print_short(std::cout, ss.str().size());

  if (!action->description.empty()) {
    std::cout << std::endl << action->description << std::endl;
  }

  std::cout << std::endl;
  option_printer.print_detailed(std::cout);
}

void Shell::print_unknown_action(const std::string &app_name,
                                 const std::vector<std::string> &command_spec) {
  std::cerr << "error: unknown option '"
            << joinify<std::string>(command_spec.begin(),
                                    command_spec.end(), " ") << "'"
            << std::endl << std::endl;
  print_help(app_name);
}

} // namespace rbd
