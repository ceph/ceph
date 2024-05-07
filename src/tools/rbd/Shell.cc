// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/Shell.h"
#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/IndentStream.h"
#include "tools/rbd/OptionPrinter.h"
#include "common/ceph_argparse.h"
#include "common/config.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "include/stringify.h"
#include <algorithm>
#include <iostream>
#include <set>

namespace rbd {

namespace at = argument_types;
namespace po = boost::program_options;

namespace {

static const std::string APP_NAME("rbd");
static const std::string HELP_SPEC("help");
static const std::string BASH_COMPLETION_SPEC("bash-completion");

boost::intrusive_ptr<CephContext> global_init(
    int argc, const char **argv, std::vector<std::string> *command_args,
    std::vector<std::string> *global_init_args) {
  auto cmd_args = argv_to_vec(argc, argv);
  std::vector<const char*> args(cmd_args);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_MON_CONFIG);

  *command_args = {args.begin(), args.end()};

  // Scan command line arguments for ceph global init args (those are
  // filtered out from args vector by global_init).

  auto cursor = args.begin();
  for (auto &arg : cmd_args) {
    auto iter = cursor;
    for (; iter != args.end(); iter++) {
      if (*iter == arg) {
        break;
      }
    }
    if (iter == args.end()) {
      // filtered out by global_init
      global_init_args->push_back(arg);
    } else {
      cursor = ++iter;
    }
  }

  return cct;
}

std::string format_command_spec(const Shell::CommandSpec &spec) {
  return joinify<std::string>(spec.begin(), spec.end(), " ");
}

std::string format_alias_spec(const Shell::CommandSpec &spec,
                              const Shell::CommandSpec &alias_spec) {
    auto spec_it = spec.begin();
    auto alias_it = alias_spec.begin();
    int level = 0;
    while (spec_it != spec.end() && alias_it != alias_spec.end() &&
           *spec_it == *alias_it) {
      spec_it++;
      alias_it++;
      level++;
    }
    ceph_assert(spec_it != spec.end() && alias_it != alias_spec.end());

    if (level < 2) {
      return joinify<std::string>(alias_spec.begin(), alias_spec.end(), " ");
    } else {
      return "... " + joinify<std::string>(alias_it, alias_spec.end(), " ");
    }
}

std::string format_command_name(const Shell::CommandSpec &spec,
                                const Shell::CommandSpec &alias_spec) {
  std::string name = format_command_spec(spec);
  if (!alias_spec.empty()) {
    name += " (" + format_alias_spec(spec, alias_spec) + ")";
  }
  return name;
}

std::string format_option_suffix(
    const boost::shared_ptr<po::option_description> &option) {
  std::string suffix;
  if (option->semantic()->max_tokens() != 0) {
    if (option->description().find("path") != std::string::npos ||
        option->description().find("file") != std::string::npos) {
      suffix += " path";
    } else if (option->description().find("host") != std::string::npos) {
      suffix += " host";
    } else {
      suffix += " arg";
    }
  }
  return suffix;
}

} // anonymous namespace

std::vector<Shell::Action *>& Shell::get_actions() {
  static std::vector<Action *> actions;

  return actions;
}

std::set<std::string>& Shell::get_switch_arguments() {
  static std::set<std::string> switch_arguments;

  return switch_arguments;
}

void print_deprecated_warning(po::option_description option, std::string description) {
  auto pos = description.find_first_of(":");
  if (pos != std::string::npos) {
  std::string param = description.substr(pos + 1, description.size() - pos - 2);
  std::cerr << "rbd: " << option.format_name() << " is deprecated, use --"
            << param << std::endl;
  }
}

int Shell::execute(int argc, const char **argv) {
  std::vector<std::string> arguments;
  std::vector<std::string> ceph_global_init_args;
  auto cct = global_init(argc, argv, &arguments, &ceph_global_init_args);

  std::vector<std::string> command_spec;
  get_command_spec(arguments, &command_spec);
  bool is_alias = true;

  if (command_spec.empty() || command_spec == CommandSpec({"help"})) {
    // list all available actions
    print_help();
    return 0;
  } else if (command_spec[0] == HELP_SPEC) {
    // list help for specific action
    command_spec.erase(command_spec.begin());
    Action *action = find_action(command_spec, NULL, &is_alias);
    if (action == NULL) {
      print_unknown_action(command_spec);
      return EXIT_FAILURE;
    } else {
      print_action_help(action, is_alias);
      return 0;
    }
  } else if (command_spec[0] == BASH_COMPLETION_SPEC) {
    command_spec.erase(command_spec.begin());
    print_bash_completion(command_spec);
    return 0;
  }

  CommandSpec *matching_spec;
  Action *action = find_action(command_spec, &matching_spec, &is_alias);
  if (action == NULL) {
    print_unknown_action(command_spec);
    return EXIT_FAILURE;
  }

  po::variables_map vm;
  try {
    po::options_description positional_opts;
    po::options_description command_opts;
    (*action->get_arguments)(&positional_opts, &command_opts);

    // dynamically allocate options for our command (e.g. snap list) and
    // its associated positional arguments
    po::options_description argument_opts;
    argument_opts.add_options()
      (at::POSITIONAL_COMMAND_SPEC.c_str(),
       po::value<std::vector<std::string> >()->required(), "")
      (at::POSITIONAL_ARGUMENTS.c_str(),
       po::value<std::vector<std::string> >(), "");

    po::positional_options_description positional_options;
    positional_options.add(at::POSITIONAL_COMMAND_SPEC.c_str(),
                           matching_spec->size());
    if (!positional_opts.options().empty()) {
      int max_count = positional_opts.options().size();
      if (positional_opts.options().back()->semantic()->max_tokens() > 1)
        max_count = -1;
      positional_options.add(at::POSITIONAL_ARGUMENTS.c_str(), max_count);
    }

    po::options_description group_opts;
    group_opts.add(command_opts)
              .add(argument_opts);

    po::store(po::command_line_parser(arguments)
      .style(po::command_line_style::default_style &
        ~po::command_line_style::allow_guessing)
      .options(group_opts)
      .positional(positional_options)
      .run(), vm);

    if (vm[at::POSITIONAL_COMMAND_SPEC].as<std::vector<std::string> >() !=
          *matching_spec) {
      std::cerr << "rbd: failed to parse command" << std::endl;
      return EXIT_FAILURE;
    }

    int r = (*action->execute)(vm, ceph_global_init_args);

    if (vm.size() > 0) {
      for (auto opt : vm) {
        try {
          auto option = command_opts.find(opt.first, false);
          auto description = option.description();
          auto result = boost::find_first(description, "deprecated");
          if (!result.empty()) {
            print_deprecated_warning(option, description);
          }
        } catch (std::exception& e) {
          continue;
        }
      }
    }

    po::options_description global_opts;
    get_global_options(&global_opts);
    auto it = ceph_global_init_args.begin();
    for ( ; it != ceph_global_init_args.end(); ++it) {
      auto  pos = (*it).find_last_of("-");
      auto prefix_style = po::command_line_style::allow_long;
      if (pos == 0) {
        prefix_style = po::command_line_style::allow_dash_for_short;
      } else if (pos == std::string::npos) {
        continue;
      }

      for (size_t i = 0; i < global_opts.options().size(); ++i) {
        std::string param_name =  global_opts.options()[i]->canonical_display_name(
                                  prefix_style);
        auto description = global_opts.options()[i]->description();
        auto result = boost::find_first(description, "deprecated");
        if (!result.empty() && *it == param_name) {
          print_deprecated_warning(*global_opts.options()[i], description);
          break;
        }
      }
    }

    if (r != 0) {
      return std::abs(r);
    }
  } catch (po::required_option& e) {
    std::cerr << "rbd: " << e.what() << std::endl;
    return EXIT_FAILURE;
  } catch (po::too_many_positional_options_error& e) {
    std::cerr << "rbd: too many arguments" << std::endl;
    return EXIT_FAILURE;
  } catch (po::error& e) {
    std::cerr << "rbd: " << e.what() << std::endl;
    return EXIT_FAILURE;
  }

  return 0;
}

void Shell::get_command_spec(const std::vector<std::string> &arguments,
                             std::vector<std::string> *command_spec) {
  for (size_t i = 0; i < arguments.size(); ++i) {
    std::string arg(arguments[i]);
    if (arg == "-h" || arg == "--help") {
      *command_spec = {HELP_SPEC};
      return;
    } else if (arg == "--") {
      // all arguments after a double-dash are positional
      if (i + 1 < arguments.size()) {
        command_spec->insert(command_spec->end(),
                             arguments.data() + i + 1,
                             arguments.data() + arguments.size());
      }
      return;
    } else if (arg[0] == '-') {
      // if the option is not a switch, skip its value
      if (arg.size() >= 2 &&
          (arg[1] == '-' ||
	   get_switch_arguments().count(arg.substr(1, 1)) == 0) &&
          (arg[1] != '-' ||
	   get_switch_arguments().count(arg.substr(2, std::string::npos)) == 0) &&
          at::SWITCH_ARGUMENTS.count(arg.substr(2, std::string::npos)) == 0 &&
          arg.find('=') == std::string::npos) {
        ++i;
      }
    } else {
      command_spec->push_back(arg);
    }
  }
}

Shell::Action *Shell::find_action(const CommandSpec &command_spec,
                                  CommandSpec **matching_spec, bool *is_alias) {
  // sort such that all "trash purge schedule ..." actions come before
  // "trash purge"
  std::vector<Action *> actions(get_actions());
  std::sort(actions.begin(), actions.end(), [](auto lhs, auto rhs) {
    return lhs->command_spec.size() > rhs->command_spec.size();
  });

  for (Action *action : actions) {
    if (action->command_spec.size() <= command_spec.size()) {
      if (std::equal(action->command_spec.begin(),
                     action->command_spec.end(),
                     command_spec.begin())) {
        if (matching_spec != NULL) {
          *matching_spec = &action->command_spec;
        }
        *is_alias = false;
        return action;
      }
    }
    if (!action->alias_command_spec.empty() &&
        action->alias_command_spec.size() <= command_spec.size()) {
      if (std::equal(action->alias_command_spec.begin(),
                     action->alias_command_spec.end(),
                     command_spec.begin())) {
        if (matching_spec != NULL) {
          *matching_spec = &action->alias_command_spec;
        }
        *is_alias = true;
        return action;
      }
    }
  }
  return NULL;
}

void Shell::get_global_options(po::options_description *opts) {
  opts->add_options()
    ((at::CONFIG_PATH + ",c").c_str(), po::value<std::string>(), "path to cluster configuration")
    ("cluster", po::value<std::string>(), "cluster name")
    ("id", po::value<std::string>(), "client id (without 'client.' prefix)")
    ("user", po::value<std::string>(), "deprecated[:id]")
    ("name,n", po::value<std::string>(), "client name")
    ("mon_host,m", po::value<std::string>(), "monitor host")
    ("secret", po::value<at::Secret>(), "deprecated[:keyfile]")
    ("keyfile,K", po::value<std::string>(), "path to secret key")
    ("keyring,k", po::value<std::string>(), "path to keyring");
}

void Shell::print_help() {
  std::cout << "usage: " << APP_NAME << " <command> ..."
            << std::endl << std::endl
            << "Command-line interface for managing Ceph RBD images."
            << std::endl << std::endl;

  std::vector<Action *> actions(get_actions());
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
    if (!action->visible)
      continue;
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

  po::options_description global_opts;
  get_global_options(&global_opts);

  std::cout << std::endl << OptionPrinter::OPTIONAL_ARGUMENTS << ":" << std::endl;
  OptionPrinter::print_optional(global_opts, name_width, std::cout);

  std::cout << std::endl
            << "See '" << APP_NAME << " help <command>' for help on a specific "
            << "command." << std::endl;
 }

void Shell::print_action_help(Action *action, bool is_alias) {
  std::stringstream ss;
    ss << "usage: " << APP_NAME << " "
       << format_command_spec(is_alias ? action->alias_command_spec : action->command_spec);
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

  if (!action->help.empty()) {
    std::cout << action->help << std::endl;
  }
}

void Shell::print_unknown_action(const std::vector<std::string> &command_spec) {
  std::cerr << "error: unknown option '"
            << joinify<std::string>(command_spec.begin(),
                                    command_spec.end(), " ") << "'"
            << std::endl << std::endl;
  print_help();
}

void Shell::print_bash_completion(const CommandSpec &command_spec) {
  
  bool is_alias = true;

  Action *action = find_action(command_spec, NULL, &is_alias);
  po::options_description global_opts;
  get_global_options(&global_opts);
  print_bash_completion_options(global_opts);

  if (action != nullptr) {
    po::options_description positional_opts;
    po::options_description command_opts;
    (*action->get_arguments)(&positional_opts, &command_opts);
    print_bash_completion_options(command_opts);
  } else {
    std::cout << "|help";
    for (size_t i = 0; i < get_actions().size(); ++i) {
      Action *action = get_actions()[i];
      std::cout << "|"
                << joinify<std::string>(action->command_spec.begin(),
                                        action->command_spec.end(), " ");
      if (!action->alias_command_spec.empty()) {
        std::cout << "|"
                   << joinify<std::string>(action->alias_command_spec.begin(),
                                          action->alias_command_spec.end(),
                                          " ");
      }
    }
  }
  std::cout << "|" << std::endl;
}

void Shell::print_bash_completion_options(const po::options_description &ops) {
  for (size_t i = 0; i < ops.options().size(); ++i) {
    auto option = ops.options()[i];
    std::string long_name(option->canonical_display_name(0));
    std::string short_name(option->canonical_display_name(
      po::command_line_style::allow_dash_for_short));

    std::cout << "|--" << long_name << format_option_suffix(option);
    if (long_name != short_name) {
      std::cout << "|" << short_name << format_option_suffix(option);
    }
  }
}

} // namespace rbd
