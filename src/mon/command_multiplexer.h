// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 * @author Jesse Williamson <jwilliamson@suse.de>
 *
*/

#ifndef COMMAND_DISPATCHER_HPP
 #define COMMAND_DISPATCHER_HPP 1

#include <string>
#include <vector>
#include <memory>
#include <algorithm>
#include <exception>
#include <string_view>

#include <experimental/iterator>

/*
This is a mini-framework for mapping commands and their parameters from the command-line (or wherever) to functions.

For example, if you are at the command-line and run "ceph mon add", this command will be forwarded to this framework.

From a high level, this library provides a container, called a command_registry, that holds bindings from strings
to functions, and manages the details of memory management, standardizing the details of whether or not a command
was handled, etc.. 

While much of the framework isn't directly specific to mon, there are two major ways involving specific code: we
have two kinds of operations, preprocess operations and prepare operations. 

Installing a command_registry into a subsystem requires patching the preprocess and prepare pipelines. Additionally,
the environment a user's function (ie. a command) runs in is centralized into a single state structure.

Implementation note:
	- rather than having a vector backing everything-- requiring expensive lookups for any search operation because
    keys in each object must be examined-- it would certainly be possible to back this with a boost::multiindex, or
    map of shared_ptr<>. Left as a future refinement. If this is done, I suggest making that portion of the implementation
    a type in common that more closely models an STL associative array, and building this around that implementation.
*/

namespace ceph {

enum class operation_type : int { preprocess, prepare };

template <typename CommandState, typename CommandResult>
struct command;

template <typename CommandType>
struct command_registry
{
 using command_type = CommandType;

 // The type returned by an operation:
 using status_type  = std::optional<typename command_type::result_type>;

 private:
 command_registry(const command_registry&)              = delete;
 command_registry& operator=(const command_registry&)   = delete;

 public:
 const std::string name;      // eg. the name of the registry 

 std::vector<std::unique_ptr<command_type>> commands;

 public:
 command_registry(std::string_view name_)
  : name(name_)
 {}

 virtual ~command_registry() = default;

 public:
 auto find_cmd_for_tag(std::string_view candidate_tag) const
 {
    using std::end;
    using std::begin;

    auto tag_match = [candidate_tag](const auto& command) {
        auto tags = command->tags();
        return end(tags) != std::find(begin(tags), end(tags), candidate_tag);
    };

    return std::find_if(begin(commands), end(commands), tag_match); 
 }

 // True if a "tag" is found:
 bool in_registry(std::string_view tag) const
 {
    return end(commands) != find_cmd_for_tag(tag);
 }

 // True if any tag in the "tags" sequence is found:
 template <typename K, typename A, template <typename, typename> typename SeqT = std::vector>
 bool in_registry(const SeqT<K, A>& tags) const
 {
    return std::end(tags) != std::find_if(std::begin(tags), std::end(tags), 
                                         [this](const auto& tag) {
                                           return this->in_registry(tag);
                                        });
 }

 void add_command(std::unique_ptr<CommandType> cmd) 
 {
    using namespace std;

    auto tags(cmd->tags());

    if (in_registry(tags)) {
      ostringstream os;
      os << "already in registry: ";
      copy(begin(tags), end(tags), experimental::ostream_joiner(os, ','));
      throw runtime_error(os.str());
    }

    commands.push_back(std::move(cmd));
 }

 // Actually accept a user's function and call it, or decline service:
 public:
 status_type apply(const operation_type op_type, 
                   std::string_view cmd_prefix, 
                   const typename CommandType::state_type& state)
 {
    auto cmd = find_cmd_for_tag(cmd_prefix);

    // Command not found:
    if (std::end(commands) != cmd) 
        return {};

    auto& c = *cmd;

    auto [result, tag] = c->accepts(op_type, cmd_prefix, c->tags());

    // Command found, but did not accept:
    if (!result)
        return {};

    if (ceph::operation_type::preprocess == op_type)
     return { c->at_preprocess(state, cmd_prefix.substr(tag.size())) };

    if (ceph::operation_type::prepare == op_type)
     return { c->at_prepare(state, cmd_prefix.substr(tag.size())) };

    // If this happens, something's very wrong:
    std::ostringstream os;
    os << "invalid operation type " << static_cast<int>(op_type);
    throw std::invalid_argument(os.str());
 }
};

/* 
This is a base class that provides a customization point for user functions, and is
what users will indirectly interact with most of the time.

In the context of your multiplexer deployment, there will be a defined type along the lines of:
    using my_command_t = ceph::command<my_command_ctx, my_command_result>;

Given that, to add a new function of your own, follow these steps: 

    * derive your new type from my_command_t;
    * define the tags (strings) you want to respond to by listing them in your
    tags() member function;
    * override one or both of the prepare() and preprocess() member functions;
    * register the type of your command with the registry helper

In practice, a minimal function would look something like this:

    struct my_command : public my_command_t
    {
        vector<string> tags() { return { "my_command" }; }
    };

...next, you need to register the type of your command function. The framework deployment will probably
provide a place for you to do this, most likely a free function called register_commands(). There
will be a place for you to add your command type. Below, we've added "my_command":

    void register_commands(my_command_registry& commands)
    {
     ceph::register_commands<my_command_registry, 
        my_command
     >(commands);
    }

That will automatically register your function, set up the command bindings, set up multiplexing for
whatever phase (preprocess, prepare) you've defined, and handle interpreting the results of your function,
leaving you to concentrate on what you want it to do.

Next, you'll probably want to define member functions for only the phases (ie. prepare, preprocess) that you
wish to respond to. For example, we can extend my_command with a prepare command like so:

    struct my_command : public my_command_t
    {
        vector<string> tags() { return { "my_command" }; }

        my_command_result at_prepare(const my_command_ctx& state, string_view cmd);
    };

...that's it. 

Your function's exact call is in the "cmd" parameter, and any other state is communicated via the
"state" parameter (which you may call something shorter if you like); the exact content depends on
what is available in your particular deployment.

Next, let's consider what to return from our functions. This is defined by your local deployment, there 
is no fixed type. Most of the time, you'll probably see something like this:
    using my_cmd_result = std::tuple<my_cmd_status, int>;

Where my_cmd_status is something like:

    enum class my_cmd_status : int { failure = 0, success };

...but, once again, nothing in the mini-framework enforces this. Above, the enum is allowed to degrade
to int because the functions it wraps expects an int <= 0 on failure, > 0 on success back.

Now, our final example:

    struct my_command : public my_command_t
    {
        vector<string> tags() { return { "my_command" }; }

        my_command_result at_prepare(const my_command_ctx& state, string_view cmd)
        {
            return { my_cmd_status::success, 1 };
        }
    };

 Design note on context structure:
	Any similar design has to have some way of passing parameters into individual commands. In
 the original version, commands were implemented as member functions within the base class, and
 has these implicitly available. By separating them, we're left with several choices: on one hand,
 we could pass them in the signature for each command. This has the advantage of not requiring a
 proxy type. On the other hand, by using a proxy type we avoid having to change the signature of
 every command function when we add or remove an element of the signature, at the comparatively
 minor expense of having to refer to a "context" structure. This has the additional benefit of
 forcing member names to be consistent, and has the additional benefit of making very explicit
 which (potentially private!) members from the base class are required by commands. There is
 a disavantage here in that the cost of constructing unused parameters is taken for any call,
 but as these are so far lightweight objects and there are ways of handling outlier types,
 I felt this was an entirely acceptable exchange.
*/

template <typename CommandState, typename CommandResult>
struct command 
{
 using state_type   = CommandState;
 using result_type  = CommandResult;

 public:
 virtual ~command() {}

 /* The default string-matching acceptance check. Nothing prevents you from 
 creating a custom version that assumes tags are regular expressions, for 
 example (this might make a future good default version, for that matter): */
 public:
 [[nodiscard]] 
 virtual auto accepts(const ceph::operation_type op_type, 
                      std::string_view command, 
                      const std::vector<std::string>& tags) 
 -> std::tuple<bool, std::string>
 {
    for (const auto& tag : tags) {
      if (tag.size() > command.size())
       continue;

      if (tag == command.substr(0, tag.size()))
       return { true, tag };
    }

    return { false, "" };
 }

 public:
 virtual std::vector<std::string> tags() = 0;

 /* CommandResult must be a DefaultConstructable type. It is expected that returning
 a default-constructed CommandResult indicates an accepted-but-failed state, however 
 there is no constraint on this within the framework itself (it is instead determined
 by the actual local dispatching stub) because it is possible that an implementation 
 will want to accomplish something by side-effect. 

 Should we need to add more than two categories, I suggest it would be best to 
 approach this design a bit differently, using an associative array. (In any 
 case, this is a situation where a little runtime overhead should be 
 acceptable.) Perhaps something like:
    auto fn = cmd.functions["preprocess"]; 
 or auto ff = cmd.functions[tag_types::preprocess];
 */
 public:
 virtual result_type at_preprocess(const state_type& state, std::string_view cmd) { return result_type {}; };
 virtual result_type at_prepare(const state_type& state, std::string_view cmd)    { return result_type {}; };
};

// Helper for adding multiple default-constructed commands:
template <typename CommandRegistry, typename ...CommandTypes>
void register_commands(CommandRegistry& registry)
{
 (registry.add_command(std::make_unique<CommandTypes>(CommandTypes {})), ...);
}

} // namespace ceph

#endif
