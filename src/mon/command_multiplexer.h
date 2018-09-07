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

template <typename CommandState, typename CommandResult>
struct command 
{
 using state_type   = CommandState;
 using result_type  = CommandResult;

 public:
 virtual ~command() {}

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
