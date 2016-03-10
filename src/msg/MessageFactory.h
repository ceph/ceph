//
// Created by skyinno on 2/14/16.
//

#ifndef CEPH_MESSAGEFACTORY_H
#define CEPH_MESSAGEFACTORY_H

#include <map>
#include <functional>
#include <memory>
#include "Message.h"

struct MessageFactory
{
    template<typename T>
    struct register_t
    {
        register_t(int key)
        {
            MessageFactory::get().map_.emplace(key, &register_t<T>::create);
        }

        template<typename... Args>
        register_t(int key, Args... args)
        {
            MessageFactory::get().map_.emplace(key, [&] { return new T(args...); });
        }
        inline static Message* create() { return new T; }
    };

	inline Message* produce(int key)
	{
		auto it = map_.find(key);
		if (it == map_.end())
			return nullptr;

		return it->second();
	}

    /*
    std::unique_ptr<Message> produce_unique(const std::string& key)
    {
        return std::unique_ptr<Message>(produce(key));
    }

    std::shared_ptr<Message> produce_shared(const std::string& key)
    {
        return std::shared_ptr<Message>(produce(key));
    }
     */

    typedef Message*(*FunPtr)();

    inline static MessageFactory& get()
    {
        static MessageFactory instance;
        return instance;
    }

private:
    MessageFactory() {};
    MessageFactory(const MessageFactory&) = delete;
    MessageFactory(MessageFactory&&) = delete;

    std::map<int, FunPtr> map_;
};

//std::map<std::string, factory::FunPtr> factory::map_;

#define REGISTER_MESSAGE_VNAME(T) reg_msg_##T##_
#define REGISTER_MESSAGE(T, key, ...) static MessageFactory::register_t<T> REGISTER_MESSAGE_VNAME(T)(key, ##__VA_ARGS__);

#endif //CEPH_MESSAGEFACTORY_H