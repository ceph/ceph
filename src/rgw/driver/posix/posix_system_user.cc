#include "posix_system_user.h"
#include "rgw_sal_posix.h"

/* Map entry formatting:
 * { "rgwUser": {
 *     "id": ...
 *   }, 
 *   "posixUser": {
 *     "uid": ...,
 *     "gid": ...,
 *   } 
 */ 

int read_mapping(const DoutPrefixProvider *dpp, int fd, char* read_buf, int size, std::multimap<std::string, std::pair<int, int>>& user_data) {
  int ret = lseek(fd, (size_t)0, SEEK_SET);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "POSIXSystemManager::" << __func__ << "(): ERROR: Could not seek mapping" << ": "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }
  
  memset(read_buf, 0, size);

  ret = ::read(fd, read_buf, size);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "POSIXSystemManager::" << __func__ << "(): ERROR: Could not read mapping" << ": "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  std::string read_str = read_buf;
  std::vector<std::string> vals;
  boost::replace_all(read_str, "}}", "}}#");
  boost::split(vals, read_str, boost::is_any_of("#"));

  for (const auto& val : vals) {
    JSONParser parser;
    bufferlist bl;
    if (!val.length() || !boost::starts_with(val, "{")) {
      continue;
    }

    bl.append(val);
    if (!parser.parse(bl.c_str(), bl.length())) {
      ldpp_dout(dpp, 0) << "POSIXSystemManager::" << __func__ << "(): ERROR: Malformed json" << dendl;
      return -EINVAL;
    }

    auto iter = parser.find_first("rgw_user");
    for (; !iter.end(); ++iter) {
      JSONObj *obj = *iter;

      std::pair<int, int> posix_user;
      JSONObjIter iter2 = obj->find_first();
      for (; !iter2.end(); ++iter2) {
	JSONObj *child = *iter2;
	if (child->get_name() == "uid") {
	  posix_user.first = std::stoi(child->get_data());
	} else if (child->get_name() == "gid") {
	  posix_user.second = std::stoi(child->get_data());
	}
      }

      user_data.insert({(*iter)->get_data(), {posix_user.first, posix_user.second}});
    }
  }
  
  return 0;
}

int POSIXSystemManager::init(const DoutPrefixProvider *dpp) {
  path = g_conf().get_val<std::string>("rgw_posix_base_path") + "/user_mapping"; 
  
  int fd = open(path.c_str(), (O_CREAT) | O_RDONLY | O_NOFOLLOW, S_IRWXU);
  if (fd < 0) { 
    fd = errno;
    ldpp_dout(dpp, 0) << "POSIXSystemManager::" << __func__ << "(): ERROR: Could not open mapping, err=" << cpp_strerror(fd) << dendl;
    return -fd;
  } 

  auto size = lseek(fd, (size_t)0, SEEK_END);
  if (size < 0) {
    int ret = errno;
    ldpp_dout(dpp, 0) << "POSIXSystemManager::" << __func__ << "(): ERROR: Could not retrieve mapping size" << ": "
      << cpp_strerror(ret) << dendl;
    close(fd);
    return -ret;
  }

  if (!size) {
    ldpp_dout(dpp, 20) << "POSIXSystemManager::" << __func__ << "(): Mapping successfully created." << dendl;
  } else { // Mapping already exists; initialize user_data
    char read_buf[size];
    int ret = read_mapping(dpp, fd, read_buf, size, user_data); 
    if (ret < 0) {
      close(fd);
      return ret;
    }
  }

  ldpp_dout(dpp, 20) << "POSIXSystemManager::" << __func__ << "(): Mapping path: " << path << dendl;
  return 0;
}

/* Must be called before every workflow to ensure user_data is correctly populated.
 * In the future, inotify and Watch Notify can be used to guarantee consistency in 
 * local and distributed files, respectively. */
int POSIXSystemManager::populate_user_data(const DoutPrefixProvider *dpp) {
  int ret;
  int fd = open(path.c_str(), (O_APPEND) | O_RDONLY | O_NOFOLLOW, S_IRWXU);
  if (fd < 0) { 
    fd = errno;
    ldpp_dout(dpp, 0) << "POSIXSystemManager::" << __func__ << "(): ERROR: Could not open mapping, err=" << cpp_strerror(fd) << dendl;
    return -fd;
  } 

  auto size = lseek(fd, (size_t)0, SEEK_END);
  if (size < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "POSIXSystemManager::" << __func__ << "(): ERROR: Could not retrieve mapping size" << ": "
      << cpp_strerror(ret) << dendl;
    close(fd);
    return -ret;
  }

  ldpp_dout(dpp, 20) << "POSIXSystemManager::" << __func__ << "(): Read buf size: " << size << dendl;

  if (size) {
    char read_buf[size];
    ret = read_mapping(dpp, fd, read_buf, size, user_data); 
    if (ret < 0) {
      close(fd);
      return ret;
    }
  }

  return 0;
}

int POSIXSystemManager::find_posix_user(const DoutPrefixProvider *dpp, const rgw_user& ruser, POSIXSystemUser& posix_user) {
  if (dpp->get_cct()->_conf->rgw_posix_user_mapping_type == "none") {
    return 0; // do nothing
  }

  auto it = user_data.find(ruser.id);
  if (it == user_data.end()) {
    ldpp_dout(dpp, 0) << "POSIXSystemManager::" << __func__ << "(): ERROR: No entry found for rgw_user " << ruser.id << dendl;
    return -ENOENT;
  } else {
    posix_user.set_uid(it->second.first);
    posix_user.set_gid(it->second.second);
  }
  
  return 0;
}

// TODO: Consider case where ruser already exists in mapping
int POSIXSystemManager::update_posix_user(const DoutPrefixProvider *dpp, const rgw_user& ruser, POSIXSystemUser posix_user) {
  if (dpp->get_cct()->_conf->rgw_posix_user_mapping_type == "none") {
    return 0;
  }

  int fd = open(path.c_str(), (O_APPEND) | O_RDWR | O_NOFOLLOW, S_IRWXU);
  if (fd < 0) { 
    fd = errno;
    ldpp_dout(dpp, 0) << "POSIXSystemManager::" << __func__ << "(): ERROR: Could not open mapping, err=" << cpp_strerror(fd) << dendl;
    return -fd;
  } 

  // Write to user mapping file
  JSONFormatter formatter;
  bufferlist bl;

  formatter.open_object_section("user");
  formatter.dump_string("rgw_user", ruser.id.c_str());
  formatter.open_object_section("posix_user");
  formatter.dump_int("uid", posix_user.get_uid());
  formatter.dump_int("gid", posix_user.get_gid());
  formatter.close_section();
  formatter.close_section();
  formatter.flush(bl);

  int count = write(fd, bl.c_str(), bl.length());
  if (count < 0) {
    count = errno;
    ldpp_dout(dpp, 0) << "POSIXSystemManager::" << __func__ << "(): ERROR: Could not write to mapping" << cpp_strerror(count) << dendl;
    close(fd);
    return -count;
  }

  user_data.insert({ruser.id, {posix_user.get_uid(), posix_user.get_gid()}});

  close(fd);
  return 0;
}

int POSIXSystemManager::remove_posix_user(const DoutPrefixProvider *dpp, const rgw_user& ruser) {
  if (dpp->get_cct()->_conf->rgw_posix_user_mapping_type == "none") {
    return 0;
  }

  auto it = user_data.find(ruser.id);
  if (it == user_data.end()) {
    ldpp_dout(dpp, 20) << "POSIXSystemManager::" << __func__ << "(): No entry found for rgw_user " << ruser.id << dendl;
    return 0;
  }

  int fd = open(path.c_str(), (O_APPEND) | O_RDONLY | O_NOFOLLOW, S_IRWXU);
  if (fd < 0) { 
    fd = errno;
    ldpp_dout(dpp, 0) << "POSIXSystemManager::" << __func__ << "(): ERROR: Could not open mapping, err=" << cpp_strerror(fd) << dendl;
    return -fd;
  } 

  int ret;
  auto size = lseek(fd, (size_t)0, SEEK_END);
  if (size < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "POSIXSystemManager::" << __func__ << "(): ERROR: Could not retrieve mapping size" << ": "
      << cpp_strerror(ret) << dendl;
    close(fd);
    return -ret;
  }

  ldpp_dout(dpp, 20) << "POSIXSystemManager::" << __func__ << "(): Read buf size: " << size << dendl;

  if (size) {
    char read_buf[size];
    ret = read_mapping(dpp, fd, read_buf, size, user_data); 
    if (ret < 0) {
      close(fd);
      return ret;
    }

    // Close and reinitialize file
    close(fd);
    fd = open(path.c_str(), (O_TRUNC) | O_RDWR | O_NOFOLLOW, S_IRWXU);
    if (fd < 0) { 
      fd = errno;
      ldpp_dout(dpp, 0) << "POSIXSystemManager::" << __func__ << "(): ERROR: Could not open mapping, err=" << cpp_strerror(fd) << dendl;
      return -fd;
    } 

    bufferlist bl;
    std::string read_str = read_buf;
    size_t pos = read_str.find(ruser.id);
    std::string begin = R"({"rgw_user":")";
    std::string end = ruser.id + R"(","posix_user":{"uid":")" + std::to_string(it->second.first) + R"(,"gid":)" + std::to_string(it->second.first) + R"(}})";
    std::string new_mapping = read_str.substr(0, pos - begin.size()) + read_str.substr(pos + end.size() - 1, size - pos - end.size() - 1); 
    bl.append(new_mapping);

    int count = write(fd, bl.c_str(), bl.length());
    if (count < 0) {
      count = errno;
      ldpp_dout(dpp, 0) << "POSIXSystemManager::" << __func__ << "(): ERROR: Could not write to mapping" << cpp_strerror(count) << dendl;
      close(fd);
      return -count;
    }

    user_data.erase(it);
    close(fd);
  } else {
    ldpp_dout(dpp, 0) << "POSIXSystemManager::" << __func__ << "(): ERROR: Mapping not found" << dendl;
    close(fd);
    return -EINVAL;
  }

  return 0;
}
