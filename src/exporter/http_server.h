#pragma once

#include <string>

void http_server_thread_entrypoint(std::string cert_path, std::string key_path, std::string tls_options);
