// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_rest_callback_s3.h"

int RGWCallBackClient::receive_header(void * const ptr, const size_t len) {
    const boost::string_ref header_line(static_cast<const char * const>(ptr), len);

    /* We're tokening the line that way due to backward compatibility. */
    const size_t sep_loc = header_line.find_first_of(" \t:");

    if (boost::string_ref::npos == sep_loc) {
        /* Wrongly formatted header? Just skip it. */
        return 0;
    }

    header_name_t name(header_line.substr(0, sep_loc));
    if (sep_loc >=5 && header_line.substr(0, 5) ==  "HTTP/") {
        set_http_status(atoi(header_line.substr(sep_loc + 1).data()));
        if (get_http_status() == 100) /* 100-continue response */
            return 0;
    }

    if (0 == relevant_headers.count(name)) {
        /* Not interested in this particular header. */
        return 0;
    }

    const auto value_part = header_line.substr(sep_loc + 1);

    /* Skip spaces and tabs after the separator. */
    const size_t val_loc_s = value_part.find_first_not_of(' ');
    const size_t val_loc_e = value_part.find_first_of("\r\n");

    if (boost::string_ref::npos == val_loc_s ||
        boost::string_ref::npos == val_loc_e) {
        /* Empty value case. */
        found_headers.emplace(name, header_value_t());
    } else {
        found_headers.emplace(name, header_value_t(
                value_part.substr(val_loc_s, val_loc_e - val_loc_s)));
        if (name == "Content-Length"){
          long long length = atoll(value_part.substr(val_loc_s, val_loc_e - val_loc_s).data());
          if (length <= max_response)
            callback_contentlength_valid = true;
        }
        if (name == "Content-Type" && value_part.substr(val_loc_s, val_loc_e - val_loc_s).compare("application/json") == 0){
          callback_contenttype_valid = true;
        }
    }
    return 0;
}

void RGWCallBackClient::set_http_status(long _http_status) {
    http_status = _http_status;
}

int RGWCallBackClient::receive_data(void *ptr, size_t len, bool *pause) {
    if (!callback_contentlength_valid)
      return 0; /* don't read extra data */
    read_bl->append((char *)ptr, len);
    return 0;
}

std::string get_mimetype_from_name(struct req_state *s,
                                   const std::string name) {
    const char *suffix = strrchr(name.c_str(), '.');
    if (suffix) {
        suffix++;
        if (*suffix) {
            string suffix_str(suffix);
            const char *mime = rgw_find_mime_by_ext(suffix_str);
            if (mime) {
                return std::string(mime);
            }
        }
    }
    return "text/plain"; //default mimetype
}

std::string load_rgw_rsa_pri_key(CephContext *ctx,
                                 const std::string path) {
    string callback_pri_key = "";

    if (!path.empty()) {
        char callback_pri_key_buffer[2048];
        memset(callback_pri_key_buffer, 0, 2048);
        int prilen = safe_read_file("" /* base */, path.c_str(),
                                    callback_pri_key_buffer, 2047);
        if (prilen) {
            callback_pri_key = callback_pri_key_buffer;
            boost::algorithm::trim(callback_pri_key);
            if (callback_pri_key.back() == '\n')
                callback_pri_key.pop_back();
        }
    }
    return std::move(callback_pri_key);
}

RSA *createPrivateRSA(std::string key) {
    RSA *rsa = NULL;
    const char *c_string = key.c_str();
    BIO *keybio = BIO_new_mem_buf((void *) c_string, -1);
    if (keybio == NULL) {
        return 0;
    }
    rsa = PEM_read_bio_RSAPrivateKey(keybio, &rsa, NULL, NULL);
    return rsa;
}

bool RSASign(RSA *rsa,
             const unsigned char *Msg,
             size_t MsgLen,
             unsigned char **EncMsg,
             size_t *MsgLenEnc) {
    EVP_MD_CTX *m_RSASignCtx = EVP_MD_CTX_create();
    EVP_PKEY *priKey = EVP_PKEY_new();
    EVP_PKEY_assign_RSA(priKey, rsa);

    if (EVP_DigestSignInit(m_RSASignCtx, NULL, EVP_md5(), NULL, priKey) <= 0) {
        return false;
    }
    if (EVP_DigestSignUpdate(m_RSASignCtx, Msg, MsgLen) <= 0) {
        return false;
    }
    if (EVP_DigestSignFinal(m_RSASignCtx, NULL, MsgLenEnc) <= 0) {
        return false;
    }
    *EncMsg = (unsigned char *) malloc(*MsgLenEnc);
    if (EVP_DigestSignFinal(m_RSASignCtx, *EncMsg, MsgLenEnc) <= 0) {
        return false;
    }
    EVP_MD_CTX_cleanup(m_RSASignCtx);
    return true;
}

boost::optional <std::string> signMessage(std::string privateKey,
                                          std::string plainText) {
    RSA *privateRSA = createPrivateRSA(privateKey);
    unsigned char *encMessage;
    size_t encMessageLength;
    bool sign_ret = RSASign(privateRSA,
                            (unsigned char *) plainText.c_str(),
                            plainText.length(),
                            &encMessage,
                            &encMessageLength);
    if (sign_ret) {
        string base64Text = rgw::to_base64(std::string((const char *) encMessage,
                                                       encMessageLength));
        free(encMessage);
        return std::move(base64Text);
    }
    return boost::none;
}

boost::optional <std::string>
generate_auth_header_for_callback_request(const boost::string_ref url,
                                          const string body,
                                          const string privateKey) {
    std::string url_str(url.data(), url.size());
    std::string path, query;
    size_t pos = url_str.find("://");
    if (pos == std::string::npos) {
        return std::string("");
    }
    url_str = url_str.substr(pos + strlen("://"));
    pos = url_str.find("/");
    if (pos == std::string::npos) {
        path = "/";
        query = "";
    } else {
        std::string url_sub = url_str.substr(pos);
        size_t querypos = url_sub.find("?");
        if (querypos == std::string::npos) {
            path = url_sub;
            query = "";
        } else {
            path = url_sub.substr(0, querypos);
            query = url_sub.substr(querypos);
        }
    }
    std::string urldecode_path;
    urldecode_path = url_decode(path);
    std::stringstream auth_stream;
    auth_stream << urldecode_path << query << "\n" << body;
    boost::optional <std::string> auth = signMessage(privateKey,
                                                     auth_stream.str());
    if (auth != boost::none) {
        boost::erase_all(*auth, "\n");
        return std::move(*auth);
    } else {
        return boost::none;
    }
}

static std::string rsa_pri_key="";

void process_callback_err(struct req_state *s,
                          RGWOp* op,
                          int err_no,
                          const char* msg){
    set_req_state_err(s, err_no);
    std::stringstream message;
    message << msg;
    s->err.message = message.str();
    dump_errno(s);
    end_header(s, op, "application/xml");
    dump_start(s);
}

/* return 0 means will not send post request to user custom callback server and process as normal, -1 means need to
 * replace normal response with callback response
 */
int process_callback(struct req_state *s,
                     RGWOp* op,
                     int op_ret,
                     std::string etag,
                     boost::optional<std::map<std::string, RGWPostObj_ObjStore::post_form_part, 
					 const ltstr_nocase>> parts){

    if (s->cct->_conf->rgw_callback_private_rsa_key.empty() | s->cct->_conf->rgw_callback_public_rsa_key_url.empty())
        return 0;

    std::string callbackstr, callbackvarstr;
    int rgw_callback_callback_var_max_length = 1024 * s->cct->_conf->rgw_callback_callback_var_max_length;

    if (parts != boost::none) {
    //Post
	  std::map<std::string, RGWPostObj_ObjStore::post_form_part, ltstr_nocase>::iterator iter =
        parts->find("callback");
      if (iter == parts->end())
        return 0;
	  callbackstr = iter->second.data.to_str();;
	  if (callbackstr.length() > rgw_callback_callback_var_max_length){
		ldout(s->cct, 20) << "ERROR callback length is too big" << dendl;
		process_callback_err(s, op, -EINVAL,
							 "The callback length is too big");
		return -1;
	  }
    } else {
        //Put or CompleteMultiPartUpload
        const char *callback = s->info.env->get("HTTP_X_AMZ_META_CALLBACK");
        if (callback == NULL)
            return 0;
        callbackstr = callback;
        if (callbackstr.length() > rgw_callback_callback_var_max_length){
            ldout(s->cct, 20) << "ERROR callback length is too big" << dendl;
            process_callback_err(s, op, -EINVAL,
                                 "The x-amz-meta-callback length is too big");
            return -1;
        }

        const char *callbackvar = s->info.env->get("HTTP_X_AMZ_META_CALLBACK_VAR");
        if (callbackvar)
            callbackvarstr = callbackvar;

        if (callbackvarstr.length() > rgw_callback_callback_var_max_length){
            ldout(s->cct, 20) << "ERROR callback var length is too big" << dendl;
            process_callback_err(s, op, -EINVAL,
                                 "The x-amz-meta-callback-var length is too big");
            return -1;
        }
    }

    if (callbackstr.empty())
        return 0;

    if (rsa_pri_key.empty() &&
        !s->cct->_conf->rgw_callback_private_rsa_key.empty())
        rsa_pri_key = load_rgw_rsa_pri_key(s->cct,
                                           s->cct->_conf->rgw_callback_private_rsa_key);

    std::string decodecallback;
    try {
        decodecallback = rgw::from_base64(callbackstr);
    } catch(...) {
        ldout(s->cct, 20) << "ERROR base64 Decode" << dendl;
        process_callback_err(s, op, -EINVAL,
                             "The x-amz-meta-callback must be base64 encode.");
        return -1;
    }

    JSONParser parser;
    bool parse_success = parser.parse(decodecallback.c_str(),
                                      decodecallback.length());
    if (!parse_success) {
        ldout(s->cct, 20) << "ERROR Parse CallBack Json" << dendl;
        process_callback_err(s, op, -EINVAL,
                             "The callback configuration is not json format.");
        return -1;
    }
    auto cburl = parser.find_obj("callbackUrl");
    if (cburl == NULL) {
        ldout(s->cct, 20) << "ERROR Parse CallBack Json: No Found callbackUrl" << dendl;
        process_callback_err(s, op, -EINVAL,
                             "The callbackUrl is not found.");
        return -1;
    }
    auto cbbody = parser.find_obj("callbackBody");
    if (cbbody == NULL) {
        ldout(s->cct, 20) << "ERROR Parse CallBack Json: No Found callbackBody" << dendl;
        process_callback_err(s, op, -EINVAL,
                             "The callbackBody is not found.");
        return -1;
    }
    std::string body = cbbody->get_data();
    boost::replace_all(body, "${bucket}", s->bucket_name);
    std::string urlencode_object;
    url_encode(s->object.name, urlencode_object);
    boost::replace_all(body, "${object}", urlencode_object);
    boost::replace_all(body, "${etag}", etag);
    boost::replace_all(body, "${size}", std::to_string(s->obj_size));
    std::string urlencode_mimeType;
    url_encode(get_mimetype_from_name(s, s->object.name),
               urlencode_mimeType);
    boost::replace_all(body, "${mimeType}", urlencode_mimeType);

    if (!callbackvarstr.empty() || parts == boost::none) {
        if ( parts == boost::none ) {
            //Put or MultiPartUploadComplete
            string decodecallbackvar;
            try {
                decodecallbackvar = rgw::from_base64(callbackvarstr);
            } catch(...) {
                ldout(s->cct, 20) << "ERROR base64 Decode" << dendl;
                process_callback_err(s, op, -EINVAL,
                                     "The x-amz-meta-callback-var must be base64 encode.");
                return -1;
            }
            JSONParser parser;
            bool parse_success = parser.parse(decodecallbackvar.c_str(),
                                              decodecallbackvar.length());
            if (!parse_success){
                ldout(s->cct, 20) << "ERROR Parse CallBack var Json" << dendl;
                process_callback_err(s, op, -EINVAL,
                                     "The callback var is not json format.");
                return -1;
            } else {
                JSONObjIter iter = parser.find_first();
                for (; !iter.end(); ++iter) {
                    JSONObj *obj = *iter;
                    std::stringstream replacevar;
                    replacevar  << "${" << obj->get_name() << "}";
                    if (boost::starts_with(obj->get_name(),"x:")){
                        std::string urlencode_var;
                        url_encode(obj->get_data(), urlencode_var);
                        boost::replace_all(body, replacevar.str(), urlencode_var);
                    }
                }
            }
        } else {
            //Post
            for (auto const& part : *parts)
            {
              std::stringstream replacevar;
              replacevar  << "${" << part.first << "}";
              if (boost::starts_with(part.first,"x:")){
                std::string urlencode_var;
                url_encode(part.second.data.to_str(), urlencode_var);
                boost::replace_all(body, replacevar.str(), urlencode_var);
              }
            }
        }
    }

    bufferlist callback_bl;
    RGWCallBackClient callback_req(s->cct, "POST",
                                   cburl->get_data().c_str(),
                                   &callback_bl,
                                   {"Content-Type", "Content-Length"});

    callback_req.set_max_response(1024 * 1024 * s->cct->_conf->rgw_callback_max_response);

    auto cbbodytype = parser.find_obj("callbackBodyType");
    if (cbbodytype)
		callback_req.append_header("Content-Type", cbbodytype->get_data());
        
    auto cbhost = parser.find_obj("callbackHost");
    if (cbhost)
		callback_req.append_header("Host", cbhost->get_data());

    if (!rsa_pri_key.empty() && !s->cct->_conf->rgw_callback_public_rsa_key_url.empty()){
        callback_req.append_header("X-Callback-Pub-Key-Url",
                                   rgw::to_base64(s->cct->_conf->rgw_callback_public_rsa_key_url));
        boost::optional<std::string> auth = generate_auth_header_for_callback_request(cburl->get_data().c_str(),
                                                                                      body,
                                                                                      rsa_pri_key);
        if (auth !=boost::none)
            callback_req.append_header("Authorization", *auth);
    }
	
	callback_req.set_post_data(body);
	callback_req.set_send_length(body.length());
	callback_req.process();
	callback_bl.append((char)0); // NULL terminate for debug output

    if (callback_req.get_http_status() != 200){
        if (callback_req.get_http_status() < 0) {
            process_callback_err(s, op, -ERR_CALLBACK_BAD_RESPONSE,
                                 "Error status : Callback Server Network is Unreachable.");
        } else {
            std::stringstream ss;
            ss << "Error status : Get HTTP Status " << callback_req.get_http_status() << " From Callback Server";
            process_callback_err(s, op, -ERR_CALLBACK_BAD_RESPONSE, ss.str().c_str());
        }
    } else {
        if (!callback_req.is_valid_content_lenth()) {
            ldout(s->cct, 20) << "ERROR Parse Received Header From Callback Server" << dendl;
            process_callback_err(s, op, -ERR_CALLBACK_BAD_RESPONSE,
                                 "Content-Length not found or is too big.");
            return -1;
        }
        if (!callback_req.is_valid_content_type()) {
            ldout(s->cct, 20) << "ERROR Parse Received Header From Callback Server" << dendl;
            process_callback_err(s, op, -ERR_CALLBACK_BAD_RESPONSE,
                                 "Content-Type must be application/json.");
            return -1;
        }
        JSONParser parser;
        bool parse_success = parser.parse(callback_bl.c_str(), callback_bl.length());
        if (!parse_success) {
            ldout(s->cct, 20) << "ERROR Parse Received Json From Callback Server" << dendl;
            process_callback_err(s, op, -ERR_CALLBACK_BAD_RESPONSE,
                                 "Response body is not valid json format.");
            return -1;
        }
        set_req_state_err(s, op_ret);
        dump_content_length(s, callback_bl.length());
        dump_errno(s);
        end_header(s, op, "application/json");
        dump_body(s, callback_bl.c_str());
    }
    return -1;
}

