#include "rgw_s3_request.h"
#define COPY_BUF_SIZE (4 * 1024 * 1024)
#include <errno.h>
#define dout_subsys ceph_subsys_rgw


int RemoteRequest::prepare_op(std::string key,  bufferlist *bl, off_t read_len, off_t ofs, off_t read_ofs, string dest, rgw::Aio* aio, rgw::AioResult* r, cache_block *c_block, string path, void(*func)(RemoteRequest*)){

  this->r = r;
  this->aio = aio;
//  this->bl = bl;
  this->ofs = ofs;
  this->read_ofs = read_ofs;
  this->key = key;
  this->read_len = read_len;
  this->dest = dest;
  this->path = path;
  this->ak = c_block->c_obj.accesskey.id;
  this->sk = c_block->c_obj.accesskey.key;
  this->func= func;
  return 0;
}



string RemoteS3Request::sign_s3_request(string HTTP_Verb, string uri, string date, string YourSecretAccessKeyID, string AWSAccessKeyId){
  std::string Content_Type = "application/x-www-form-urlencoded; charset=utf-8";
  std::string Content_MD5 ="";
  std::string CanonicalizedResource = uri.c_str();
  std::string StringToSign = HTTP_Verb + "\n" + Content_MD5 + "\n" + Content_Type + "\n" + date + "\n" +CanonicalizedResource;
  char key[YourSecretAccessKeyID.length()+1] ;
  strcpy(key, YourSecretAccessKeyID.c_str());
  const char * data = StringToSign.c_str();
  unsigned char* digest;
  digest = HMAC(EVP_sha1(), key, strlen(key), (unsigned char*)data, strlen(data), NULL, NULL);
  std::string signature = base64_encode(digest, 20);
  return signature;
}

string RemoteS3Request::get_date(){
  time_t now = time(0);
  tm *gmtm = gmtime(&now);
  string date;
  char buffer[128];
  std::strftime(buffer,128,"%a, %d %b %Y %X %Z",gmtm);
  date = buffer;
  return date;
}




int RemoteS3Request::submit_http_get_request_s3(){
  //int begin = req->ofs + req->read_ofs;
  //int end = req->ofs + req->read_ofs + req->read_len - 1;
  auto start = chrono::steady_clock::now();

  off_t begin = req->ofs;
  off_t end = req->ofs + req->read_len - 1;
  std::string range = std::to_string(begin)+ "-"+ std::to_string(end);
  if (req->dest.compare("")==0)
	req->dest = cct->_conf->backend_url;
  //std::string range = std::to_string( (int)req->ofs + (int)(req->read_ofs))+ "-"+ std::to_string( (int)(req->ofs) + (int)(req->read_ofs) + (int)(req->read_len - 1));
  ldout(cct, 10) << __func__  << " key " << req->key << " range " << range  << " dest "<< req->dest <<dendl;
  
  CURLcode res;
  string uri = "/"+ req->path;;
  //string uri = "/"+req->c_block->c_obj.bucket_name + "/" +req->c_block->c_obj.obj_name;
  string date = get_date();
   
  //string AWSAccessKeyId=req->c_block->c_obj.accesskey.id;
  //string YourSecretAccessKeyID=req->c_block->c_obj.accesskey.key;
  string AWSAccessKeyId=req->ak;
  string YourSecretAccessKeyID=req->sk;
  string signature = sign_s3_request("GET", uri, date, YourSecretAccessKeyID, AWSAccessKeyId);
  string Authorization = "AWS "+ AWSAccessKeyId +":" + signature;
  string loc =  req->dest + uri;
  string auth="Authorization: " + Authorization;
  string timestamp="Date: " + date;
  string user_agent="User-Agent: aws-sdk-java/1.7.4 Linux/3.10.0-514.6.1.el7.x86_64 OpenJDK_64-Bit_Server_VM/24.131-b00/1.7.0_131";
  string content_type="Content-Type: application/x-www-form-urlencoded; charset=utf-8";
  curl_handle = curl_easy_init();
  if(curl_handle) {
    struct curl_slist *chunk = NULL;
    chunk = curl_slist_append(chunk, auth.c_str());
    chunk = curl_slist_append(chunk, timestamp.c_str());
    chunk = curl_slist_append(chunk, user_agent.c_str());
    chunk = curl_slist_append(chunk, content_type.c_str());
    chunk = curl_slist_append(chunk, "CACHE_GET_REQ:rgw_datacache");
    curl_easy_setopt(curl_handle, CURLOPT_RANGE, range.c_str());
    res = curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, chunk); //set headers
    curl_easy_setopt(curl_handle, CURLOPT_URL, loc.c_str());
    curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1L); //for redirection of the url
    curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, _remote_req_cb);
    curl_easy_setopt(curl_handle, CURLOPT_NOSIGNAL, 1L);
//    curl_easy_setopt(curl_handle, CURLOPT_VERBOSE, 1L);
    curl_easy_setopt(curl_handle, CURLOPT_FAILONERROR, 1L);
    curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void*)req);
    res = curl_easy_perform(curl_handle); //run the curl command
    curl_easy_reset(curl_handle);
    curl_slist_free_all(chunk);
    curl_easy_cleanup(curl_handle);
  }
  if(res == CURLE_HTTP_RETURNED_ERROR) {
   ldout(cct,10) << "__func__ " << " CURLE_HTTP_RETURNED_ERROR" <<curl_easy_strerror(res) << " key " << req->key << dendl;
   return -1;
  }
   auto end2 = chrono::steady_clock::now();
   ldout(cct,10) << __func__  << " done dest " <<req->dest << " milisecond " <<
   chrono::duration_cast<chrono::microseconds>(end2 - start).count() 
   << dendl; 
  if (res != CURLE_OK) { return -1;}
  else { return 0; }

}
string RemoteS3Request::sign_s3_request2(string HTTP_Verb, string uri, string date, string YourSecretAccessKeyID, string AWSAccessKeyId){
  std::string Content_Type = "text/plain";
  std::string Content_MD5 ="";
  std::string CanonicalizedResource = uri.c_str();
  std::string StringToSign = HTTP_Verb + "\n" + Content_MD5 + "\n" + Content_Type + "\n" + date + "\n" +CanonicalizedResource;
  char key[YourSecretAccessKeyID.length()+1] ;
  strcpy(key, YourSecretAccessKeyID.c_str());
  const char * data = StringToSign.c_str();
  unsigned char* digest;
  digest = HMAC(EVP_sha1(), key, strlen(key), (unsigned char*)data, strlen(data), NULL, NULL);
  std::string signature = base64_encode(digest, 20);
  return signature;

}


int RemoteS3Request::submit_http_put_request_s3()
{
  CURL *curl_handle;
  CURLcode res;

  string block_id = req->path; 
  string tmp = req->path;
  string location = req->dest;
  size_t block_size = req->sizeleft;
  ldout(cct,10) << __func__  << block_id << dendl;
  ldout(cct,10) << __func__  << "dest : " << req->dest << dendl;
  
  const char x1 = '/';
  const char y2 = '_';
  std::replace(block_id.begin(), block_id.end(), x1, y2);
  ldout(cct,10) << __func__  << "dest22 : " << req->dest << dendl;
   
  std::size_t pos = block_id.find_first_of("_");
  string bucket = block_id.substr(0, pos);
  std::size_t found = block_id.find_last_of("_");
  string object = block_id.substr(pos+1, found-pos-1);
  ldout(cct,10) << __func__  << block_id << dendl;

//  string uri = "/"+bucket+"/" + block_id;
  string uri = "/"+bucket+ "/"+ object;
  string AWSAccessKeyId="TX2XS2M6LVH5WJWBCW53";
  string YourSecretAccessKeyID="BKg6KC5DpUhWDRukuINZidEv06vbTyZQybj2NiIu";
  string date = get_date();
  string signature = sign_s3_request2("PUT", uri, date, YourSecretAccessKeyID, AWSAccessKeyId);
  string Authorization = "AWS "+ AWSAccessKeyId +":" + signature;
  string loc =  "http://" + location + uri;
  string auth="Authorization: " + Authorization;
  string timestamp="Date: " + date;
  string user_agent="User-Agent: rgw_datacache";
  string content_type="Content-Type: text/plain";
  string custom_header="Block-id:" + block_id.substr(found+1);
  
  string file = cct->_conf->rgw_datacache_path + "/"+ block_id ;
  FILE * hd_src;
  hd_src = fopen(file.c_str(), "r");
  
  curl_handle = curl_easy_init();
  if(curl_handle) {
    struct curl_slist *chunk = NULL;
    chunk = curl_slist_append(chunk, auth.c_str());
    chunk = curl_slist_append(chunk, timestamp.c_str());
    chunk = curl_slist_append(chunk, user_agent.c_str());
    chunk = curl_slist_append(chunk, content_type.c_str());
	chunk = curl_slist_append(chunk, custom_header.c_str());
	chunk = curl_slist_append(chunk, "CACHE_PUT_REQ:rgw_datacache");
    res = curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, chunk); //set headers
    curl_easy_setopt(curl_handle, CURLOPT_URL, loc.c_str());
    curl_easy_setopt(curl_handle, CURLOPT_UPLOAD, 1L);
    curl_easy_setopt(curl_handle, CURLOPT_READFUNCTION, read_callback);
	curl_easy_setopt(curl_handle, CURLOPT_READDATA, hd_src);
	curl_easy_setopt(curl_handle, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl_handle, CURLOPT_FAILONERROR, 1L);
//	curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)file_info.st_size);
    curl_easy_setopt(curl_handle, CURLOPT_INFILESIZE, block_size);
    res = curl_easy_perform(curl_handle); //run the curl command
    curl_easy_cleanup(curl_handle);
  }
  fclose(hd_src);
  curl_global_cleanup();
  const char x = '/';
  const char y = '_';
  std::replace(tmp.begin(), tmp.end(), x, y);
  location = cct->_conf->rgw_datacache_path + "/" + tmp;
  remove(location.c_str());

  if(res == CURLE_HTTP_RETURNED_ERROR) {
    ldout(cct,10) << "__func__ " << " CURLE_HTTP_RETURNED_ERROR" <<curl_easy_strerror(res) << dendl;
    return -1;
  } 
  if (res != CURLE_OK)
        return -1;
  return 0;
  }


void RemoteS3Request::run() {

  ldout(cct, 20) << __func__  <<dendl;
  int max_retries = cct->_conf->max_remote_retries;
  int r = 0;
  if (req->req_type == 0) {
  for (int i=0; i<max_retries; i++ ){
    if(!(r = submit_http_get_request_s3()) && (req->s.size() == req->read_len)){
       ldout(cct, 10) <<  __func__  << "remote get success"<<req->key << " r-id "<< req->r->id << dendl;
//       req->func(req);
        req->finish();
      	return;
    }
    if(req->s.size() != req->read_len){
//#if(req->bl->length() != r->read_len){
       req->s.clear();
    }
    req->s.clear();
    }

    if (r == ECANCELED) {
    ldout(cct, 0) << "ERROR: " << __func__  << "(): remote s3 request for failed, obj="<<req->key << dendl;
    req->r->result = -1;
    req->aio->put(*(req->r));
    return;
    }
  }
  else {
  for (int i=0; i<max_retries; i++ ){
	if(!(r = submit_http_put_request_s3()))
	  return;
	if (r == ECANCELED) {  
      ldout(cct, 0) << "ERROR: " << __func__  << dendl;
      return;
	}
	  
	return;
    }	

  }
}

