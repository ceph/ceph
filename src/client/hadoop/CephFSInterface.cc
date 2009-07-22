// -*- mode:c++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
#include "CephFSInterface.h"

#include "client/Client.h"
#include "config.h"
#include "msg/SimpleMessenger.h"
#include "common/Timer.h"

#include <sys/stat.h>

using namespace std;

#define dout generic_dout


/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_initializeClient
 * Signature: ()J
 * Initializes a ceph client.
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1initializeClient
(JNIEnv *env, jobject, jstring j_mon_host)
{
  dout(3) << "CephFSInterface: Initializing Ceph client:" << dendl;

  g_conf.mon_host = env->GetStringUTFChars(j_mon_host, 0);

  // parse args from CEPH_ARGS 
  vector<const char*> args; 
  env_to_vec(args);
  parse_config_options(args);

  if (g_conf.clock_tare) g_clock.tare();

  // be safe
  g_conf.use_abspaths = true;

  // get monmap
  MonMap monmap;
  MonClient mc;
  if (mc.get_monmap(&monmap) < 0)
    return 0;

  // start up network
  rank.bind();
  cout << "starting ceph client at " << rank.get_rank_addr() << std::endl;
  rank.start();

  // start client
  Client *client = new Client(rank.register_entity(entity_name_t::CLIENT(-1)), &monmap);
  client->init();
    
  // mount
  client->mount();
   
  jlong clientp = *(jlong*)&client;
  return clientp;
}



/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_copyFromLocalFile
 * Signature: (JLjava/lang/String;Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1copyFromLocalFile
(JNIEnv * env, jobject obj, jlong clientp, jstring j_local_path, jstring j_ceph_path) {

  dout(10) << "CephFSInterface: In copyFromLocalFile" << dendl;
  Client* client;
  //client = (Client*) clientp;
   client = *(Client**)&clientp;

  const char* c_local_path = env->GetStringUTFChars(j_local_path, 0);
  const char* c_ceph_path = env->GetStringUTFChars(j_ceph_path, 0);

  dout(10) << "CephFSInterface: Local source file is "<< c_local_path << " and Ceph destination file is " << c_ceph_path << dendl;
  struct stat st;
  int r = ::stat(c_local_path, &st);
  assert (r == 0);

  // open the files
  int fh_local = ::open(c_local_path, O_RDONLY);
  int fh_ceph = client->open(c_ceph_path, O_WRONLY|O_CREAT|O_TRUNC);  
  assert (fh_local > -1);
  assert (fh_ceph > -1);
  dout(10) << "CephFSInterface: local fd is " << fh_local << " and Ceph fd is " << fh_ceph << dendl;

  // get the source file size
  off_t remaining = st.st_size;
   
  // copy the file a MB at a time
  const int chunk = 1048576;
  bufferptr bp(chunk);

  while (remaining > 0) {
    off_t got = ::read(fh_local, bp.c_str(), MIN(remaining,chunk));
    assert(got > 0);
    remaining -= got;
    off_t wrote = client->write(fh_ceph, bp.c_str(), got, -1);
    assert (got == wrote);
  }
  client->close(fh_ceph);
  ::close(fh_local);

  env->ReleaseStringUTFChars(j_local_path, c_local_path);
  env->ReleaseStringUTFChars(j_ceph_path, c_ceph_path);
  
  return JNI_TRUE;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_copyToLocalFile
 * Signature: (JLjava/lang/String;Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1copyToLocalFile
(JNIEnv *env, jobject obj, jlong clientp, jstring j_ceph_path, jstring j_local_path) 
{
  Client* client;
  client = *(Client**)&clientp;
  const char* c_ceph_path = env->GetStringUTFChars(j_ceph_path, 0);
  const char* c_local_path = env->GetStringUTFChars(j_local_path, 0);

  dout(3) << "CephFSInterface: dout(3): In copyToLocalFile, copying from Ceph file " << c_ceph_path << 
    " to local file " << c_local_path << dendl;

  cout << "CephFSInterface: cout: In copyToLocalFile, copying from Ceph file " << c_ceph_path << 
    " to local file " << c_local_path << dendl;


  // get source file size
  struct stat st;
  //dout(10) << "Attempting lstat with file " << c_ceph_path << ":" << dendl;
  int r = client->lstat(c_ceph_path, &st);
  assert (r == 0);

  dout(10) << "CephFSInterface: Opening Ceph source file for read: " << dendl;
  int fh_ceph = client->open(c_ceph_path, O_RDONLY);  
  assert (fh_ceph > -1);

  dout(10) << "CephFSInterface: Opened Ceph file! Opening local destination file: " << dendl;
  int fh_local = ::open(c_local_path, O_WRONLY|O_CREAT|O_TRUNC, 0644);
  assert (fh_local > -1);

  // copy the file a chunk at a time
  const int chunk = 1048576;
  bufferptr bp(chunk);

  off_t remaining = st.st_size;
  while (remaining > 0) {
    off_t got = client->read(fh_ceph, bp.c_str(), MIN(remaining,chunk), -1);
    assert(got > 0);
    remaining -= got;
    off_t wrote = ::write(fh_local, bp.c_str(), got);
    assert (got == wrote);
  }
  client->close(fh_ceph);
  ::close(fh_local);

  env->ReleaseStringUTFChars(j_local_path, c_local_path);
  env->ReleaseStringUTFChars(j_ceph_path, c_ceph_path);
  
  return JNI_TRUE;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_getcwd
 * Signature: (J)Ljava/lang/String;
 * Returns the current working directory.
 */
JNIEXPORT jstring JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1getcwd
  (JNIEnv *env, jobject obj, jlong clientp)
{
  dout(10) << "CephFSInterface: In getcwd" << dendl;

  Client* client;
  client = *(Client**)&clientp;

  return (env->NewStringUTF(client->getcwd().c_str()));
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_setcwd
 * Signature: (JLjava/lang/String;)Z
 *
 * Changes the working directory.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1setcwd
(JNIEnv *env, jobject obj, jlong clientp, jstring j_path)
{
  dout(10) << "CephFSInterface: In setcwd" << dendl;

  Client* client;
  client = *(Client**)&clientp;

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  return (0 <= client->chdir(c_path)) ? JNI_TRUE : JNI_FALSE; 
  env->ReleaseStringUTFChars(j_path, c_path);
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_rmdir
 * Signature: (JLjava/lang/String;)Z
 * Removes an empty directory.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1rmdir
  (JNIEnv *env, jobject, jlong clientp, jstring j_path)
{
  dout(10) << "CephFSInterface: In rmdir" << dendl;

  Client* client;
  client = *(Client**)&clientp;

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  return (0 == client->rmdir(c_path)) ? JNI_TRUE : JNI_FALSE; 
  env->ReleaseStringUTFChars(j_path, c_path);
}


/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_mkdir
 * Signature: (JLjava/lang/String;)Z
 * Creates a directory with full permissions.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1mkdir
  (JNIEnv * env, jobject, jlong clientp, jstring j_path)
{
  dout(10) << "CephFSInterface: In mkdir" << dendl;

  Client* client;
  client = *(Client**)&clientp;

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  return (0 == client->mkdir(c_path, 0xFF)) ? JNI_TRUE : JNI_FALSE; 
  env->ReleaseStringUTFChars(j_path, c_path);
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_unlink
 * Signature: (JLjava/lang/String;)Z
 * Unlinks a path.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1unlink
  (JNIEnv * env, jobject, jlong clientp, jstring j_path)
{
  Client* client;
  client = *(Client**)&clientp;

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  dout(10) << "CephFSInterface: In unlink for path " << c_path <<  ":" << dendl;

  // is it a file or a directory?
  struct stat stbuf;
  int stat_result = client->lstat(c_path, &stbuf);
  if (stat_result < 0) {// then the path doesn't even exist
    dout(0) << "ceph_unlink: path " << c_path << " does not exist" << dendl;
    return false;
  }  
  int result;
  if (0 != S_ISDIR(stbuf.st_mode)) { // it's a directory
    dout(10) << "ceph_unlink: path " << c_path << " is a directory. Calling client->rmdir()" << dendl;
    result = client->rmdir(c_path);
  }
  else if (0 != S_ISREG(stbuf.st_mode)) { // it's a file
    dout(10) << "ceph_unlink: path " << c_path << " is a file. Calling client->unlink()" << dendl;
    result = client->unlink(c_path);
  }
  else {
    dout(0) << "ceph_unlink: path " << c_path << " is not a file or a directory. Failing:" << dendl;
    result = -1;
  }
    
  dout(10) << "In ceph_unlink for path " << c_path << 
    ": got result " 
       << result << ". Returning..."<< dendl;

  env->ReleaseStringUTFChars(j_path, c_path);
  return (0 == result) ? JNI_TRUE : JNI_FALSE; 
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_rename
 * Signature: (JLjava/lang/String;Ljava/lang/String;)Z
 * Renames a file.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1rename
  (JNIEnv *env, jobject, jlong clientp, jstring j_from, jstring j_to)
{
  dout(10) << "CephFSInterface: In rename" << dendl;

  Client* client;
  client = *(Client**)&clientp;

  const char* c_from = env->GetStringUTFChars(j_from, 0);
  const char* c_to   = env->GetStringUTFChars(j_to,   0);

  return (0 <= client->rename(c_from, c_to)) ? JNI_TRUE : JNI_FALSE; 
  env->ReleaseStringUTFChars(j_from, c_from);
  env->ReleaseStringUTFChars(j_to, c_to);
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_exists
 * Signature: (JLjava/lang/String;)Z
 * Returns true if the path exists.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1exists
(JNIEnv *env, jobject, jlong clientp, jstring j_path)
{

  dout(10) << "CephFSInterface: In exists" << dendl;

  Client* client;
  struct stat stbuf;
  client = *(Client**)&clientp;

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  dout(10) << "Attempting lstat with file " << c_path << ":" ;
  int result = client->lstat(c_path, &stbuf);
  dout(10) << "result is " << result << dendl;
  env->ReleaseStringUTFChars(j_path, c_path);
  if (result < 0) {
    dout(10) << "Returning false (file does not exist)" << dendl;
    return JNI_FALSE;
  }
  else {
    dout(10) << "Returning true (file exists)" << dendl;
    return JNI_TRUE;
  }
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_getblocksize
 * Signature: (JLjava/lang/String;)J
 * Returns the block size. Size is -1 if the file
 * does not exist.
 * TODO: see if Hadoop wants something more like stripe size
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1getblocksize
  (JNIEnv *env, jobject obj, jlong clientp, jstring j_path)
{
  dout(10) << "In getblocksize" << dendl;

  Client* client;
  //struct stat stbuf;
  client = *(Client**)&clientp;
  
  jint result;

  const char* c_path = env->GetStringUTFChars(j_path, 0);

  /*
  if (0 > client->lstat(c_path, &stbuf))
    result =  -1;
  else
    result = stbuf.st_blksize;
  */

  // we need to open the file to retrieve the stripe size
  dout(10) << "CephFSInterface: getblocksize: opening file" << dendl;
  int fh = client->open(c_path, O_RDONLY);  
  if (fh < 0)
    return -1;

  result = client->get_stripe_unit(fh);

  int close_result = client->close(fh);
  assert (close_result > -1);


  env->ReleaseStringUTFChars(j_path, c_path);
  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_getfilesize
 * Signature: (JLjava/lang/String;)J
 * Returns the file size, or -1 on failure.
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1getfilesize
  (JNIEnv *env, jobject, jlong clientp, jstring j_path)
{
  dout(10) << "In getfilesize" << dendl;

  Client* client;
  struct stat stbuf;
  client = *(Client**)&clientp;

  jlong result;

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  if (0 > client->lstat(c_path, &stbuf)) result =  -1; 
  else result = stbuf.st_size;
  env->ReleaseStringUTFChars(j_path, c_path);

  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_isfile
 * Signature: (JLjava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1isfile
  (JNIEnv *env, jobject obj, jlong clientp, jstring j_path)
{
  dout(10) << "In isfile" << dendl;

  Client* client;
  struct stat stbuf;
  client = *(Client**)&clientp;


  const char* c_path = env->GetStringUTFChars(j_path, 0);
  int result = client->lstat(c_path, &stbuf);

  env->ReleaseStringUTFChars(j_path, c_path);

  // if the stat call failed, it's definitely not a file...
  if (0 > result) return JNI_FALSE; 

  // check the stat result
  return (0 == S_ISREG(stbuf.st_mode)) ? JNI_FALSE : JNI_TRUE;
}


/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_isdirectory
 * Signature: (JLjava/lang/String;)Z
 * Returns true if the path is a directory.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1isdirectory
  (JNIEnv *env, jobject, jlong clientp, jstring j_path)
{
  dout(10) << "In isdirectory" << dendl;

  Client* client;
  struct stat stbuf;
  client = *(Client**)&clientp;

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  int result = client->lstat(c_path, &stbuf);
  env->ReleaseStringUTFChars(j_path, c_path);

  // if the stat call failed, it's definitely not a directory...
  if (0 > result) return JNI_FALSE; 

  // check the stat result
  return (0 == S_ISDIR(stbuf.st_mode)) ? JNI_FALSE : JNI_TRUE;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_getdir
 * Signature: (JLjava/lang/String;)[Ljava/lang/String;
 * Returns a Java array of Strings with the directory contents
 */
JNIEXPORT jobjectArray JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1getdir
(JNIEnv *env, jobject obj, jlong clientp, jstring j_path) {

  dout(10) << "In getdir" << dendl;

  Client* client;
  client = *(Client**)&clientp;

  // get the directory listing
  list<string> contents;
  const char* c_path = env->GetStringUTFChars(j_path, 0);
  int result = client->getdir(c_path, contents);
  env->ReleaseStringUTFChars(j_path, c_path);
  
  if (result < 0) return NULL;

  // Hadoop freaks out if the listing contains "." or "..".  Shrink
  // the listing size by two, or by one if the directory is the root.
  if (!contents.empty() && contents.front() == ".")
    contents.pop_front();
  if (!contents.empty() && contents.front() == "..")
    contents.pop_front();

  dout(10) << "checking for empty dir" << dendl;
  jint dir_size = contents.size();
  assert (dir_size >= 0);
		
  // Create a Java String array of the size of the directory listing
  // jstring blankString = env->NewStringUTF("");
  jclass stringClass = env->FindClass("java/lang/String");
  if (NULL == stringClass) {
    dout(0) << "ERROR: java String class not found; dying a horrible, painful death" << dendl;
    assert(0);
  }
  jobjectArray dirListingStringArray = (jobjectArray) env->NewObjectArray(dir_size, stringClass, NULL);
  
  // populate the array with the elements of the directory list,
  // omitting . and ..
  int i = 0;
  for (list<string>::iterator it = contents.begin();
       it != contents.end();
       it++) {
    if (0 == dir_size)
      dout(0) << "CephFSInterface: WARNING: adding stuff to an empty array." << dendl;
    assert (i < dir_size);
    env->SetObjectArrayElement(dirListingStringArray, i, 
			       env->NewStringUTF(it->c_str()));
    ++i;
  }
			     
  return dirListingStringArray;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_mkdirs
 * Signature: (JLjava/lang/String;I)I
 * Create the specified directory and any required intermediate ones.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1mkdirs
(JNIEnv *env, jobject, jlong clientp, jstring j_path, jint mode) {
  dout(10) << "In Hadoop mk_dirs" << dendl;
  //get our client
  Client* client;
  client = *(Client**) &clientp;

  //get c-style string and make the call, clean up the string...
  jint result;
  const char* c_path = env->GetStringUTFChars(j_path, 0);
  result = client->mkdirs(c_path, mode);
  env->ReleaseStringUTFChars(j_path, c_path);

  //...and return
  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_open_for_append
 * Signature: (JLjava/lang/String;)I
 * Open a file for writing
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1open_1for_1append
(JNIEnv *env, jobject obj, jlong clientp, jstring j_path){
  dout(10) << "In hadoop open_for_append" << dendl;

  Client* client;
  client = *(Client**)&clientp;

  jint result;

  const char *c_path = env->GetStringUTFChars(j_path, 0);
  result = client->open(c_path, O_WRONLY|O_CREAT|O_APPEND);
  env->ReleaseStringUTFChars(j_path, c_path);

  return result;
}


/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_open_for_read
 * Signature: (JLjava/lang/String;)I
 * Open a file for reading.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1open_1for_1read
  (JNIEnv *env, jobject obj, jlong clientp, jstring j_path)

{
  dout(10) << "In open_for_read" << dendl;

  Client* client;
  client = *(Client**)&clientp;

  jint result; 

  // open as read-only: flag = O_RDONLY

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  result = client->open(c_path, O_RDONLY);
  env->ReleaseStringUTFChars(j_path, c_path);

  // returns file handle, or -1 on failure
  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_open_for_overwrite
 * Signature: (JLjava/lang/String;)I
 * Opens a file for overwriting; creates it if necessary.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1open_1for_1overwrite
  (JNIEnv *env, jobject obj, jlong clientp, jstring j_path, jint mode)
{
  dout(10) << "In open_for_overwrite" << dendl;

  Client* client;
  client = *(Client**)&clientp;

  jint result; 


  const char* c_path = env->GetStringUTFChars(j_path, 0);
  result = client->open(c_path, O_WRONLY|O_CREAT|O_TRUNC, mode);
  env->ReleaseStringUTFChars(j_path, c_path);

  // returns file handle, or -1 on failure
  return result;       
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_kill_client
 * Signature: (J)Z
 * 
 * Closes the Ceph client.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1kill_1client
  (JNIEnv *env, jobject obj, jlong clientp)
{  
  Client* client;
  client = *(Client**)&clientp;

  client->unmount();
  client->shutdown();
  delete client;
  
  // wait for messenger to finish
  rank.wait();

  return true;
}



/*
 * Class:     org_apache_hadoop_fs_ceph_CephInputStream
 * Method:    ceph_read
 * Signature: (JI[BII)I
 * Reads into the given byte array from the current position.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephInputStream_ceph_1read
  (JNIEnv *env, jobject obj, jlong clientp, jint fh, jbyteArray j_buffer, jint buffer_offset, jint length)
{
  dout(10) << "In read" << dendl;


  // IMPORTANT NOTE: Hadoop read arguments are a bit different from POSIX so we
  // have to convert.  The read is *always* from the current position in the file,
  // and buffer_offset is the location in the *buffer* where we start writing.


  Client* client;
  client = *(Client**)&clientp;
  jint result; 

  // Step 1: get a pointer to the buffer.
  jbyte* j_buffer_ptr = env->GetByteArrayElements(j_buffer, NULL);
  char* c_buffer = (char*) j_buffer_ptr;

  // Step 2: pointer arithmetic to start in the right buffer position
  c_buffer += (int)buffer_offset;

  // Step 3: do the read
  result = client->read((int)fh, c_buffer, length, -1);

  // Step 4: release the pointer to the buffer
  env->ReleaseByteArrayElements(j_buffer, j_buffer_ptr, 0);
  
  return result;
}


/*
 * Class:     org_apache_hadoop_fs_ceph_CephInputStream
 * Method:    ceph_seek_from_start
 * Signature: (JIJ)J
 * Seeks to the given position.
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephInputStream_ceph_1seek_1from_1start
  (JNIEnv *env, jobject obj, jlong clientp, jint fh, jlong pos)
{
  dout(10) << "In CephInputStream::seek_from_start" << dendl;

  Client* client;
  client = *(Client**)&clientp;
  jint result; 

  result = client->lseek(fh, pos, SEEK_SET);
  
  return result;
}

JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephInputStream_ceph_1getpos
  (JNIEnv *env, jobject obj, jlong clientp, jint fh)
{
  dout(10) << "In CephInputStream::ceph_getpos" << dendl;

  Client* client;
  client = *(Client**)&clientp;
  jint result; 

  // seek a distance of 0 to get current offset
  result = client->lseek(fh, 0, SEEK_CUR);  

  return result;
}


/*
 * Class:     org_apache_hadoop_fs_ceph_CephInputStream
 * Method:    ceph_close
 * Signature: (JI)I
 * Closes the file.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephInputStream_ceph_1close
  (JNIEnv *env, jobject obj, jlong clientp, jint fh)
{
  dout(10) << "In CephInputStream::ceph_close" << dendl;

  Client* client;
  client = *(Client**)&clientp;
  jint result; 

  result = client->close(fh);

  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephOutputStream
 * Method:    ceph_seek_from_start
 * Signature: (JIJ)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephOutputStream_ceph_1seek_1from_1start
  (JNIEnv *env, jobject obj, jlong clientp, jint fh, jlong pos)
{
  dout(10) << "In CephOutputStream::ceph_seek_from_start" << dendl;

  Client* client;
  client = *(Client**)&clientp;
  jint result; 

  result = client->lseek(fh, pos, SEEK_SET);
  
  return result;
}


/*
 * Class:     org_apache_hadoop_fs_ceph_CephOutputStream
 * Method:    ceph_getpos
 * Signature: (JI)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephOutputStream_ceph_1getpos
  (JNIEnv *env, jobject obj, jlong clientp, jint fh)
{
  dout(10) << "In CephOutputStream::ceph_getpos" << dendl;

  Client* client;
  client = *(Client**)&clientp;
  jint result; 

  // seek a distance of 0 to get current offset
  result = client->lseek(fh, 0, SEEK_CUR);  

  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephOutputStream
 * Method:    ceph_close
 * Signature: (JI)I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephOutputStream_ceph_1close
  (JNIEnv *env, jobject obj, jlong clientp, jint fh)
{
  dout(10) << "In CephOutputStream::ceph_close" << dendl;

  Client* client;
  client = *(Client**)&clientp;
  jint result; 

  result = client->close(fh);

  return result;
}



/*
 * Class:     org_apache_hadoop_fs_ceph_CephOutputStream
 * Method:    ceph_write
 * Signature: (JI[BII)I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephOutputStream_ceph_1write
  (JNIEnv *env, jobject obj, jlong clientp, jint fh, jbyteArray j_buffer, jint buffer_offset, jint length)
{
  dout(10) << "In write" << dendl;

  // IMPORTANT NOTE: Hadoop write arguments are a bit different from POSIX so we
  // have to convert.  The write is *always* from the current position in the file,
  // and buffer_offset is the location in the *buffer* where we start writing.

  Client* client;
  client = *(Client**)&clientp;
  jint result; 

  // Step 1: get a pointer to the buffer.
  jbyte* j_buffer_ptr = env->GetByteArrayElements(j_buffer, NULL);
  char* c_buffer = (char*) j_buffer_ptr;

  // Step 2: pointer arithmetic to start in the right buffer position
  c_buffer += (int)buffer_offset;

  // Step 3: do the write
  result = client->write((int)fh, c_buffer, length, -1);
  
  // Step 4: release the pointer to the buffer
  env->ReleaseByteArrayElements(j_buffer, j_buffer_ptr, 0);

  return result;
}

