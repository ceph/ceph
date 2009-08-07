// -*- mode:c++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
#include "CephFSInterface.h"

#include "client/libceph.h"
#include "config.h"
#include "msg/SimpleMessenger.h"
#include "common/Timer.h"

#include <sys/stat.h>

using namespace std;
/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_initializeClient
 * Signature: (Ljava/lang/String;)Z
 */

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_initializeClient
 * Signature: (Ljava/lang/String;Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1initializeClient
  (JNIEnv * env, jobject obj, jstring j_debug_level, jstring j_mon_addr)
{
  dout(3) << "CephFSInterface: Initializing Ceph client:" << dendl;

  const char* c_debug_level = env->GetStringUTFChars(j_debug_level, 0);
  if (c_debug_level == NULL) return false; //out of memory!
  const char* c_mon_addr = env->GetStringUTFChars(j_mon_addr, 0);
  if(c_mon_addr == NULL) {
    env->ReleaseStringUTFChars(j_debug_level, c_debug_level);
    return false;
  }
  //construct an arguments array
  const char *argv[10];
  int argc = 0;
  argv[argc++] = "CephFSInterface";
  argv[argc++] = "-m";
  argv[argc++] = c_mon_addr;
  argv[argc++] = "--debug_client";
  argv[argc++] = c_debug_level;

  int r = ceph_initialize(argc, argv);
  env->ReleaseStringUTFChars(j_debug_level, c_debug_level);
  env->ReleaseStringUTFChars(j_mon_addr, c_mon_addr);
  if (r < 0) return false;
  r = ceph_mount();
  if (r < 0) return false;
  return true;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_getcwd
 * Signature: (J)Ljava/lang/String;
 * Returns the current working directory.
 */
JNIEXPORT jstring JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1getcwd
  (JNIEnv *env, jobject obj)
{
  dout(10) << "CephFSInterface: In getcwd" << dendl;

  string path;
  ceph_getcwd(path);
  return env->NewStringUTF(path.c_str());
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_setcwd
 * Signature: (Ljava/lang/String;)Z
 *
 * Changes the working directory.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1setcwd
(JNIEnv *env, jobject obj, jstring j_path)
{
  dout(10) << "CephFSInterface: In setcwd" << dendl;

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  if(c_path == NULL ) return false;
  jboolean success = (0 <= ceph_chdir(c_path)) ? JNI_TRUE : JNI_FALSE; 
  env->ReleaseStringUTFChars(j_path, c_path);
  return success;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_rmdir
 * Signature: (Ljava/lang/String;)Z
 * Removes an empty directory.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1rmdir
  (JNIEnv *env, jobject, jstring j_path)
{
  dout(10) << "CephFSInterface: In rmdir" << dendl;

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  if(c_path == NULL ) return false;
  jboolean success = (0 == ceph_rmdir(c_path)) ? JNI_TRUE : JNI_FALSE; 
  env->ReleaseStringUTFChars(j_path, c_path);
  return success;
}


/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_mkdir
 * Signature: (Ljava/lang/String;)Z
 * Creates a directory with full permissions.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1mkdir
  (JNIEnv * env, jobject, jstring j_path)
{
  dout(10) << "CephFSInterface: In mkdir" << dendl;

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return false;
  jboolean success = (0 == ceph_mkdir(c_path, 0xFF)) ? JNI_TRUE : JNI_FALSE; 
  env->ReleaseStringUTFChars(j_path, c_path);
  return success;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_unlink
 * Signature: (Ljava/lang/String;)Z
 * Unlinks a path.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1unlink
  (JNIEnv * env, jobject, jstring j_path)
{
  const char* c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return false;
  dout(10) << "CephFSInterface: In unlink for path " << c_path <<  ":" << dendl;
  // is it a file or a directory?
  struct stat stbuf;
  int stat_result = ceph_lstat(c_path, &stbuf);
  if (stat_result < 0) {// then the path doesn't even exist
    dout(0) << "ceph_unlink: path " << c_path << " does not exist" << dendl;
    env->ReleaseStringUTFChars(j_path, c_path);
    return false;
  }
  int result;
  if (0 != S_ISDIR(stbuf.st_mode)) { // it's a directory
    dout(10) << "ceph_unlink: path " << c_path << " is a directory. Calling client->rmdir()" << dendl;
    result = ceph_rmdir(c_path);
  }
  else if (0 != S_ISREG(stbuf.st_mode)) { // it's a file
    dout(10) << "ceph_unlink: path " << c_path << " is a file. Calling client->unlink()" << dendl;
    result = ceph_unlink(c_path);
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
 * Signature: (Ljava/lang/String;Ljava/lang/String;)Z
 * Renames a file.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1rename
  (JNIEnv *env, jobject, jstring j_from, jstring j_to)
{
  dout(10) << "CephFSInterface: In rename" << dendl;
  const char* c_from = env->GetStringUTFChars(j_from, 0);
  if (c_from == NULL) return false;
  const char* c_to   = env->GetStringUTFChars(j_to,   0);
  if (c_to == NULL) {
    env->ReleaseStringUTFChars(j_from, c_from);
    return false;
  }

  jboolean success = (0 <= ceph_rename(c_from, c_to)) ? JNI_TRUE : JNI_FALSE; 
  env->ReleaseStringUTFChars(j_from, c_from);
  env->ReleaseStringUTFChars(j_to, c_to);
  return success;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_exists
 * Signature: (Ljava/lang/String;)Z
 * Returns true if the path exists.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1exists
(JNIEnv *env, jobject, jstring j_path)
{

  dout(10) << "CephFSInterface: In exists" << dendl;

  struct stat stbuf;

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return false;
  dout(10) << "Attempting lstat with file " << c_path << ":" << dendl;
  int result = ceph_lstat(c_path, &stbuf);
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
 * Signature: (Ljava/lang/String;)J
 * Returns the block size. Size is -1 if the file
 * does not exist.
 * TODO: see if Hadoop wants something more like stripe size
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1getblocksize
  (JNIEnv *env, jobject obj, jstring j_path)
{
  dout(10) << "In getblocksize" << dendl;

  //struct stat stbuf;
  
  jint result;

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return -ENOMEM;
  // we need to open the file to retrieve the stripe size
  dout(10) << "CephFSInterface: getblocksize: opening file" << dendl;
  int fh = ceph_open(c_path, O_RDONLY);  
  env->ReleaseStringUTFChars(j_path, c_path);
  if (fh < 0) return fh;

  result = ceph_get_file_stripe_unit(fh);

  int close_result = ceph_close(fh);
  assert (close_result > -1);

  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_getfilesize
 * Signature: (Ljava/lang/String;)J
 * Returns the file size, or -1 on failure.
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1getfilesize
  (JNIEnv *env, jobject, jstring j_path)
{
  dout(10) << "In getfilesize" << dendl;

  struct stat stbuf;

  jlong result;

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return -ENOMEM;
  result = ceph_lstat(c_path, &stbuf);
  env->ReleaseStringUTFChars(j_path, c_path);

  if (result < 0) return result;
  else result = stbuf.st_size;

  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_isfile
 * Signature: (Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1isfile
  (JNIEnv *env, jobject obj, jstring j_path)
{
  dout(10) << "In isfile" << dendl;

  struct stat stbuf;

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return false;
  int result = ceph_lstat(c_path, &stbuf);
  env->ReleaseStringUTFChars(j_path, c_path);

  // if the stat call failed, it's definitely not a file...
  if (0 > result) return false; 

  // check the stat result
  return (!(0 == S_ISREG(stbuf.st_mode)));
}


/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_isdirectory
 * Signature: (Ljava/lang/String;)Z
 * Returns true if the path is a directory.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1isdirectory
  (JNIEnv *env, jobject, jstring j_path)
{
  dout(10) << "In isdirectory" << dendl;

  struct stat stbuf;

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return false;
  int result = ceph_lstat(c_path, &stbuf);
  env->ReleaseStringUTFChars(j_path, c_path);

  // if the stat call failed, it's definitely not a directory...
  if (0 > result) return JNI_FALSE; 

  // check the stat result
  return (!(0 == S_ISDIR(stbuf.st_mode)));
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_getdir
 * Signature: (Ljava/lang/String;)[Ljava/lang/String;
 * Returns a Java array of Strings with the directory contents
 */
JNIEXPORT jobjectArray JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1getdir
(JNIEnv *env, jobject obj, jstring j_path) {

  dout(10) << "In getdir" << dendl;

  // get the directory listing
  list<string> contents;
  const char* c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return NULL;
  int result = ceph_getdir(c_path, contents);
  env->ReleaseStringUTFChars(j_path, c_path);
  
  if (result < 0) return NULL;

  dout(10) << "checking for empty dir" << dendl;
  int dir_size = contents.size();
  assert ( dir_size>= 0);

  // Create a Java String array of the size of the directory listing
  // jstring blankString = env->NewStringUTF("");
  jclass stringClass = env->FindClass("java/lang/String");
  if (stringClass == NULL) {
    dout(0) << "ERROR: java String class not found; dying a horrible, painful death" << dendl;
    assert(0);
  }
  jobjectArray dirListingStringArray = (jobjectArray) env->NewObjectArray(dir_size, stringClass, NULL);
  if(dirListingStringArray == NULL) return NULL;

  // populate the array with the elements of the directory list
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
 * Signature: (Ljava/lang/String;I)I
 * Create the specified directory and any required intermediate ones.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1mkdirs
(JNIEnv *env, jobject, jstring j_path, jint mode) {
  dout(10) << "In Hadoop mk_dirs" << dendl;

  //get c-style string and make the call, clean up the string...
  jint result;
  const char* c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return -ENOMEM;
  result = ceph_mkdirs(c_path, mode);
  env->ReleaseStringUTFChars(j_path, c_path);

  //...and return
  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_open_for_append
 * Signature: (Ljava/lang/String;)I
 * Open a file for writing
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1open_1for_1append
(JNIEnv *env, jobject obj, jstring j_path){
  dout(10) << "In hadoop open_for_append" << dendl;

  jint result;

  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return -ENOMEM;
  result = ceph_open(c_path, O_WRONLY|O_CREAT|O_APPEND);
  env->ReleaseStringUTFChars(j_path, c_path);

  return result;
}


/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_open_for_read
 * Signature: (Ljava/lang/String;)I
 * Open a file for reading.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1open_1for_1read
  (JNIEnv *env, jobject obj, jstring j_path)
{
  dout(10) << "In open_for_read" << dendl;

  jint result; 

  // open as read-only: flag = O_RDONLY
  const char* c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return -ENOMEM;
  result = ceph_open(c_path, O_RDONLY);
  env->ReleaseStringUTFChars(j_path, c_path);

  // returns file handle, or -1 on failure
  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_open_for_overwrite
 * Signature: (Ljava/lang/String;)I
 * Opens a file for overwriting; creates it if necessary.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1open_1for_1overwrite
  (JNIEnv *env, jobject obj, jstring j_path, jint mode)
{
  dout(10) << "In open_for_overwrite" << dendl;

  jint result; 


  const char* c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return -ENOMEM;
  result = ceph_open(c_path, O_WRONLY|O_CREAT|O_TRUNC, mode);
  env->ReleaseStringUTFChars(j_path, c_path);

  // returns file handle, or -1 on failure
  return result;       
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_close
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1close
(JNIEnv * env, jobject ojb, jint fh) {
  dout(10) << "In CephFileSystem::ceph_close" << dendl;

  return ceph_close(fh);
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_setPermission
 * Signature: (Ljava/lang/String;I)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1setPermission
(JNIEnv *env, jobject obj, jstring j_path, jint j_new_mode) {
  const char* c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return false;
  int result = ceph_chmod(c_path, j_new_mode);
  env->ReleaseStringUTFChars(j_path, c_path);

  return (result==0);
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_kill_client
 * Signature: (J)Z
 * 
 * Closes the Ceph client.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1kill_1client
  (JNIEnv *env, jobject obj)
{  
  ceph_deinitialize();  
  return true;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_replication
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1replication
(JNIEnv *env, jobject obj, jstring j_path) {
  //get c-string of path, send off to libceph, release c-string, return
  const char* c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return -ENOMEM;
  int replication = ceph_get_file_replication(c_path);
  env->ReleaseStringUTFChars(j_path, c_path);
  return replication;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_hosts
 * Signature: (IJ)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1hosts
(JNIEnv * env, jobject obj, jint j_fh, jlong j_offset) {
  //get the address
  string address;
  ceph_get_file_stripe_address(j_fh, j_offset, address);
  //make java String of address
  return env->NewStringUTF(address.c_str());
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephInputStream
 * Method:    ceph_read
 * Signature: (JI[BII)I
 * Reads into the given byte array from the current position.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephInputStream_ceph_1read
  (JNIEnv *env, jobject obj, jint fh, jbyteArray j_buffer, jint buffer_offset, jint length)
{
  dout(10) << "In read" << dendl;


  // IMPORTANT NOTE: Hadoop read arguments are a bit different from POSIX so we
  // have to convert.  The read is *always* from the current position in the file,
  // and buffer_offset is the location in the *buffer* where we start writing.

  jint result; 

  // Step 1: get a pointer to the buffer.
  jbyte* j_buffer_ptr = env->GetByteArrayElements(j_buffer, NULL);
  if (j_buffer_ptr == NULL) return -ENOMEM;
  char* c_buffer = (char*) j_buffer_ptr;

  // Step 2: pointer arithmetic to start in the right buffer position
  c_buffer += (int)buffer_offset;

  // Step 3: do the read
  result = ceph_read((int)fh, c_buffer, length, -1);

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
  (JNIEnv *env, jobject obj, jint fh, jlong pos)
{
  dout(10) << "In CephInputStream::seek_from_start" << dendl;

  return ceph_lseek(fh, pos, SEEK_SET);
}

JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephInputStream_ceph_1getpos
  (JNIEnv *env, jobject obj, jint fh)
{
  dout(10) << "In CephInputStream::ceph_getpos" << dendl;

  // seek a distance of 0 to get current offset
  return ceph_lseek(fh, 0, SEEK_CUR);  
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephInputStream
 * Method:    ceph_close
 * Signature: (JI)I
 * Closes the file.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephInputStream_ceph_1close
  (JNIEnv *env, jobject obj, jint fh)
{
  dout(10) << "In CephInputStream::ceph_close" << dendl;

  return ceph_close(fh);
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephOutputStream
 * Method:    ceph_seek_from_start
 * Signature: (JIJ)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephOutputStream_ceph_1seek_1from_1start
  (JNIEnv *env, jobject obj, jint fh, jlong pos)
{
  dout(10) << "In CephOutputStream::ceph_seek_from_start" << dendl;

   return ceph_lseek(fh, pos, SEEK_SET);  
 }


/*
 * Class:     org_apache_hadoop_fs_ceph_CephOutputStream
 * Method:    ceph_getpos
 * Signature: (JI)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephOutputStream_ceph_1getpos
  (JNIEnv *env, jobject obj, jint fh)
{
  dout(10) << "In CephOutputStream::ceph_getpos" << dendl;

  // seek a distance of 0 to get current offset
  return ceph_lseek(fh, 0, SEEK_CUR);
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephOutputStream
 * Method:    ceph_close
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephOutputStream_ceph_1close
  (JNIEnv *env, jobject obj, jint fh)
{
  dout(10) << "In CephOutputStream::ceph_close" << dendl;

  return ceph_close(fh);
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephOutputStream
 * Method:    ceph_write
 * Signature: (I[BII)I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephOutputStream_ceph_1write
  (JNIEnv *env, jobject obj, jint fh, jbyteArray j_buffer, jint buffer_offset, jint length)
{
  dout(10) << "In write" << dendl;

  // IMPORTANT NOTE: Hadoop write arguments are a bit different from POSIX so we
  // have to convert.  The write is *always* from the current position in the file,
  // and buffer_offset is the location in the *buffer* where we start writing.
  jint result; 

  // Step 1: get a pointer to the buffer.
  jbyte* j_buffer_ptr = env->GetByteArrayElements(j_buffer, NULL);
  if (j_buffer_ptr == NULL) return -ENOMEM;
  char* c_buffer = (char*) j_buffer_ptr;

  // Step 2: pointer arithmetic to start in the right buffer position
  c_buffer += (int)buffer_offset;

  // Step 3: do the write
  result = ceph_write((int)fh, c_buffer, length, -1);
  
  // Step 4: release the pointer to the buffer
  env->ReleaseByteArrayElements(j_buffer, j_buffer_ptr, 0);

  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_stat
 * Signature: (Ljava/lang/String;Lorg/apache/hadoop/fs/ceph/CephFileSystem/Stat;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1stat
(JNIEnv * env, jobject obj, jstring j_path, jobject j_stat) {
  //setup variables
  struct stat st;
  const char* c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return false;

  jclass cls = env->GetObjectClass(j_stat);
  if (cls == NULL) return false;
  jfieldID c_size_id = env->GetFieldID(cls, "size", "J");
  if (c_size_id == NULL) return false;
  jfieldID c_dir_id = env->GetFieldID(cls, "is_dir", "Z");
  if (c_dir_id == NULL) return false;
  jfieldID c_block_id = env->GetFieldID(cls, "block_size", "J");
  if (c_block_id == NULL) return false;
  jfieldID c_mod_id = env->GetFieldID(cls, "mod_time", "J");
  if (c_mod_id == NULL) return false;
  jfieldID c_access_id = env->GetFieldID(cls, "access_time", "J");
  if (c_access_id == NULL) return false;
  jfieldID c_mode_id = env->GetFieldID(cls, "mode", "I");
  if (c_mode_id == NULL) return false;
  jfieldID c_user_id = env->GetFieldID(cls, "user_id", "I");
  if (c_user_id == NULL) return false;
  jfieldID c_group_id = env->GetFieldID(cls, "group_id", "I");
  if (c_group_id == NULL) return false;

  //do actual lstat
  int r = ceph_lstat(c_path, &st);
  env->ReleaseStringUTFChars(j_path, c_path);

  if (r < 0) return false; //fail out; file DNE or Ceph broke

  //put variables from struct stat into Java
  env->SetLongField(j_stat, c_size_id, (long)st.st_size);
  env->SetBooleanField(j_stat, c_dir_id, (0 != S_ISDIR(st.st_mode)));
  env->SetLongField(j_stat, c_block_id, (long)st.st_blksize);
  env->SetLongField(j_stat, c_mod_id, (long long)st.st_mtime);
  env->SetLongField(j_stat, c_access_id, (long long)st.st_atime);
  env->SetIntField(j_stat, c_mode_id, (int)st.st_mode);
  env->SetIntField(j_stat, c_user_id, (int)st.st_uid);
  env->SetIntField(j_stat, c_group_id, (int)st.st_gid);

  //return happy
  return true;
}
