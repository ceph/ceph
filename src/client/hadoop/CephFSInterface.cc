// -*- mode:c++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
#include "CephFSInterface.h"

#include "client/libceph.h"
#include "config.h"
#include "msg/SimpleMessenger.h"
#include "common/Timer.h"

#include <sys/stat.h>

using namespace std;
const static int IP_ADDR_LENGTH = 24;//a buffer size; may want to up for IPv6.
static int path_size;
/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_initializeClient
 * Signature: (Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1initializeClient
  (JNIEnv *env, jobject obj, jstring j_args )
{
  dout(3) << "CephFSInterface: Initializing Ceph client:" << dendl;
  const char *c_args = env->GetStringUTFChars(j_args, 0);
  if (c_args == NULL) return false; //out of memory!
  string args(c_args);
  path_size = 64; //reasonable starting point?

  //construct an arguments vector
  vector<string> args_vec;
  size_t i = 0;
  size_t j = 0;
  while (1) {
    j = args.find(' ', i);
    if (j == string::npos) {
      if (i == 0) { //there were no spaces? That can't happen!
	env->ReleaseStringUTFChars(j_args, c_args);
	return false;
      }
      //otherwise it's the last argument, so push it on and exit loop
      args_vec.push_back(args.substr(i, args.size()));
      break;
    }
    if (j!=i) //if there are two spaces in a row, dont' make a new arg
      args_vec.push_back(args.substr(i, j-i));
    i = j+1;
  }

  //convert to array
  const char ** argv = new const char*[args_vec.size()];
  for (size_t i = 0; i < args_vec.size(); ++i)
    argv[i] = args_vec[i].c_str();

  int r = ceph_initialize(args_vec.size(), argv);
  env->ReleaseStringUTFChars(j_args, c_args);
  delete argv;

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

  char *path = new char[path_size];
  int r = ceph_getcwd(path, path_size);
  if (r==-ERANGE) { //path is too short
    path_size = ceph_getcwd(path, 0) * 1.2; //leave some extra
    delete path;
    path = new char[path_size];
    ceph_getcwd(path, path_size);
  }
  jstring j_path = env->NewStringUTF(path);
  delete path;
  return j_path;
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

  const char *c_path = env->GetStringUTFChars(j_path, 0);
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

  const char *c_path = env->GetStringUTFChars(j_path, 0);
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
  (JNIEnv *env, jobject, jstring j_path)
{
  dout(10) << "CephFSInterface: In mkdir" << dendl;

  const char *c_path = env->GetStringUTFChars(j_path, 0);
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
  (JNIEnv *env, jobject, jstring j_path)
{
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return false;
  dout(10) << "CephFSInterface: In unlink for path " << c_path <<  ":" << dendl;
  int result = ceph_unlink(c_path);
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
  const char *c_from = env->GetStringUTFChars(j_from, 0);
  if (c_from == NULL) return false;
  const char *c_to   = env->GetStringUTFChars(j_to,   0);
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

  const char *c_path = env->GetStringUTFChars(j_path, 0);
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

  const char *c_path = env->GetStringUTFChars(j_path, 0);
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
 * Method:    ceph_isfile
 * Signature: (Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1isfile
  (JNIEnv *env, jobject obj, jstring j_path)
{
  dout(10) << "In isfile" << dendl;

  struct stat stbuf;

  const char *c_path = env->GetStringUTFChars(j_path, 0);
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

  const char *c_path = env->GetStringUTFChars(j_path, 0);
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
(JNIEnv *env, jobject obj, jstring j_path)
{
  dout(10) << "In getdir" << dendl;

  // get the directory listing
  list<string> contents;
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return NULL;
  DIR *dirp;
  ceph_opendir(c_path, &dirp);
  int r;
  int buflen = 100; //good default?
  char *buf = new char[buflen];
  string *ent;
  int bufpos;
  while (1) {
    r = ceph_getdnames(dirp, buf, buflen);
    if (r==-ERANGE) { //expand the buffer
      delete buf;
      buflen *= 2;
      buf = new char[buflen];
      continue;
    }
    if (r<=0) break;

    //if we make it here, we got at least one name
    bufpos = 0;
    while (bufpos<r) {//make new strings and add them to listing
      ent = new string(buf+bufpos);
      if (ent->compare(".") && ent->compare(".."))
	//we DON'T want to include dot listings; Hadoop gets confused
	contents.push_back(*ent);
      bufpos+=ent->size()+1;
      delete ent;
    }
  }
  delete buf;
  ceph_closedir(dirp);
  env->ReleaseStringUTFChars(j_path, c_path);
  
  if (r < 0) return NULL;

  dout(10) << "checking for empty dir" << dendl;
  int dir_size = contents.size();
  assert ( dir_size>= 0);

  // Create a Java String array of the size of the directory listing
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
(JNIEnv *env, jobject, jstring j_path, jint mode)
{
  dout(10) << "In Hadoop mk_dirs" << dendl;

  //get c-style string and make the call, clean up the string...
  jint result;
  const char *c_path = env->GetStringUTFChars(j_path, 0);
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
(JNIEnv *env, jobject obj, jstring j_path)
{
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
  const char *c_path = env->GetStringUTFChars(j_path, 0);
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


  const char *c_path = env->GetStringUTFChars(j_path, 0);
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
(JNIEnv *env, jobject ojb, jint fh)
{
  dout(10) << "In CephFileSystem::ceph_close" << dendl;

  return ceph_close(fh);
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_setPermission
 * Signature: (Ljava/lang/String;I)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1setPermission
(JNIEnv *env, jobject obj, jstring j_path, jint j_new_mode)
{
  const char *c_path = env->GetStringUTFChars(j_path, 0);
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
 * Method:    ceph_stat
 * Signature: (Ljava/lang/String;Lorg/apache/hadoop/fs/ceph/CephFileSystem/Stat;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1stat
(JNIEnv *env, jobject obj, jstring j_path, jobject j_stat)
{
  //setup variables
  struct stat_precise st;
  const char *c_path = env->GetStringUTFChars(j_path, 0);
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
  //do actual lstat
  int r = ceph_lstat_precise(c_path, &st);
  env->ReleaseStringUTFChars(j_path, c_path);

  if (r < 0) return false; //fail out; file DNE or Ceph broke

  //put variables from struct stat into Java
  env->SetLongField(j_stat, c_size_id, (long)st.st_size);
  env->SetBooleanField(j_stat, c_dir_id, (0 != S_ISDIR(st.st_mode)));
  env->SetLongField(j_stat, c_block_id, (long)st.st_blksize);
  env->SetLongField(j_stat, c_mod_id, (long long)st.st_mtime_sec*1000
		    +st.st_mtime_micro/1000);
  env->SetLongField(j_stat, c_access_id, (long long)st.st_atime_sec*1000
		    +st.st_atime_micro/1000);
  env->SetIntField(j_stat, c_mode_id, (int)st.st_mode);

  //return happy
  return true;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_statfs
 * Signature: (Ljava/lang/String;Lorg/apache/hadoop/fs/ceph/CephFileSystem/CephStat;)I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1statfs
(JNIEnv *env, jobject obj, jstring j_path, jobject j_cephstat)
{
  //setup variables
  struct statvfs stbuf;
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return -ENOMEM;
  jclass cls = env->GetObjectClass(j_cephstat);
  if (cls == NULL) return 1; //JVM error of some kind
  jfieldID c_capacity_id = env->GetFieldID(cls, "capacity", "J");
  jfieldID c_used_id = env->GetFieldID(cls, "used", "J");
  jfieldID c_remaining_id = env->GetFieldID(cls, "remaining", "J");

  //do the statfs
  int r = ceph_statfs(c_path, &stbuf);
  env->ReleaseStringUTFChars(j_path, c_path);


  if (r!=0) return r; //something broke

  //place info into Java; convert from bytes to kilobytes
  env->SetLongField(j_cephstat, c_capacity_id,
		    (long)stbuf.f_blocks*stbuf.f_bsize/1024);
  env->SetLongField(j_cephstat, c_used_id,
		    (long)(stbuf.f_blocks-stbuf.f_bavail)*stbuf.f_bsize/1024);
  env->SetLongField(j_cephstat, c_remaining_id,
		    (long)stbuf.f_bavail*stbuf.f_bsize/1024);
  return r;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_replication
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1replication
(JNIEnv *env, jobject obj, jstring j_path)
{
  //get c-string of path, send off to libceph, release c-string, return
  const char *c_path = env->GetStringUTFChars(j_path, 0);
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
(JNIEnv *env, jobject obj, jint j_fh, jlong j_offset)
{
  //get the address
  char *address = new char[IP_ADDR_LENGTH];
  int r = ceph_get_file_stripe_address(j_fh, j_offset, address, IP_ADDR_LENGTH);
  if (r == -ERANGE) {//buffer's too small
    delete address;
    int size = ceph_get_file_stripe_address(j_fh, j_offset, address, 0);
    address = new char[size];
    r = ceph_get_file_stripe_address(j_fh, j_offset, address, size);
  }
  if (r != 0) { //some rather worse problem
    if (r == -EINVAL) return NULL; //ceph thinks there are no OSDs
  }
  //make java String of address
  jstring j_addr = env->NewStringUTF(address);
  delete address;
  return j_addr;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_setTimes
 * Signature: (Ljava/lang/String;JJ)I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1setTimes
(JNIEnv *env, jobject obj, jstring j_path, jlong mtime, jlong atime)
{
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if(c_path == NULL) return -ENOMEM;

  //build the mask for ceph_setattr
  int mask = 0;
  if (mtime!=-1) mask = CEPH_SETATTR_MTIME;
  if (atime!=-1) mask |= CEPH_SETATTR_ATIME;
  //build a struct stat and fill it in!
  //remember to convert from millis to seconds and microseconds
  stat_precise attr;
  attr.st_mtime_sec = mtime / 1000;
  attr.st_mtime_micro = (mtime % 1000) * 1000;
  attr.st_atime_sec = atime / 1000;
  attr.st_atime_micro = (atime % 1000) * 1000;
  return ceph_setattr_precise(c_path, &attr, mask);
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
  jbyte *j_buffer_ptr = env->GetByteArrayElements(j_buffer, NULL);
  if (j_buffer_ptr == NULL) return -ENOMEM;
  char *c_buffer = (char*) j_buffer_ptr;

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
  jbyte *j_buffer_ptr = env->GetByteArrayElements(j_buffer, NULL);
  if (j_buffer_ptr == NULL) return -ENOMEM;
  char *c_buffer = (char*) j_buffer_ptr;

  // Step 2: pointer arithmetic to start in the right buffer position
  c_buffer += (int)buffer_offset;

  // Step 3: do the write
  result = ceph_write((int)fh, c_buffer, length, -1);
  
  // Step 4: release the pointer to the buffer
  env->ReleaseByteArrayElements(j_buffer, j_buffer_ptr, 0);

  return result;
}
