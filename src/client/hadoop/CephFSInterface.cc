// -*- mode:c++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "CephFSInterface.h"
#include "include/cephfs/libcephfs.h"
#include "common/ceph_argparse.h"
#include "common/config.h"
#include "msg/SimpleMessenger.h"

#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

#define dout_subsys ceph_subsys_hadoop

union ceph_mount_union_t {
  struct ceph_mount_info *cmount;
  jlong cjlong;
};

static void set_ceph_mount_info(JNIEnv *env, jobject obj, struct ceph_mount_info *cmount)
{
  jclass cls = env->GetObjectClass(obj);
  if (cls == NULL)
    return;
  jfieldID fid = env->GetFieldID(cls, "cluster", "J");
  if (fid == NULL)
    return;
  ceph_mount_union_t ceph_mount_union;
  ceph_mount_union.cjlong = 0;
  ceph_mount_union.cmount = cmount;
  env->SetLongField(obj, fid, ceph_mount_union.cjlong);
}

static struct ceph_mount_info *get_ceph_mount_t(JNIEnv *env, jobject obj)
{
  jclass cls = env->GetObjectClass(obj);
  jfieldID fid = env->GetFieldID(cls, "cluster", "J");
  if (fid == NULL)
    return NULL;
  ceph_mount_union_t ceph_mount_union;
  ceph_mount_union.cjlong = env->GetLongField(obj, fid);
  return ceph_mount_union.cmount;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_initializeClient
 * Signature: (Ljava/lang/String;I)Z
 *
 * Performs any necessary setup to allow general use of the filesystem.
 * Inputs:
 *  jstring args -- a command-line style input of Ceph config params
 *  jint block_size -- the size in bytes to use for blocks
 * Returns: true on success, false otherwise
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1initializeClient
  (JNIEnv *env, jobject obj, jstring j_args, jint block_size)
{
  // Convert Java argument string to argv
  const char *c_args = env->GetStringUTFChars(j_args, 0);
  if (c_args == NULL)
    return false; //out of memory!
  string cppargs(c_args);
  char b[cppargs.length()+1];
  strcpy(b, cppargs.c_str());
  env->ReleaseStringUTFChars(j_args, c_args);
  std::vector<const char*> args;
  char *p = b;
  while (*p) {
    args.push_back(p);
    while (*p && *p != ' ')
      p++;
    if (!*p)
      break;
    *p++ = 0;
    while (*p && *p == ' ')
      p++;
  }

  // parse the arguments
  bool set_local_writes = false;
  std::string mount_root, val;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_witharg(args, i, &val, "mount_root", (char*)NULL)) {
      mount_root = val;
    } else if (ceph_argparse_flag(args, i, "set_local_pg", (char*)NULL)) {
      set_local_writes = true;
    } else {
      ++i;
    }
  }

  // connect to the cmount
  struct ceph_mount_info *cmount;
  int ret = ceph_create(&cmount, NULL);
  if (ret)
    return false;
  ceph_conf_read_file(cmount, NULL); // read config file from the default location
  ceph_conf_parse_argv(cmount, args.size(), &args[0]);
  CephContext *cct = ceph_get_mount_context(cmount);
  ldout(cct, 3) << "CephFSInterface: mounting filesystem...:" << dendl;

  ret = ceph_mount(cmount, mount_root.c_str());
  if (ret)
    return false;

  ceph_localize_reads(cmount, true);
  ceph_set_default_file_stripe_unit(cmount, block_size);
  ceph_set_default_object_size(cmount, block_size);

  if (set_local_writes) {
    ceph_set_default_preferred_pg(cmount, ceph_get_local_osd(cmount));
  }

  set_ceph_mount_info(env, obj, cmount);
  return true;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_getcwd
 * Signature: (J)Ljava/lang/String;
 *
 * Returns the current working directory.(absolute) as a jstring
 */
JNIEXPORT jstring JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1getcwd
  (JNIEnv *env, jobject obj)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  CephContext *cct = ceph_get_mount_context(cmount);
  ldout(cct, 10) << "CephFSInterface: In getcwd" << dendl;
  jstring j_path = env->NewStringUTF(ceph_getcwd(cmount));
  return j_path;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_setcwd
 * Signature: (Ljava/lang/String;)Z
 *
 * Changes the working directory.
 * Inputs:
 *  jstring j_path: The path (relative or absolute) to switch to
 * Returns: true on success, false otherwise.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1setcwd
(JNIEnv *env, jobject obj, jstring j_path)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  CephContext *cct = ceph_get_mount_context(cmount);
  ldout(cct, 10) << "CephFSInterface: In setcwd" << dendl;

  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL)
    return false;
  int ret = ceph_chdir(cmount, c_path);
  env->ReleaseStringUTFChars(j_path, c_path);
  return ret ? JNI_FALSE : JNI_TRUE;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_rmdir
 * Signature: (Ljava/lang/String;)Z
 *
 * Given a path to a directory, removes the directory.if empty.
 * Inputs:
 *  jstring j_path: The path (relative or absolute) to the directory
 * Returns: true on successful delete; false otherwise
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1rmdir
  (JNIEnv *env, jobject obj, jstring j_path)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  CephContext *cct = ceph_get_mount_context(cmount);
  ldout(cct, 10) << "CephFSInterface: In rmdir" << dendl;

  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if(c_path == NULL)
    return false;
  int ret = ceph_rmdir(cmount, c_path);
  env->ReleaseStringUTFChars(j_path, c_path);
  return ret ? JNI_FALSE : JNI_TRUE;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_unlink
 * Signature: (Ljava/lang/String;)Z
 * Given a path, unlinks it.
 * Inputs:
 *  jstring j_path: The path (relative or absolute) to the file or empty dir
 * Returns: true if the unlink occurred, false otherwise.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1unlink
  (JNIEnv *env, jobject obj, jstring j_path)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  CephContext *cct = ceph_get_mount_context(cmount);
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL)
    return false;
  ldout(cct, 10) << "CephFSInterface: In unlink for path " << c_path <<  ":" << dendl;
  int ret = ceph_unlink(cmount, c_path);
  env->ReleaseStringUTFChars(j_path, c_path);
  return ret ? JNI_FALSE : JNI_TRUE;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_rename
 * Signature: (Ljava/lang/String;Ljava/lang/String;)Z
 * Changes a given path name to a new name.
 * Inputs:
 *  jstring j_from: The path whose name you want to change.
 *  jstring j_to: The new name for the path.
 * Returns: true if the rename occurred, false otherwise
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1rename
  (JNIEnv *env, jobject obj, jstring j_from, jstring j_to)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  CephContext *cct = ceph_get_mount_context(cmount);
  ldout(cct, 10) << "CephFSInterface: In rename" << dendl;
  const char *c_from = env->GetStringUTFChars(j_from, 0);
  if (c_from == NULL)
    return false;
  const char *c_to = env->GetStringUTFChars(j_to,   0);
  if (c_to == NULL) {
    env->ReleaseStringUTFChars(j_from, c_from);
    return false;
  }
  struct stat stbuf;
  int ret = ceph_lstat(cmount, c_to, &stbuf);
  if (ret != -ENOENT) {
    // Hadoop doesn't want to overwrite files in a rename.
    env->ReleaseStringUTFChars(j_from, c_from);
    env->ReleaseStringUTFChars(j_to, c_to);
    return JNI_FALSE;
  }

  ret = ceph_rename(cmount, c_from, c_to);
  return ret ? JNI_FALSE : JNI_TRUE;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_exists
 * Signature: (Ljava/lang/String;)Z
 * Returns true if it the input path exists, false
 * if it does not or there is an unexpected failure.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1exists
(JNIEnv *env, jobject obj, jstring j_path)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  CephContext *cct = ceph_get_mount_context(cmount);
  ldout(cct, 10) << "CephFSInterface: In exists" << dendl;

  struct stat stbuf;

  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL)
    return false;
  ldout(cct, 10) << "Attempting lstat with file " << c_path << ":" << dendl;

  int ret = ceph_lstat(cmount, c_path, &stbuf);
  ldout(cct, 10) << "result is " << ret << dendl;
  env->ReleaseStringUTFChars(j_path, c_path);
  if (ret < 0) {
    ldout(cct, 10) << "Returning false (file does not exist)" << dendl;
    return JNI_FALSE;
  }
  else {
    ldout(cct, 10) << "Returning true (file exists)" << dendl;
    return JNI_TRUE;
  }
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_getblocksize
 * Signature: (Ljava/lang/String;)J
 * Get the block size for a given path.
 * Input:
 *  j_string j_path: The path (relative or absolute) you want
 *  the block size for.
 * Returns: block size (as a long) if the path exists, otherwise a negative
 *  number corresponding to the standard C++ error codes (which are positive).
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1getblocksize
  (JNIEnv *env, jobject obj, jstring j_path)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  CephContext *cct = ceph_get_mount_context(cmount);
  ldout(cct, 10) << "In getblocksize" << dendl;

  //struct stat stbuf;

  jlong result;

  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL)
    return -ENOMEM;
  // we need to open the file to retrieve the stripe size
  ldout(cct, 10) << "CephFSInterface: getblocksize: opening file" << dendl;
  int fh = ceph_open(cmount, c_path, O_RDONLY, 0);
  env->ReleaseStringUTFChars(j_path, c_path);
  if (fh < 0)
    return fh;

  result = ceph_get_file_stripe_unit(cmount, fh);

  int close_result = ceph_close(cmount, fh);
  if (close_result < 0)
    return close_result;

  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_isfile
 * Signature: (Ljava/lang/String;)Z
 * Returns true if the given path is a file; false otherwise.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1isfile
  (JNIEnv *env, jobject obj, jstring j_path)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  CephContext *cct = ceph_get_mount_context(cmount);
  ldout(cct, 10) << "In isfile" << dendl;

  struct stat stbuf;

  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL)
    return false;
  int ret = ceph_lstat(cmount, c_path, &stbuf);
  env->ReleaseStringUTFChars(j_path, c_path);

  // if the stat call failed, it's definitely not a file...
  if (ret < 0)
    return false;

  // check the stat result
  return !!S_ISREG(stbuf.st_mode);
}


/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_isdirectory
 * Signature: (Ljava/lang/String;)Z
 * Returns true if the given path is a directory, false otherwise.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1isdirectory
  (JNIEnv *env, jobject obj, jstring j_path)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  CephContext *cct = ceph_get_mount_context(cmount);
  ldout(cct, 10) << "In isdirectory" << dendl;

  struct stat stbuf;

  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL)
    return false;
  int result = ceph_lstat(cmount, c_path, &stbuf);
  env->ReleaseStringUTFChars(j_path, c_path);

  // if the stat call failed, it's definitely not a directory...
  if (result < 0)
    return JNI_FALSE;

  // check the stat result
  return !!S_ISDIR(stbuf.st_mode);
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_getdir
 * Signature: (Ljava/lang/String;)[Ljava/lang/String;
 * Get the contents of a given directory.
 * Inputs:
 *  jstring j_path: The path (relative or absolute) to the directory.
 * Returns: A Java String[] of the contents of the directory, or
 *  NULL if there is an error (ie, path is not a dir). This listing
 *  will not contain . or .. entries.
 */
JNIEXPORT jobjectArray JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1getdir
(JNIEnv *env, jobject  obj, jstring j_path)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  CephContext *cct = ceph_get_mount_context(cmount);
  ldout(cct, 10) << "In getdir" << dendl;

  // get the directory listing
  list<string> contents;
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return NULL;
  struct ceph_dir_result *dirp;
  int r;
  r = ceph_opendir(cmount, c_path, &dirp);
  if (r<0) {
    env->ReleaseStringUTFChars(j_path, c_path);
    return NULL;
  }
  int buflen = 100; //good default?
  char *buf = new char[buflen];
  string *ent;
  int bufpos;
  while (1) {
    r = ceph_getdnames(cmount, dirp, buf, buflen);
    if (r==-ERANGE) { //expand the buffer
      delete [] buf;
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
  delete [] buf;
  ceph_closedir(cmount, dirp);
  env->ReleaseStringUTFChars(j_path, c_path);

  if (r < 0) return NULL;

  // Create a Java String array of the size of the directory listing
  jclass stringClass = env->FindClass("java/lang/String");
  if (stringClass == NULL) {
    ldout(cct, 0) << "ERROR: java String class not found; dying a horrible, painful death" << dendl;
    assert(0);
  }
  jobjectArray dirListingStringArray = (jobjectArray) env->NewObjectArray(contents.size(), stringClass, NULL);
  if(dirListingStringArray == NULL) return NULL;

  // populate the array with the elements of the directory list
  int i = 0;
  for (list<string>::iterator it = contents.begin();
       it != contents.end();
       ++it) {
    env->SetObjectArrayElement(dirListingStringArray, i,
			       env->NewStringUTF(it->c_str()));
    ++i;
  }

  return dirListingStringArray;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_mkdirs
 * Signature: (Ljava/lang/String;I)I
 * Create the specified directory and any required intermediate ones with the
 * given mode.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1mkdirs
(JNIEnv *env, jobject obj, jstring j_path, jint mode)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  CephContext *cct = ceph_get_mount_context(cmount);
  ldout(cct, 10) << "In Hadoop mk_dirs" << dendl;

  //get c-style string and make the call, clean up the string...
  jint result;
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL)
    return -ENOMEM;
  result = ceph_mkdirs(cmount, c_path, mode);
  env->ReleaseStringUTFChars(j_path, c_path);

  //...and return
  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_open_for_append
 * Signature: (Ljava/lang/String;)I
 * Open a file to append. If the file does not exist, it will be created.
 * Opening a dir is possible but may have bad results.
 * Inputs:
 *  jstring j_path: The path to open.
 * Returns: a jint filehandle, or a number<0 if an error occurs.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1open_1for_1append
(JNIEnv *env, jobject obj, jstring j_path)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  CephContext *cct = ceph_get_mount_context(cmount);
  ldout(cct, 10) << "In hadoop open_for_append" << dendl;

  jint result;

  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL)
    return -ENOMEM;
  result = ceph_open(cmount, c_path, O_WRONLY|O_CREAT|O_APPEND, 0);
  env->ReleaseStringUTFChars(j_path, c_path);

  return result;
}


/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_open_for_read
 * Signature: (Ljava/lang/String;)I
 * Open a file for reading.
 * Opening a dir is possible but may have bad results.
 * Inputs:
 *  jstring j_path: The path to open.
 * Returns: a jint filehandle, or a number<0 if an error occurs.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1open_1for_1read
  (JNIEnv *env, jobject obj, jstring j_path)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  CephContext *cct = ceph_get_mount_context(cmount);
  ldout(cct, 10) << "In open_for_read" << dendl;

  jint result;

  // open as read-only: flag = O_RDONLY
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL)
    return -ENOMEM;
  result = ceph_open(cmount, c_path, O_RDONLY, 0);
  env->ReleaseStringUTFChars(j_path, c_path);

  // returns file handle, or -1 on failure
  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_open_for_overwrite
 * Signature: (Ljava/lang/String;)I
 * Opens a file for overwriting; creates it if necessary.
 * Opening a dir is possible but may have bad results.
 * Inputs:
 *  jstring j_path: The path to open.
 *  jint mode: The mode to open with.
 * Returns: a jint filehandle, or a number<0 if an error occurs.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1open_1for_1overwrite
  (JNIEnv *env, jobject obj, jstring j_path, jint mode)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  CephContext *cct = ceph_get_mount_context(cmount);
  ldout(cct, 10) << "In open_for_overwrite" << dendl;

  jint result;
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL)
    return -ENOMEM;
  result = ceph_open(cmount, c_path, O_WRONLY|O_CREAT|O_TRUNC, mode);
  env->ReleaseStringUTFChars(j_path, c_path);

  // returns file handle, or -1 on failure
  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_close
 * Signature: (I)I
 * Closes a given filehandle.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1close
(JNIEnv *env, jobject obj, jint fh)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  CephContext *cct = ceph_get_mount_context(cmount);
  ldout(cct, 10) << "In CephTalker::ceph_close" << dendl;
  return ceph_close(cmount, fh);
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_setPermission
 * Signature: (Ljava/lang/String;I)Z
 * Change the mode on a path.
 * Inputs:
 *  jstring j_path: The path to change mode on.
 *  jint j_new_mode: The mode to apply.
 * Returns: true if the mode is properly applied, false if there
 *  is any error.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1setPermission
(JNIEnv *env, jobject obj, jstring j_path, jint j_new_mode)
{
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL)
    return false;
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  int result = ceph_chmod(cmount, c_path, j_new_mode);
  env->ReleaseStringUTFChars(j_path, c_path);

  return (result==0);
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_kill_client
 * Signature: (J)Z
 *
 * Closes the Ceph client. This should be called before shutting down
 * (multiple times is okay but redundant).
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1kill_1client
  (JNIEnv *env, jobject obj)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  if (!cmount)
    return true;
  ceph_shutdown(cmount);
  set_ceph_mount_info(env, obj, NULL);
  return true;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_stat
 * Signature: (Ljava/lang/String;Lorg/apache/hadoop/fs/ceph/CephFileSystem/Stat;)Z
 * Get the statistics on a path returned in a custom format defined
 *  in CephTalker.
 * Inputs:
 *  jstring j_path: The path to stat.
 *  jobject j_stat: The stat object to fill.
 * Returns: true if the stat is successful, false otherwise.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1stat
(JNIEnv *env, jobject obj, jstring j_path, jobject j_stat)
{
  //setup variables
  struct stat st;
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
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  int r = ceph_lstat(cmount, c_path, &st);
  env->ReleaseStringUTFChars(j_path, c_path);

  if (r < 0) return false; //fail out; file DNE or Ceph broke

  //put variables from struct stat into Java
  env->SetLongField(j_stat, c_size_id, st.st_size);
  env->SetBooleanField(j_stat, c_dir_id, (0 != S_ISDIR(st.st_mode)));
  env->SetLongField(j_stat, c_block_id, st.st_blksize);

  long long java_mtime(st.st_mtim.tv_sec);
  java_mtime *= 1000;
  java_mtime += st.st_mtim.tv_nsec / 1000;
  env->SetLongField(j_stat, c_mod_id, java_mtime);

  long long java_atime(st.st_atim.tv_sec);
  java_atime *= 1000;
  java_atime += st.st_atim.tv_nsec / 1000;
  env->SetLongField(j_stat, c_access_id, java_atime);

  env->SetIntField(j_stat, c_mode_id, (int)st.st_mode);

  //return happy
  return true;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_statfs
 * Signature: (Ljava/lang/String;Lorg/apache/hadoop/fs/ceph/CephFileSystem/CephStat;)I
 * Statfs a filesystem in a custom format defined in CephTalker.
 * Inputs:
 *  jstring j_path: A path on the filesystem that you wish to stat.
 *  jobject j_ceph_stat: The CephStat object to fill.
 * Returns: true if successful and the CephStat is filled; false otherwise.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1statfs
(JNIEnv *env, jobject obj, jstring j_path, jobject j_cephstat)
{
  //setup variables
  struct statvfs stbuf;
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL)
    return -ENOMEM;
  jclass cls = env->GetObjectClass(j_cephstat);
  if (cls == NULL)
    return 1; //JVM error of some kind
  jfieldID c_capacity_id = env->GetFieldID(cls, "capacity", "J");
  jfieldID c_used_id = env->GetFieldID(cls, "used", "J");
  jfieldID c_remaining_id = env->GetFieldID(cls, "remaining", "J");

  //do the statfs
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  int r = ceph_statfs(cmount, c_path, &stbuf);
  env->ReleaseStringUTFChars(j_path, c_path);
  if (r != 0)
    return r; //something broke

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
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_replication
 * Signature: (Ljava/lang/String;)I
 * Check how many times a path should be replicated (if it is
 * degraded it may not actually be replicated this often).
 * Inputs:
 *  jstring j_path: The path to check.
 * Returns: an int containing the number of times replicated.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1replication
(JNIEnv *env, jobject obj, jstring j_path)
{
  //get c-string of path, send off to libceph, release c-string, return
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL)
    return -ENOMEM;
  ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  int fh = 0;
  fh = ceph_open(cmount, c_path, O_RDONLY, 0);
  env->ReleaseStringUTFChars(j_path, c_path);
  if (fh < 0) {
	  return fh;
  }
  int replication = ceph_get_file_replication(cmount, fh);
  ceph_close(cmount, fh);
  return replication;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_hosts
 * Signature: (IJ)[Ljava/lang/String;
 * Find the IP:port addresses of the primary OSD for a given file and offset.
 * Inputs:
 *  jint j_fh: The filehandle for the file.
 *  jlong j_offset: The offset to get the location of.
 * Returns: a jstring of the location as IP, or NULL if there is an error.
 */
JNIEXPORT jobjectArray JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1hosts
(JNIEnv *env, jobject obj, jint j_fh, jlong j_offset)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  struct sockaddr_storage *ss;
  char address[30];
  jobjectArray addr_array;
  jclass string_cls;
  jstring j_addr;
  int r, n = 3; /* initial guess at # of replicas */

  for (;;) {
    ss = new struct sockaddr_storage[n];
    r = ceph_get_file_stripe_address(cmount, j_fh, j_offset, ss, n);
    if (r < 0) {
      if (r == -ERANGE) {
	delete [] ss;
	n *= 2;
	continue;
      }
      return NULL;
    }
    n = r;
    break;
  }

  /* TODO: cache this */
  string_cls = env->FindClass("java/lang/String");
  if (!string_cls)
    goto out;

  addr_array = env->NewObjectArray(n, string_cls, NULL);
  if (!addr_array)
    goto out;

  for (r = 0; r < n; r++) {
    /* Hadoop only deals with IPv4 */
    if (ss[r].ss_family != AF_INET)
      goto out;

    memset(address, 0, sizeof(address));

    inet_ntop(ss[r].ss_family, &((struct sockaddr_in *)&ss[r])->sin_addr,
	      address, sizeof(address));

    j_addr = env->NewStringUTF(address);

    env->SetObjectArrayElement(addr_array, r, j_addr);
    if (env->ExceptionOccurred())
      goto out;

    env->DeleteLocalRef(j_addr);
  }

  delete [] ss;
  return addr_array;

out:
  delete [] ss;
  return NULL;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_setTimes
 * Signature: (Ljava/lang/String;JJ)I
 * Set the mtime and atime for a given path.
 * Inputs:
 *  jstring j_path: The path to set the times for.
 *  jlong mtime: The mtime to set, in millis since epoch (-1 to not set).
 *  jlong atime: The atime to set, in millis since epoch (-1 to not set)
 * Returns: 0 if successful, an error code otherwise.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1setTimes
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
  struct stat attr;
  attr.st_mtim.tv_sec = mtime / 1000;
  attr.st_mtim.tv_nsec = (mtime % 1000) * 1000000;
  attr.st_atim.tv_sec = atime / 1000;
  attr.st_atim.tv_nsec = (atime % 1000) * 1000000;
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  return ceph_setattr(cmount, c_path, &attr, mask);
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_read
 * Signature: (JI[BII)I
 * Reads into the given byte array from the current position.
 * Inputs:
 *  jint fh: the filehandle to read from
 *  jbyteArray j_buffer: the byte array to read into
 *  jint buffer_offset: where in the buffer to start writing
 *  jint length: how much to read.
 * There'd better be enough space in the buffer to write all
 * the data from the given offset!
 * Returns: the number of bytes read on success (as jint),
 *  or an error code otherwise.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1read
  (JNIEnv *env, jobject obj, jint fh, jbyteArray j_buffer, jint buffer_offset, jint length)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  CephContext *cct = ceph_get_mount_context(cmount);
  ldout(cct, 10) << "In read" << dendl;


  // Make sure to convert the Hadoop read arguments into a
  // more ceph-friendly form
  jint result;

  // Step 1: get a pointer to the buffer.
  jbyte *j_buffer_ptr = env->GetByteArrayElements(j_buffer, NULL);
  if (j_buffer_ptr == NULL) return -ENOMEM;
  char *c_buffer = (char*) j_buffer_ptr;

  // Step 2: pointer arithmetic to start in the right buffer position
  c_buffer += (int)buffer_offset;

  // Step 3: do the read
  result = ceph_read(cmount, (int)fh, c_buffer, length, -1);

  // Step 4: release the pointer to the buffer
  env->ReleaseByteArrayElements(j_buffer, j_buffer_ptr, 0);

  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_seek_from_start
 * Signature: (JIJ)J
 * Seeks to the given position in the given file.
 * Inputs:
 *  jint fh: The filehandle to seek in.
 *  jlong pos: The position to seek to.
 * Returns: the new position (as a jlong) of the filehandle on success,
 *  or a negative error code on failure.
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1seek_1from_1start
  (JNIEnv *env, jobject obj, jint fh, jlong pos)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  CephContext *cct = ceph_get_mount_context(cmount);
  ldout(cct, 10) << "In CephTalker::seek_from_start" << dendl;
  return ceph_lseek(cmount, fh, pos, SEEK_SET);
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_getpos
 * Signature: (I)J
 *
 * Get the current position in a file (as a jlong) of a given filehandle.
 * Returns: jlong current file position on success, or a
 *  negative error code on failure.
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1getpos
  (JNIEnv *env, jobject obj, jint fh)
{
  // seek a distance of 0 to get current offset
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  CephContext *cct = ceph_get_mount_context(cmount);
  ldout(cct, 10) << "In CephTalker::ceph_getpos" << dendl;
  return ceph_lseek(cmount, fh, 0, SEEK_CUR);
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_write
 * Signature: (I[BII)I
 * Write the given buffer contents to the given filehandle.
 * Inputs:
 *  jint fh: The filehandle to write to.
 *  jbyteArray j_buffer: The buffer to write from
 *  jint buffer_offset: The position in the buffer to write from
 *  jint length: The number of (sequential) bytes to write.
 * Returns: jint, on success the number of bytes written, on failure
 *  a negative error code.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1write
  (JNIEnv *env, jobject obj, jint fh, jbyteArray j_buffer, jint buffer_offset, jint length)
{
  struct ceph_mount_info *cmount = get_ceph_mount_t(env, obj);
  CephContext *cct = ceph_get_mount_context(cmount);
  ldout(cct, 10) << "In write" << dendl;

  // IMPORTANT NOTE: Hadoop write arguments are a bit different from POSIX so we
  // have to convert.  The write is *always* from the current position in the file,
  // and buffer_offset is the location in the *buffer* where we start writing.
  jint result;

  // Step 1: get a pointer to the buffer.
  jbyte *j_buffer_ptr = env->GetByteArrayElements(j_buffer, NULL);
  if (j_buffer_ptr == NULL)
    return -ENOMEM;
  char *c_buffer = (char*) j_buffer_ptr;

  // Step 2: pointer arithmetic to start in the right buffer position
  c_buffer += (int)buffer_offset;

  // Step 3: do the write
  result = ceph_write(cmount, (int)fh, c_buffer, length, -1);

  // Step 4: release the pointer to the buffer
  env->ReleaseByteArrayElements(j_buffer, j_buffer_ptr, 0);

  return result;
}
