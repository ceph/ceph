#include "CephFSInterface.h"

using namespace std;

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_initializeClient
 * Signature: ()J
 * Initializes a ceph client.
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1initializeClient
  (JNIEnv *, jobject)
{

  cout << "Initializing Ceph client:" << endl;
  //cout.flush();
  // parse args from CEPH_ARGS 
  vector<char*> args; 
  env_to_vec(args);
  parse_config_options(args);

  // crap for getting args from the command line
  //vector<char*> args;
  //argv_to_vec(argc, argv, args);
  //parse_config_options(args);

  // args for fuse
  // vec_to_argv(args, argc, argv);

  // FUSE will chdir("/"); be ready.
  g_conf.use_abspaths = true;

  // load monmap
  MonMap monmap;
  int r = monmap.read(".ceph_monmap");
  if (r < 0) {
    cout << "could not find .ceph_monmap" << endl; 
    return 0;
  }
  //assert(r >= 0);

  // start up network
  rank.start_rank();

  // start client
  Client *client;
  client = new Client(rank.register_entity(MSG_ADDR_CLIENT_NEW), &monmap);
  client->init();
    
  // start up fuse
  // use my argc, argv (make sure you pass a mount point!)
  cout << "mounting client to filesystem..." << endl;
  client->mount();
  
  //cerr << "starting fuse on pid " << getpid() << endl;
  //ceph_fuse_main(client, argc, argv);
  //cerr << "fuse finished on pid " << getpid() << endl;
  
  //client->unmount();
  //cout << "unmounted" << endl;
  //client->shutdown();
  
  //delete client;
  
  // wait for messenger to finish
  //rank.wait();
  
  jlong clientp = *(jlong*)&client;
  return clientp;
  //return (jlong)client;

}


/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_copyFromLocalFile
 * Signature: (JLjava/lang/String;Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1copyFromLocalFile
(JNIEnv * env, jobject obj, jlong clientp, jstring j_local_path, jstring j_ceph_path) {

  cout << "In copyFromLocalFile" << endl;
  cout.flush();
  Client* client;
  //client = (Client*) clientp;
   client = *(Client**)&clientp;

  const char* c_local_path = env->GetStringUTFChars(j_local_path, 0);
  const char* c_ceph_path = env->GetStringUTFChars(j_ceph_path, 0);

  cout << "Local source file is "<< c_local_path << " and Ceph destination file is " << c_ceph_path << endl;
  struct stat st;
  int r = ::stat(c_local_path, &st);
  assert (r == 0);

  // open the files
  int fh_local = ::open(c_local_path, O_RDONLY);
  int fh_ceph = client->open(c_ceph_path, O_WRONLY|O_CREAT|O_TRUNC);  
  assert (fh_local > -1);
  assert (fh_ceph > -1);
  cout << "local fd is " << fh_local << " and Ceph fd is " << fh_ceph << endl;

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
  cout << "In copyToLocalFile" << endl;
  cout.flush();


  Client* client;
  client = *(Client**)&clientp;
  const char* c_ceph_path = env->GetStringUTFChars(j_ceph_path, 0);
  const char* c_local_path = env->GetStringUTFChars(j_local_path, 0);

  // get source file size
  struct stat st;
  int r = client->lstat(c_ceph_path, &st);
  assert (r == 0);

  int fh_ceph = client->open(c_ceph_path, O_WRONLY|O_CREAT|O_TRUNC);  
  int fh_local = ::open(c_local_path, O_RDONLY);
  assert (fh_ceph > -1);
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
  cout << "In getcwd" << endl;
  cout.flush();

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
  cout << "In setcwd" << endl;
  cout.flush();

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
  cout << "In rmdir" << endl;
  cout.flush();

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
  //cout << "In mkdir" << endl;
  //cout.flush();


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
  cout << "In unlink" << endl;
  cout.flush();

  Client* client;
  client = *(Client**)&clientp;

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  return (0 == client->unlink(c_path)) ? JNI_TRUE : JNI_FALSE; 
  env->ReleaseStringUTFChars(j_path, c_path);
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
  cout << "In rename" << endl;
  cout.flush();


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

  //cout << "In exists" << endl;
  //cout.flush();

  Client* client;
  struct stat stbuf;
  client = *(Client**)&clientp;

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  cout << "Attempting lstat with file " << c_path << ":" ;
  //int i = (int) (*c_path);
  //cout << "First character value is " << i;
  // cout.flush();
  int result = client->lstat(c_path, &stbuf);
  cout << "result is " << result << endl;
  //  cout << "Attempting to release string \"" << c_path << "\"" << endl;
  //cout.flush();
  env->ReleaseStringUTFChars(j_path, c_path);
  //cout << "String released!" << endl;
  if (result < 0) {
    //cout << "Returning false (file does not exist)" << endl;
    //cout.flush();
    return JNI_FALSE;
  }
  else {
    //cout << "Returning true (file exists)" << endl;
    //cout.flush();
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
  cout << "In getblocksize" << endl;
  cout.flush();


  Client* client;
  struct stat stbuf;
  client = *(Client**)&clientp;
  
  jint result;

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  if (0 > client->lstat(c_path, &stbuf))
    result =  -1;
  else
    result = stbuf.st_blksize;

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
  cout << "In getfilesize" << endl;
  cout.flush();

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
  //cout << "In isfile" << endl;
  //cout.flush();

  Client* client;
  struct stat stbuf;
  client = *(Client**)&clientp;


  const char* c_path = env->GetStringUTFChars(j_path, 0);
  //cout << "Attempting lstat with file " << c_path << ":" << endl;
  //cout.flush();
  int result = client->lstat(c_path, &stbuf);
  //cout << "Got through lstat without crashing: result is " << result << endl;
  //cout.flush();

  env->ReleaseStringUTFChars(j_path, c_path);

  // if the stat call failed, it's definitely not a file...
  if (0 > result) return JNI_FALSE; 

  // check the stat result
  //cout << "Stat call succeeded: attempting to look inside stbuf for result" << endl;
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
  //cout << "In isdirectory" << endl;
  //cout.flush();

  Client* client;
  struct stat stbuf;
  client = *(Client**)&clientp;

  const char* c_path = env->GetStringUTFChars(j_path, 0);
  int result = client->lstat(c_path, &stbuf);
  env->ReleaseStringUTFChars(j_path, c_path);
  //cout << "String released!" << endl;
  //cout.flush();

  // if the stat call failed, it's definitely not a directory...
  if (0 > result) return JNI_FALSE; 

  // check the stat result
  return (0 == S_ISDIR(stbuf.st_mode)) ? JNI_FALSE : JNI_TRUE;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephFileSystem
 * Method:    ceph_getdir
 * Signature: (JLjava/lang/String;)[Ljava/lang/String;
 */
JNIEXPORT jobjectArray JNICALL Java_org_apache_hadoop_fs_ceph_CephFileSystem_ceph_1getdir
(JNIEnv *env, jobject obj, jlong clientp, jstring j_path) {

  //cout << "In getdir" << endl;
  //cout.flush();


  Client* client;
  client = *(Client**)&clientp;

  // get the directory listing
  map<string, inode_t> contents;
  const char* c_path = env->GetStringUTFChars(j_path, 0);
  int result = client->getdir(c_path, contents);
  //cout << "Releasing string" << endl;
  env->ReleaseStringUTFChars(j_path, c_path);
  
  if (result < 0) return NULL;

  //cout << "checking for empty dir" << endl;
  jint dir_size = contents.size();
  if (dir_size < 1) 
    {
      //    cout << "dir was empty" << endl;
      //return NULL;
    }
  //out << "dir was not empty" << endl;

  // Create a Java String array of the size of the directory listing
  // jstring blankString = env->NewStringUTF("");
  jclass stringClass = env->FindClass("java/lang/String");
  if (NULL == stringClass) {
    cout << "ERROR: java String class not found; dying a horrible, painful death" << endl;
    assert(0);
  }
  jobjectArray dirListingStringArray = (jobjectArray) env->NewObjectArray(dir_size, stringClass, NULL);
  
  // populate the array with the elements of the directory list
  int i = 0;
  for (map<string, inode_t>::iterator it = contents.begin();
       it != contents.end();
       it++) {
    if (0 == dir_size)
      cout << "WARNING: adding stuff to an empty array" << endl;
    env->SetObjectArrayElement(dirListingStringArray, i, 
			       env->NewStringUTF(it->first.c_str()));
    ++i;
  }
			     
  return dirListingStringArray;
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
  //cout << "In open_for_read" << endl;
  //cout.flush();


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
  (JNIEnv *env, jobject obj, jlong clientp, jstring j_path)
{
  //cout << "In open_for_overwrite" << endl;
  //cout.flush();

  Client* client;
  client = *(Client**)&clientp;

  jint result; 


  const char* c_path = env->GetStringUTFChars(j_path, 0);
  result = client->open(c_path, O_WRONLY|O_CREAT|O_TRUNC);
  env->ReleaseStringUTFChars(j_path, c_path);

  // returns file handle, or -1 on failure
  return result;       
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
  //cout << "In read" << endl;
  //cout.flush();


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
  //cout << "In CephInputStream::seek_from_start" << endl;
  //cout.flush();


  Client* client;
  client = *(Client**)&clientp;
  jint result; 

  result = client->lseek(fh, pos, SEEK_SET);
  
  return result;
}


JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephInputStream_ceph_1getpos
  (JNIEnv *env, jobject obj, jlong clientp, jint fh)
{
  cout << "In CephInputStream::ceph_getpos" << endl;
  cout.flush();


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
  cout << "In CephInputStream::ceph_close" << endl;
  cout.flush();

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
  cout << "In CephOutputStream::ceph_seek_from_start" << endl;
  cout.flush();

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
  cout << "In CephOutputStream::ceph_getpos" << endl;
  cout.flush();

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
  cout << "In CephOutputStream::ceph_close" << endl;
  cout.flush();

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
  cout << "In write" << endl;
  cout.flush();


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

