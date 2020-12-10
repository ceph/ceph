/*
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/un.h>
#include <jni.h>

#include "include/rados/rgw_file.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_javaclient

#include "com_ceph_rgw_CephRgwAdapter.h"

#define CEPH_RGW_ADAPTER_CLASS		"com/ceph/rgw/CephRgwAdapter"
#define CEPH_LISTDIR_HANDLER_CLASS	"com/ceph/rgw/CephRgwAdapter$ListDirHandler"
#define CEPH_STAT_CLASS			"com/ceph/rgw/CephStat"
#define CEPH_STAT_VFS_CLASS		"com/ceph/rgw/CephStatVFS"
#define CEPH_FILEEXISTS_CLASS		"com/ceph/rgw/CephFileAlreadyExistsException"


/*
 * Flags to open(). must be synchronized with CephRgwAdapter.java
 *
 * There are two versions of flags: the version in Java and the version in the
 * target library (e.g. libc or libcephfs). We control the Java values and map
 * to the target value with fixup_* functions below. This is much faster than
 * keeping the values in Java and making a cross-JNI up-call to retrieve them,
 * and makes it easy to keep any platform specific value changes in this file.
 */
#define JAVA_O_RDONLY		1
#define JAVA_O_RDWR		2
#define JAVA_O_APPEND		4
#define JAVA_O_CREAT		8
#define JAVA_O_TRUNC		16
#define JAVA_O_EXCL		32
#define JAVA_O_WRONLY		64
#define JAVA_O_DIRECTORY	128


/*
 * Whence flags for seek(). sync with CephRgwAdapter.java if changed.
 *
 * Mapping of SEEK_* done in seek function.
 */
#define JAVA_SEEK_SET	1
#define JAVA_SEEK_CUR	2
#define JAVA_SEEK_END	3


/*
 * File attribute flags. sync with CephRgwAdapter.java if changed.
 */
#define JAVA_SETATTR_MODE	1
#define JAVA_SETATTR_UID	2
#define JAVA_SETATTR_GID	4
#define JAVA_SETATTR_MTIME	8
#define JAVA_SETATTR_ATIME	16


/*
 * Setxattr flags. sync with CephRgwAdapter.java if changed.
 */
#define JAVA_XATTR_CREATE	1
#define JAVA_XATTR_REPLACE	2
#define JAVA_XATTR_NONE		3


/*
 * flock flags. sync with CephRgwAdapter.java if changed.
 */
#define JAVA_LOCK_SH	1
#define JAVA_LOCK_EX	2
#define JAVA_LOCK_NB	4
#define JAVA_LOCK_UN	8

/* Map JAVA_O_* open flags to values in libc */
static inline int fixup_open_flags( jint jflags )
{
	int ret = 0;

#define FIXUP_OPEN_FLAG( name )	\
	if ( jflags & JAVA_ ## name ) \
		ret |= name;

	FIXUP_OPEN_FLAG( O_RDONLY )
	FIXUP_OPEN_FLAG( O_RDWR )
	FIXUP_OPEN_FLAG( O_APPEND )
	FIXUP_OPEN_FLAG( O_CREAT )
	FIXUP_OPEN_FLAG( O_TRUNC )
	FIXUP_OPEN_FLAG( O_EXCL )
	FIXUP_OPEN_FLAG( O_WRONLY )
	FIXUP_OPEN_FLAG( O_DIRECTORY )

#undef FIXUP_OPEN_FLAG

	return(ret);
}


/* Map JAVA_SETATTR_* to values in ceph lib */
static inline int fixup_attr_mask( jint jmask )
{
	int mask = 0;

#define FIXUP_ATTR_MASK( name )	\
	if ( jmask & JAVA_ ## name ) \
		mask |= CEPH_ ## name;

	FIXUP_ATTR_MASK( SETATTR_MODE )
	FIXUP_ATTR_MASK( SETATTR_UID )
	FIXUP_ATTR_MASK( SETATTR_GID )
	FIXUP_ATTR_MASK( SETATTR_MTIME )
	FIXUP_ATTR_MASK( SETATTR_ATIME )

#undef FIXUP_ATTR_MASK

	return(mask);
}


/* Cached field IDs for com.ceph.fs.CephStat */
static jfieldID cephstat_mode_fid;
static jfieldID cephstat_uid_fid;
static jfieldID cephstat_gid_fid;
static jfieldID cephstat_size_fid;
static jfieldID cephstat_blksize_fid;
static jfieldID cephstat_blocks_fid;
static jfieldID cephstat_a_time_fid;
static jfieldID cephstat_m_time_fid;

/* Cached field IDs for com.ceph.fs.CephStatVFS */
static jfieldID cephstatvfs_bsize_fid;
static jfieldID cephstatvfs_frsize_fid;
static jfieldID cephstatvfs_blocks_fid;
static jfieldID cephstatvfs_bavail_fid;
static jfieldID cephstatvfs_files_fid;
static jfieldID cephstatvfs_fsid_fid;
static jfieldID cephstatvfs_namemax_fid;

static jmethodID	listdir_handler_mid;
static jfieldID		bucket_fh_fid;
static jfieldID		root_fh_fid;

static CephContext *cct = nullptr;


/*
 * Exception throwing helper. Adapted from Apache Hadoop header
 * org_apache_hadoop.h by adding the do {} while (0) construct.
 */
#define THROW( env, exception_name, message ) \
	do { \
		jclass ecls = env->FindClass( exception_name );	\
		if ( ecls ) { \
			int ret = env->ThrowNew( ecls, message ); \
			if ( ret < 0 ) { \
				printf( "(CephFS) Fatal Error\n" ); \
			} \
			env->DeleteLocalRef( ecls ); \
		} \
	} while ( 0 )


static void cephThrowNullArg( JNIEnv *env, const char *msg )
{
	THROW( env, "java/lang/NullPointerException", msg );
}


static void cephThrowInternal( JNIEnv *env, const char *msg )
{
	THROW( env, "java/lang/InternalError", msg );
}


static void cephThrowIndexBounds( JNIEnv *env, const char *msg )
{
	THROW( env, "java/lang/IndexOutOfBoundsException", msg );
}


static void cephThrowFNF( JNIEnv *env, const char *msg )
{
	THROW( env, "java/io/FileNotFoundException", msg );
}


static void cephThrowFileExists( JNIEnv *env, const char *msg )
{
	THROW( env, CEPH_FILEEXISTS_CLASS, msg );
}


static void handle_error( JNIEnv* env, int rc )
{
	switch ( rc )
	{
	case -ENOENT:
		cephThrowFNF( env, "" );
		return;
	case -EEXIST:
		cephThrowFileExists( env, "" );
		return;
	default:
		break;
	}

	THROW( env, "java/io/IOException", strerror( -rc ) );
}


#define CHECK_ARG_NULL( v, m, r )     \
	do {	    \
		if ( !(v) ) {	    \
			cephThrowNullArg( env, (m) ); \
			return (r);		   \
		}	\
	} while ( 0 )

#define CHECK_ARG_BOUNDS( c, m, r )  \
	do {	    \
		if ( (c) ) {	 \
			cephThrowIndexBounds( env, (m) ); \
			return (r);    \
		}     \
	} while ( 0 )

static void setup_field_ids( JNIEnv *env, jclass clz )
{
	jclass	cephstat_cls;
	jclass	cephstatvfs_cls;
	jclass	listdir_handler_cls;


/*
 * Get a fieldID from a class with a specific type
 *
 * clz: jclass
 * field: field in clz
 * type: integer, long, etc..
 *
 * This macro assumes some naming convention that is used
 * only in this file:
 *
 *   GETFID(cephstat, mode, I) gets translated into
 *     cephstat_mode_fid = env->GetFieldID(cephstat_cls, "mode", "I");
 */
#define GETFID( clz, field, type )	 \
	do {	       \
		clz ## _ ## field ## _fid = env->GetFieldID( clz ## _cls, # field, # type ); \
		if ( !clz ## _ ## field ## _fid )	\
			return;		\
	} while ( 0 )

	/* Cache CephStat fields */

	cephstat_cls = env->FindClass( CEPH_STAT_CLASS );
	if ( !cephstat_cls )
		return;

	GETFID( cephstat, mode, I );
	GETFID( cephstat, uid, I );
	GETFID( cephstat, gid, I );
	GETFID( cephstat, size, J );
	GETFID( cephstat, blksize, J );
	GETFID( cephstat, blocks, J );
	GETFID( cephstat, a_time, J );
	GETFID( cephstat, m_time, J );

	/* Cache CephStatVFS fields */

	cephstatvfs_cls = env->FindClass( CEPH_STAT_VFS_CLASS );
	if ( !cephstatvfs_cls )
		return;

	GETFID( cephstatvfs, bsize, J );
	GETFID( cephstatvfs, frsize, J );
	GETFID( cephstatvfs, blocks, J );
	GETFID( cephstatvfs, bavail, J );
	GETFID( cephstatvfs, files, J );
	GETFID( cephstatvfs, fsid, J );
	GETFID( cephstatvfs, namemax, J );

#undef GETFID

	listdir_handler_cls = env->FindClass( CEPH_LISTDIR_HANDLER_CLASS );
	if ( !listdir_handler_cls )
		return;
	listdir_handler_mid = env->GetMethodID( listdir_handler_cls, "listDirHandler", "(Ljava/lang/String;IIIJJJJJ)V" );
	if ( !listdir_handler_mid )
		return;

	root_fh_fid	= env->GetFieldID( clz, "rootFH", "J" );
	bucket_fh_fid	= env->GetFieldID( clz, "bucketFH", "J" );
}


JNIEXPORT void JNICALL native_initialize( JNIEnv *env, jclass clz )
{
	setup_field_ids( env, clz );
}


JNIEXPORT jint JNICALL native_ceph_create( JNIEnv *env, jclass clz, jstring j_arg )
{
	librgw_t	rgw_h	= nullptr;
	char		*c_arg	= nullptr;
	int		ret;

	CHECK_ARG_NULL( j_arg, "@j_arg is null", -1 );

	if ( j_arg )
	{
		c_arg = (char *) env->GetStringUTFChars( j_arg, nullptr );
	}

	ret = librgw_create( &rgw_h, 1, &c_arg );

	if ( c_arg )
		env->ReleaseStringUTFChars( j_arg, c_arg );

	if ( ret )
	{
		THROW( env, "java/lang/RuntimeException", "failed to create rgw" );
		return(ret);
	}

	cct = (CephContext *) rgw_h;

	return(ret);
}


JNIEXPORT jlong JNICALL native_ceph_mount( JNIEnv *env, jclass clz, jobject j_adapter,
    jstring j_uid, jstring j_access, jstring j_secret, jstring j_root )
{
	struct rgw_file_handle	*bucket_fh;
	const char		* c_uid		= env->GetStringUTFChars( j_uid, nullptr );
	const char		* c_access	= env->GetStringUTFChars( j_access, nullptr );
	const char		* c_secret	= env->GetStringUTFChars( j_secret, nullptr );
	const char		* c_root	= env->GetStringUTFChars( j_root, nullptr );
	struct rgw_fs		*rgw_fs;
	struct stat		st;
	int			ret;

	ldout( cct, 10 ) << "jni: ceph_mount: " << (c_root ? c_root : "<nullptr>") << dendl;
	ret = rgw_mount2( (librgw_t) cct, c_uid, c_access, c_secret, "/", &rgw_fs, RGW_MOUNT_FLAG_NONE );
	ldout( cct, 10 ) << "jni: ceph_mount: exit ret" << ret << dendl;
	env->ReleaseStringUTFChars( j_uid, c_uid );
	env->ReleaseStringUTFChars( j_access, c_access );
	env->ReleaseStringUTFChars( j_secret, c_secret );

	if ( ret )
	{
		handle_error( env, ret );
		return(ret);
	}
	ret = rgw_lookup( rgw_fs, rgw_fs->root_fh, c_root, &bucket_fh, nullptr, 0, RGW_LOOKUP_FLAG_NONE );
	if ( ret )
	{
		st.st_mode	= 0777;
		ret		= rgw_mkdir( rgw_fs, rgw_fs->root_fh, c_root, &st, RGW_SETATTR_MODE, &bucket_fh, RGW_MKDIR_FLAG_NONE );
	}
	if ( ret )
	{
		handle_error( env, ret );
		rgw_umount( rgw_fs, RGW_UMOUNT_FLAG_NONE );
		return(ret);
	}

	env->SetLongField( j_adapter, root_fh_fid, (long) rgw_fs->root_fh );
	env->SetLongField( j_adapter, bucket_fh_fid, (long) bucket_fh );

	return( (jlong) rgw_fs);
}


JNIEXPORT jint JNICALL native_ceph_unmount( JNIEnv *env, jclass clz, jlong j_rgw_fs )
{
	struct rgw_fs	*rgw_fs = (struct rgw_fs *) j_rgw_fs;
	int		ret;

	ldout( cct, 10 ) << "jni: ceph_unmount enter" << dendl;

	ret = rgw_umount( rgw_fs, RGW_UMOUNT_FLAG_NONE );

	ldout( cct, 10 ) << "jni: ceph_unmount exit ret " << ret << dendl;

	if ( ret )
		handle_error( env, ret );

	return(ret);
}


JNIEXPORT jint JNICALL native_ceph_release( JNIEnv *env, jclass clz, jlong j_rgw_fs )
{
	ldout( cct, 10 ) << "jni: ceph_release called" << dendl;

	librgw_shutdown( (librgw_t) cct );

	return(0);
}


JNIEXPORT jint JNICALL native_ceph_statfs( JNIEnv *env, jclass clz, jlong j_rgw_fs,
    jlong j_fh, jstring j_path, jobject j_cephstatvfs )
{
	struct rgw_fs		*rgw_fs = (struct rgw_fs *) j_rgw_fs;
	struct rgw_file_handle	*root_fh= (struct rgw_file_handle *) j_fh;
	struct rgw_file_handle	*rgw_fh;
	struct rgw_statvfs	vfs_st;
	const char		*c_path;
	int			ret;

	CHECK_ARG_NULL( j_path, "@path is null", -1 );
	CHECK_ARG_NULL( j_cephstatvfs, "@stat is null", -1 );

	c_path = env->GetStringUTFChars( j_path, nullptr );
	if ( !c_path )
	{
		cephThrowInternal( env, "Failed to pin memory" );
		return(-1);
	}

	ldout( cct, 10 ) << "jni:statfs: path " << c_path << dendl;
	ret = rgw_lookup( rgw_fs, root_fh, c_path, &rgw_fh, nullptr, 0, RGW_LOOKUP_FLAG_NONE );
	if ( ret < 0 )
	{
		handle_error( env, ret );
		return(ret);
	}

	ret = rgw_statfs( rgw_fs, rgw_fh, &vfs_st, RGW_STATFS_FLAG_NONE );

	ldout( cct, 10 ) << "jni: statfs: exit ret " << ret << dendl;

	env->ReleaseStringUTFChars( j_path, c_path );

	if ( ret < 0 )
	{
		handle_error( env, ret );
		return(ret);
	}

	env->SetLongField( j_cephstatvfs, cephstatvfs_bsize_fid, vfs_st.f_bsize );
	env->SetLongField( j_cephstatvfs, cephstatvfs_frsize_fid, vfs_st.f_frsize );
	env->SetLongField( j_cephstatvfs, cephstatvfs_blocks_fid, vfs_st.f_blocks );
	env->SetLongField( j_cephstatvfs, cephstatvfs_bavail_fid, vfs_st.f_bavail );
	env->SetLongField( j_cephstatvfs, cephstatvfs_files_fid, vfs_st.f_files );
	env->SetLongField( j_cephstatvfs, cephstatvfs_fsid_fid, vfs_st.f_fsid[0] );
	env->SetLongField( j_cephstatvfs, cephstatvfs_namemax_fid, vfs_st.f_namemax );

	return(ret);
}


struct rgw_cb_data {
	JNIEnv			*env;
	struct rgw_fs		*rgw_fs;
	struct rgw_file_handle	*rgw_fh;
	jobject			handler;
};

static bool readdir_cb( const char *name, void *arg, uint64_t offset,
			struct stat *st, uint32_t mask, uint32_t flags )
{
	struct rgw_cb_data	*rgw_cb_data	= (struct rgw_cb_data *) arg;
	JNIEnv			* env		= rgw_cb_data->env;
	struct rgw_fs		* rgw_fs	= rgw_cb_data->rgw_fs;
	struct rgw_file_handle	*dir_fh		= rgw_cb_data->rgw_fh;
	struct rgw_file_handle	*rgw_fh;
	int			ret;

	if ( strcmp( name, "." ) == 0 || strcmp( name, ".." ) == 0 )
		return(true);

	ret = rgw_lookup( rgw_fs, dir_fh, name, &rgw_fh, st, mask,
			  RGW_LOOKUP_FLAG_RCB | (flags & RGW_LOOKUP_TYPE_FLAGS) );
	if ( ret < 0 )
		return(false);

	ret = rgw_getattr( rgw_fs, rgw_fh, st, RGW_GETATTR_FLAG_NONE );
	if ( ret < 0 )
		return(false);

	jstring j_name = env->NewStringUTF( name );
	env->CallVoidMethod( rgw_cb_data->handler, listdir_handler_mid, j_name,
			     st->st_mode, st->st_uid, st->st_gid, st->st_size, st->st_blksize,
			     st->st_blocks, st->st_mtime * 1000, st->st_atime * 1000 );
	env->DeleteLocalRef( j_name );
	return(true);
}


JNIEXPORT jint JNICALL native_ceph_listdir( JNIEnv *env, jclass clz,
    jlong j_rgw_fs, jlong j_fh, jstring j_path, jobject handler )
{
	struct rgw_fs		*rgw_fs = (struct rgw_fs *) j_rgw_fs;
	struct rgw_file_handle	*rgw_fh;
	struct rgw_cb_data	rgw_cb_data;
	const char		*c_path;
	uint64_t		offset	= 0;
	bool			eof	= false;
	int			ret;

	CHECK_ARG_NULL( j_path, "@path is null", -EINVAL);
	c_path = env->GetStringUTFChars( j_path, nullptr );
	if ( !c_path )
	{
		cephThrowInternal( env, "failed to pin memory" );
		return(-ENOMEM);
	}

	ret = rgw_lookup( rgw_fs, (struct rgw_file_handle *)j_fh, c_path, &rgw_fh, nullptr, 0, RGW_LOOKUP_FLAG_NONE );
	env->ReleaseStringUTFChars( j_path, c_path );
	if ( ret < 0 )
	{
		handle_error( env, ret );
		return(ret);
	}

	rgw_cb_data.env		= env;
	rgw_cb_data.handler	= handler;
	rgw_cb_data.rgw_fs	= rgw_fs;
	rgw_cb_data.rgw_fh	= rgw_fh;
	while ( !eof )
	{
		ret = rgw_readdir( rgw_fs, rgw_fh, &offset, readdir_cb, &rgw_cb_data, &eof, RGW_READDIR_FLAG_NONE );
		if ( ret < 0 )
		{
			handle_error( env, ret );
			break;
		}
	}
	return(ret);
}


JNIEXPORT jint JNICALL native_ceph_unlink( JNIEnv *env, jclass clz, jlong j_rgw_fs, jlong j_fh, jstring j_path )
{
	struct rgw_fs	*rgw_fs = (struct rgw_fs *) j_rgw_fs;
	const char	*c_path;
	int		ret;

	CHECK_ARG_NULL( j_path, "@path is null", -1 );

	c_path = env->GetStringUTFChars( j_path, nullptr );
	if ( !c_path )
	{
		cephThrowInternal( env, "failed to pin memory" );
		return(-1);
	}

	ldout( cct, 10 ) << "jni: unlink: path " << c_path << dendl;
	ret = rgw_unlink( rgw_fs, (struct rgw_file_handle *)j_fh, c_path, RGW_UNLINK_FLAG_NONE );

	ldout( cct, 10 ) << "jni: unlink: exit ret " << ret << dendl;

	env->ReleaseStringUTFChars( j_path, c_path );

	if ( ret < 0 )
		handle_error( env, ret );

	return(ret);
}


JNIEXPORT jint JNICALL native_ceph_rename( JNIEnv *env, jclass clz,
    jlong j_rgw_fs, jlong j_src_fh, jstring j_src_path, jstring j_src_name,
    jlong j_dst_fh, jstring j_dst_path, jstring j_dst_name )
{
	struct rgw_fs		*rgw_fs		= (struct rgw_fs *) j_rgw_fs;
	struct rgw_file_handle	*srcdir_fh	= (struct rgw_file_handle *) j_src_fh;
	struct rgw_file_handle	*dstdir_fh	= (struct rgw_file_handle *) j_dst_fh;
	struct rgw_file_handle	*src_fh;
	struct rgw_file_handle	*dst_fh;
	const char		*c_src_path;
	const char		*c_src_name;
	const char		*c_dst_path;
	const char		*c_dst_name;
	int			ret;

	CHECK_ARG_NULL( j_src_path, "@src_path is null", -1 );
	CHECK_ARG_NULL( j_src_name, "@src_name is null", -1 );
	CHECK_ARG_NULL( j_dst_path, "@dst_path is null", -1 );
	CHECK_ARG_NULL( j_dst_name, "@dst_name is null", -1 );

	c_src_path	= env->GetStringUTFChars( j_src_path, nullptr );
	c_src_name	= env->GetStringUTFChars( j_src_name, nullptr );
	c_dst_path	= env->GetStringUTFChars( j_dst_path, nullptr );
	c_dst_name	= env->GetStringUTFChars( j_dst_name, nullptr );

	ret = rgw_lookup( rgw_fs, srcdir_fh, c_src_path, &src_fh, nullptr, 0,
			  RGW_LOOKUP_FLAG_NONE );
	if ( ret < 0 )
	{
		goto out;
	}

	ret = rgw_lookup( rgw_fs, dstdir_fh, c_dst_path, &dst_fh, nullptr, 0,
			  RGW_LOOKUP_FLAG_NONE );
	if ( ret < 0 )
	{
		goto out;
	}

	ldout( cct, 10 )	<< "jni:rename: from " << c_src_path << c_src_name
				<< " to " << c_dst_path << c_dst_name << dendl;
	ret = rgw_rename( rgw_fs, src_fh, c_src_name, dst_fh, c_dst_name, RGW_RENAME_FLAG_NONE );
	ldout( cct, 10 ) << "jni: rename: exit ret " << ret << dendl;

out:
	env->ReleaseStringUTFChars( j_src_path, c_src_path );
	env->ReleaseStringUTFChars( j_src_name, c_src_name );
	env->ReleaseStringUTFChars( j_dst_path, c_dst_path );
	env->ReleaseStringUTFChars( j_dst_name, c_dst_name );

	if ( ret < 0 )
		handle_error( env, ret );

	return(ret);
}


JNIEXPORT jboolean JNICALL native_ceph_mkdirs( JNIEnv *env, jclass clz,
    jlong j_rgw_fs, jlong j_fh, jstring j_path, jstring j_name, jint j_mode )
{
	struct rgw_fs		*rgw_fs = (struct rgw_fs *) j_rgw_fs;
	struct rgw_file_handle	*root_fh = (struct rgw_file_handle *) j_fh;
	struct rgw_file_handle	*rgw_fh;
	struct rgw_file_handle	*fh;
	const char		*c_path;
	const char		*c_name;
	struct stat		st;
	int			ret;

	CHECK_ARG_NULL( j_path, "@path is null", -1 );

	c_path	= env->GetStringUTFChars( j_path, nullptr );
	c_name	= env->GetStringUTFChars( j_name, nullptr );
	if ( !c_path )
	{
		cephThrowInternal( env, "failed to pin memory" );
		return(false);
	}

	ret = rgw_lookup( rgw_fs, root_fh, c_path, &rgw_fh, nullptr, 0, RGW_LOOKUP_FLAG_CREATE | RGW_LOOKUP_FLAG_DIR );
	if ( ret < 0 )
	{
		handle_error( env, ret );
		return(false);
	}

	ldout( cct, 10 ) << "jni: mkdirs: path " << c_path << " mode " << (int) j_mode << dendl;
	st.st_mode	= (int) j_mode;
	ret		= rgw_mkdir( rgw_fs, rgw_fh, c_name, &st, RGW_SETATTR_MODE, &fh, RGW_MKDIR_FLAG_NONE );

	ldout( cct, 10 ) << "jni: mkdirs: exit ret " << ret << dendl;

	env->ReleaseStringUTFChars( j_path, c_path );
	env->ReleaseStringUTFChars( j_name, c_name );

	if ( ret )
		handle_error( env, ret );

	return(!!ret);
}


static void fill_cephstat( JNIEnv *env, jobject j_cephstat, struct stat *st )
{
	env->SetIntField( j_cephstat, cephstat_mode_fid, st->st_mode );
	env->SetIntField( j_cephstat, cephstat_uid_fid, st->st_uid );
	env->SetIntField( j_cephstat, cephstat_gid_fid, st->st_gid );
	env->SetLongField( j_cephstat, cephstat_size_fid, st->st_size );
	env->SetLongField( j_cephstat, cephstat_blksize_fid, st->st_blksize );
	env->SetLongField( j_cephstat, cephstat_blocks_fid, st->st_blocks );

	env->SetLongField( j_cephstat, cephstat_m_time_fid, st->st_mtime * 1000 );
	env->SetLongField( j_cephstat, cephstat_a_time_fid, st->st_atime * 1000 );
}


JNIEXPORT jint JNICALL native_ceph_lstat( JNIEnv *env, jclass clz,
    jlong j_rgw_fs, jlong j_fh, jstring j_path, jobject j_cephstat )
{
	struct rgw_fs		*rgw_fs		= (struct rgw_fs *) j_rgw_fs;
	struct rgw_file_handle	*root_fh	= (struct rgw_file_handle *) j_fh;
	struct rgw_file_handle	*rgw_fh;
	struct stat		st;
	const char		*c_path;
	int			ret;

	CHECK_ARG_NULL( j_path, "@path is null", -1 );
	CHECK_ARG_NULL( j_cephstat, "@stat is null", -1 );

	c_path = (char *) env->GetStringUTFChars( j_path, nullptr );

	ldout( cct, 10 ) << "jni: lstat: path " << c_path << " len " << strlen( c_path ) << dendl;
	ret = rgw_lookup( rgw_fs, root_fh, c_path, &rgw_fh, nullptr, 0, RGW_LOOKUP_FLAG_NONE );
	env->ReleaseStringUTFChars( j_path, c_path );
	if ( ret < 0 )
	{
		goto out;
	}
	ret = rgw_getattr( rgw_fs, rgw_fh, &st, RGW_GETATTR_FLAG_NONE );
	if ( ret == 0 )
		fill_cephstat( env, j_cephstat, &st );

out:
	if ( ret < 0 )
	{
		handle_error( env, ret );
	}

	ldout( cct, 10 ) << "jni: lstat exit ret " << ret << dendl;
	return(ret);
}


JNIEXPORT jint JNICALL native_ceph_setattr( JNIEnv *env, jclass clz,
    jlong j_rgw_fs, jlong j_fh, jstring j_path, jobject j_cephstat, jint j_mask )
{
	struct rgw_fs		*rgw_fs		= (struct rgw_fs *) j_rgw_fs;
	struct rgw_file_handle	*root_fh	= (struct rgw_file_handle *) j_fh;
	struct rgw_file_handle	*rgw_fh;
	struct stat		st;
	const char		*c_path;
	long 			mtime_msec;
	long 			atime_msec;
	int			mask = fixup_attr_mask( j_mask );
	int 			ret;

	CHECK_ARG_NULL( j_path, "@path is null", -1 );
	CHECK_ARG_NULL( j_cephstat, "@stat is null", -1 );

	c_path = env->GetStringUTFChars( j_path, nullptr );
	if ( !c_path )
	{
		cephThrowInternal( env, "Failed to pin memory" );
		return(-1);
	}
	ldout( cct, 10 ) << "jni: statfs: path " << c_path << dendl;
	ret = rgw_lookup( rgw_fs, root_fh, c_path, &rgw_fh, nullptr, 0,
			  RGW_LOOKUP_FLAG_NONE );
	if ( ret < 0 )
	{
		handle_error( env, ret );
		return(ret);
	}

	memset( &st, 0, sizeof(st) );

	st.st_mode	= env->GetIntField( j_cephstat, cephstat_mode_fid );
	st.st_uid	= env->GetIntField( j_cephstat, cephstat_uid_fid );
	st.st_gid	= env->GetIntField( j_cephstat, cephstat_gid_fid );
	mtime_msec	= env->GetLongField( j_cephstat, cephstat_m_time_fid );
	atime_msec	= env->GetLongField( j_cephstat, cephstat_a_time_fid );
	st.st_mtime	= mtime_msec / 1000;
	st.st_atime	= atime_msec / 1000;

	ldout( cct, 10 ) << "jni: setattr: path " << c_path << " mask " << mask << dendl;

	ret = rgw_setattr( rgw_fs, rgw_fh, &st, mask, RGW_SETATTR_FLAG_NONE );
	ldout( cct, 10 ) << "jni: setattr: exit ret " << ret << dendl;

	env->ReleaseStringUTFChars( j_path, c_path );
	if ( ret < 0 )
		handle_error( env, ret );

	return(ret);
}


JNIEXPORT jlong JNICALL native_ceph_open( JNIEnv *env, jclass clz,
    jlong j_rgw_fs, jlong j_fh, jstring j_path, jint j_flags, jint j_mode )
{
	struct rgw_fs		*rgw_fs		= (struct rgw_fs *) j_rgw_fs;
	struct rgw_file_handle	*root_fh	= (struct rgw_file_handle *) j_fh;
	struct rgw_file_handle	*rgw_fh;
	const char		*c_path;
	uint32_t		lookup_flags	= RGW_LOOKUP_FLAG_FILE;
	int			flags		= fixup_open_flags( j_flags );
	int			ret;

	CHECK_ARG_NULL( j_path, "@path is null", -1 );

	c_path = env->GetStringUTFChars( j_path, nullptr );
	if ( !c_path )
	{
		cephThrowInternal( env, "Failed to pin memory" );
		return(-1);
	}
	if ( j_flags & JAVA_O_CREAT )
		lookup_flags |= RGW_LOOKUP_FLAG_CREATE;
	ldout( cct, 10 )	<< "jni: open: path " << c_path << " flags " << flags
				<< " lookup_flags " << lookup_flags << dendl;

	ret = rgw_lookup( rgw_fs, root_fh, c_path, &rgw_fh, nullptr, 0, lookup_flags );
	if ( ret < 0 )
	{
		handle_error( env, ret );
		return(ret);
	}

	ret = rgw_open( rgw_fs, rgw_fh, flags, RGW_OPEN_FLAG_NONE );
	ldout( cct, 10 ) << "jni: open: exit ret " << ret << dendl;

	env->ReleaseStringUTFChars( j_path, c_path );
	if ( ret < 0 )
		handle_error( env, ret );

	return( (jlong) rgw_fh);
}


JNIEXPORT jint JNICALL native_ceph_close( JNIEnv *env, jclass clz, jlong j_rgw_fs, jlong j_fd )
{
	struct rgw_fs		*rgw_fs = (struct rgw_fs *) j_rgw_fs;
	int			ret;

	ldout( cct, 10 ) << "jni: close: fd " << (long) j_fd << dendl;
	ret = rgw_close( rgw_fs, (struct rgw_file_handle *)j_fd, RGW_CLOSE_FLAG_RELE );
	ldout( cct, 10 ) << "jni: close: ret " << ret << dendl;

	if ( ret )
		handle_error( env, ret );

	return(ret);
}


JNIEXPORT jlong JNICALL native_ceph_read( JNIEnv *env, jclass clz,
    jlong j_rgw_fs, jlong j_fd, jlong j_offset, jbyteArray j_buf, jlong j_size )
{
	struct rgw_fs		*rgw_fs = (struct rgw_fs *) j_rgw_fs;
	struct rgw_file_handle	*fh	= (struct rgw_file_handle *) j_fd;
	jboolean		iscopy	= JNI_FALSE;
	size_t			bytes_read;
	jsize			buf_size;
	jbyte			*c_buf;
	long			ret;

	CHECK_ARG_NULL( j_buf, "@buf is null", -1 );
	CHECK_ARG_BOUNDS( j_size < 0, "@size is negative", -1 );

	buf_size = env->GetArrayLength( j_buf );
	CHECK_ARG_BOUNDS( j_size > buf_size, "@size > @buf.length", -1 );

	c_buf = env->GetByteArrayElements( j_buf, &iscopy );
	if ( !c_buf )
	{
		cephThrowInternal( env, "failed to pin memory" );
		return(-1);
	}

	ldout( cct, 10 ) << "jni: read: fd " << (int) j_fd << " len " << (long) j_size <<
	" offset " << (long) j_offset << dendl;

	ret = rgw_read( rgw_fs, fh, (long) j_offset, (long) j_size, &bytes_read, c_buf, RGW_READ_FLAG_NONE );
	ldout( cct, 10 ) << "jni: read: exit ret " << ret << "bytes_read " << bytes_read << dendl;
	if ( ret < 0 )
	{
		handle_error( env, (int) ret );
		bytes_read = 0;
	}

	env->ReleaseByteArrayElements( j_buf, c_buf, JNI_COMMIT );
	if ( iscopy )
	{
		free( c_buf );
	}

	return( (jlong) bytes_read);
}


JNIEXPORT jlong JNICALL native_ceph_write( JNIEnv *env, jclass clz,
    jlong j_rgw_fs, jlong j_fd, jlong j_offset, jbyteArray j_buf, jlong j_size )
{
	struct rgw_fs		*rgw_fs = (struct rgw_fs *) j_rgw_fs;
	struct rgw_file_handle	*fh	= (struct rgw_file_handle *) j_fd;
	size_t			bytes_written;
	jsize			buf_size;
	jbyte			*c_buf;
	long			ret;

	CHECK_ARG_NULL( j_buf, "@buf is null", -1 );
	CHECK_ARG_BOUNDS( j_size < 0, "@size is negative", -1 );

	buf_size = env->GetArrayLength( j_buf );
	CHECK_ARG_BOUNDS( j_size > buf_size, "@size > @buf.length", -1 );

	c_buf = env->GetByteArrayElements( j_buf, 0 );
	if ( !c_buf )
	{
		cephThrowInternal( env, "failed to pin memory" );
		return(-1);
	}

	ldout( cct, 10 ) << "jni: write: fd " << (int) j_fd << " len " << (long) j_size <<
	" offset " << (long) j_offset << dendl;

	ret = rgw_write( rgw_fs, fh, (long) j_offset, (long) j_size, &bytes_written, c_buf, RGW_WRITE_FLAG_NONE );

	ldout( cct, 10 ) << "jni: write: exit ret " << ret << dendl;

	if ( ret < 0 )
		handle_error( env, (int) ret );

	env->ReleaseByteArrayElements( j_buf, c_buf, JNI_ABORT );

	return( (jlong) bytes_written);
}


JNIEXPORT jint JNICALL native_ceph_fsync( JNIEnv *env, jclass clz,
    jlong j_rgw_fs, jlong j_fd, jboolean j_dataonly )
{
	struct rgw_fs		*rgw_fs = (struct rgw_fs *) j_rgw_fs;
	int			ret;

	ldout( cct, 10 ) << "jni:fsync: fd " << (int) j_fd <<
	" dataonly " << (j_dataonly ? 1 : 0) << dendl;

	ret = rgw_fsync( rgw_fs, (struct rgw_file_handle *)j_fd, RGW_WRITE_FLAG_NONE );

	ldout( cct, 10 ) << "jni: fsync: exit ret " << ret << dendl;

	if ( ret )
		handle_error( env, ret );

	return(ret);
}


static const JNINativeMethod gMethods[] = {
	{ "native_initialize",	 "()V",													     (void *) native_initialize	  },
	{ "native_ceph_create",	 "(Ljava/lang/String;)I",										     (void *) native_ceph_create  },
	{ "native_ceph_mount",	 "(Lcom/ceph/rgw/CephRgwAdapter;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)J", (void *) native_ceph_mount	  },
	{ "native_ceph_unmount", "(J)I",												     (void *) native_ceph_unmount },
	{ "native_ceph_release", "(J)I",												     (void *) native_ceph_release },
	{ "native_ceph_statfs",	 "(JJLjava/lang/String;Lcom/ceph/rgw/CephStatVFS;)I",							     (void *) native_ceph_statfs  },
	{ "native_ceph_listdir", "(JJLjava/lang/String;Lcom/ceph/rgw/CephRgwAdapter$ListDirHandler;)I",					     (void *) native_ceph_listdir },
	{ "native_ceph_unlink",	 "(JJLjava/lang/String;)I",										     (void *) native_ceph_unlink  },
	{ "native_ceph_rename",	 "(JJLjava/lang/String;Ljava/lang/String;JLjava/lang/String;Ljava/lang/String;)I",			     (void *) native_ceph_rename  },
	{ "native_ceph_mkdirs",	 "(JJLjava/lang/String;Ljava/lang/String;I)Z",								     (void *) native_ceph_mkdirs  },
	{ "native_ceph_lstat",	 "(JJLjava/lang/String;Lcom/ceph/rgw/CephStat;)I",							     (void *) native_ceph_lstat	  },
	{ "native_ceph_setattr", "(JJLjava/lang/String;Lcom/ceph/rgw/CephStat;I)I",							     (void *) native_ceph_setattr },
	{ "native_ceph_open",	 "(JJLjava/lang/String;II)J",										     (void *) native_ceph_open	  },
	{ "native_ceph_close",	 "(JJ)I",												     (void *) native_ceph_close	  },
	{ "native_ceph_read",	 "(JJJ[BJ)J",												     (void *) native_ceph_read	  },
	{ "native_ceph_write",	 "(JJJ[BJ)J",												     (void *) native_ceph_write	  },
	{ "native_ceph_fsync",	 "(JJZ)I",												     (void *) native_ceph_fsync	  },
};

JNIEXPORT jint JNICALL JNI_OnLoad( JavaVM* vm, void* reserved )
{
	JNIEnv	* env	= nullptr;

	if ( vm->GetEnv( (void * *) &env, JNI_VERSION_1_4 ) != JNI_OK )
	{
		return(JNI_FALSE);
	}
	assert( env != nullptr );

	jclass clazz = env->FindClass( CEPH_RGW_ADAPTER_CLASS );
	if ( clazz == nullptr )
		return(JNI_FALSE);
	if ( env->RegisterNatives( clazz, gMethods, sizeof(gMethods) / sizeof(gMethods[0]) ) < 0 )
		return(JNI_FALSE);

	return(JNI_VERSION_1_4);
}
