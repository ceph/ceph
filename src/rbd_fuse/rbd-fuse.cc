/*
 * rbd-fuse
 */
#define FUSE_USE_VERSION 30

#include "include/int_types.h"

#include <stddef.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>
#include <getopt.h>
#include <assert.h>
#include <string>
#include <mutex>
#include <limits.h>
#include <unordered_set>
#include <map>

#if defined(__FreeBSD__)
#include <sys/param.h>
#endif

#include "include/compat.h"
#include "include/rbd/librbd.h"

static int gotrados = 0;
std::string pool_name;              // Pool name (-p)
std::string mount_image_name;       // Image to mount (-r)
bool expose_snapshots = false;      // Flag indicating whether to expose image snapshots as files (-e)
bool map_partitions = false;        // Flag indicating whether to map partitions to files (-m)
uint64_t num_images = 0;            // Number of images to mount (-n)
uint64_t max_writes_in_flight = 0;  // Maximum number of writes in flight (-w)
rados_t cluster;
rados_ioctx_t ioctx;
int next_descriptor = 0;

// Mutexes currently need to be re-entrant (std::recursive_mutex instead of std::mutex)
std::recursive_mutex readdir_lock;           // Locks access to the FUSE directory
std::recursive_mutex in_flight_writes_lock;  // Locks access to the in-flight write recordset
std::recursive_mutex file_descriptor_lock;   // Serialize increments to the next file descriptor
std::recursive_mutex image_data_lock;        // Locks access to the image data map
std::recursive_mutex open_images_lock;       // Locks access to the open image map

struct rbd_stat {
    u_char valid;
    rbd_image_info_t rbd_info;
};

// Command line options
struct rbd_options {
    char *ceph_config;
    bool expose_snapshots;
    char *pool_name;
    char *image_name;
    bool map_partitions;
    uint64_t num_images;
    uint64_t max_writes_in_flight;
};

// Packed RBD list struct
struct rbd_image_list {
    void *buf;
    size_t buf_len;
};
struct rbd_image_list rbd_image_list;  // Packed RBD list, maintained for quickly determining if the list has changed

// Structure defining parameters around an image. Since an image can also be a partition, the image name,
// sector offset, and partition size information is included.
struct rbd_image_data {
    std::string image_name;
    std::string snapshot_name;
    struct rbd_stat rbd_stat;  // Image/file information
    uint64_t sector_size;
    uint64_t starting_sector;
    uint64_t num_sectors;
};
typedef std::map<std::string, rbd_image_data> rbd_image_data_set;
rbd_image_data_set rbd_image_data;     // Unpacked+filtered RBD list

// Data representing an open RBD
struct rbd_openimage {
    std::string name;  // File name (image name or partition name)
    rbd_image_t image; // Open image object
};
typedef std::map<int, struct rbd_openimage> rbd_open_image_map;
rbd_open_image_map rbd_open_images;            // Map of open images keyed by RBD name

// Describes an in-flight write operation
struct in_flight_write_data {
    char *rbd_name;      // RBD the write is going to
    uint64_t write_num;  // Unique number assigned to the write op
    uint64_t offset;     // Offset of the write operation
    char *buffer;        // Copy of the data to be written (not just the pointer)
};
typedef std::unordered_set<long> in_flight_write_set;
typedef std::map<std::string, in_flight_write_set> in_flight_write_map;
in_flight_write_map in_flight_writes;  // Map of in-flight write operations
uint64_t next_in_flight_write = 0;     // Number to assign to the next in-flight write op

// Default command line options
struct rbd_options rbd_options = {(char*) "/etc/ceph/ceph.conf", false, (char*) "rbd",
				  (char*)"", false, (uint64_t)128, (uint64_t)1024};

#define rbdsize(fd)	get_rbd_image_data(get_open_image(fd)->name)->num_sectors * get_rbd_image_data(get_open_image(fd)->name)->sector_size
#define rbdblksize(fd)	get_rbd_image_data(get_open_image(fd)->name)->rbd_stat.rbd_info.obj_size
#define rbdblkcnt(fd)	get_rbd_image_data(get_open_image(fd)->name)->rbd_stat.rbd_info.num_objs

uint64_t imagesize = 1024ULL * 1024 * 1024;
uint64_t imageorder = 22ULL;
uint64_t imagefeatures = 1ULL;
uint64_t imagesectorsize = 512;              // Assume RBDs always use 512-byte sectors

// Minimize calls to rbd_list: marks bracketing of opendir/<ops>/releasedir
int in_opendir;

/* prototypes */
int connect_to_cluster(rados_t *pcluster);
void build_image_data_from_list(rbd_image_data_set &rbd_image_data, void *list_buffer, size_t list_buffer_len);
void enumerate_images(rbd_image_data_set &rbd_image_data);
int open_rbd_image(const char *image_name);

void simple_err(const char *msg, int err);

#define WRITE_QUEUE_SLEEP 10         // Number of milliseconds to sleep in between checks on the write queue

#define SECONDS_PER_DAY 86400
#define SECONDS_PER_HOUR 3600
#define SECONDS_PER_MINUTE 60
#define MICROSECONDS_PER_MILLISECOND 1000

// Get the next available file descriptor and increment the counter
int
get_next_file_descriptor()
{
    file_descriptor_lock.lock();
    int file_descriptor = next_descriptor++;
    file_descriptor_lock.unlock();
    
    return file_descriptor;
}

// Get the number of image data structures in the map
size_t
get_rbd_image_data_count()
{
    image_data_lock.lock();
    size_t size = rbd_image_data.size();
    image_data_lock.unlock();
    
    return size;
}
// Get/add image data for a specific image
struct rbd_image_data*
get_rbd_image_data(std::string image_name)
{
    image_data_lock.lock();
    struct rbd_image_data *image_data = &rbd_image_data[image_name];
    image_data_lock.unlock();
    
    return image_data;
}

// Clear the rbd image data structure
void
clear_rbd_image_data()
{
    image_data_lock.lock();
    rbd_image_data.clear();
    image_data_lock.unlock();
}

// Get/add an open image record for an image
struct rbd_openimage*
get_open_image(int file_descriptor)
{
    open_images_lock.lock();
    struct rbd_openimage *open_image = &rbd_open_images[file_descriptor];
    open_images_lock.unlock();
    
    return open_image;
}

// Wait for the number of in-flight write operations to get down to the specified threshold
void
wait_for_in_flight_writes(std::string image_name, size_t threshold)
{
    //Obtain in_flight_writes_lock so nothing new can be added while we're waiting (blocks writes)
    in_flight_writes_lock.lock();
    while (in_flight_writes[image_name].size() > threshold) {
        // Yield the lock during the sleep to allow another thread to remove writes from the queue
        in_flight_writes_lock.unlock();
        usleep(1000 * WRITE_QUEUE_SLEEP);
        in_flight_writes_lock.lock();
    }
    in_flight_writes_lock.unlock();
}

// Add an in-flight write to the map (new async write op)
void
insert_in_flight_write(std::string image_name, long write_num)
{
    // A maximum of 0 is considered unbounded, otherwise wait until we're below the maximum
    if (max_writes_in_flight > 0)
        wait_for_in_flight_writes(image_name, max_writes_in_flight);
    in_flight_writes_lock.lock();
    in_flight_writes[image_name].insert(write_num);
    in_flight_writes_lock.unlock();
}

// Remove an in-flight write from the map (completed async write op)
void
remove_in_flight_write(std::string image_name, long write_num)
{
    in_flight_writes_lock.lock();
    in_flight_writes[image_name].erase(write_num);
    in_flight_writes_lock.unlock();
}

// Wait for all in-flight writes to complete and clear the map
void
clear_in_flight_writes()
{
    // Wait for all writes to complete for each image in the map
    for (in_flight_write_map::iterator i = in_flight_writes.begin(); i != in_flight_writes.end(); i++)
        wait_for_in_flight_writes(i->first, 0);
    in_flight_writes_lock.lock();
    in_flight_writes.clear();
    in_flight_writes_lock.unlock();
}

// Add a partition to rbd_image_data
void add_partition(uint32_t partition_num, const char *image_name, rbd_image_t image, uint64_t start_sector, uint64_t num_sectors)
{
    std::string partition_name(image_name);  // The partition file follows the same naming convention as the RBD kernel client
    partition_name += "-part";
    partition_name += std::to_string(partition_num);
    struct rbd_image_data *image_data = get_rbd_image_data(partition_name);
    image_data_lock.lock();
    image_data->image_name = image_name;
    image_data->rbd_stat = get_rbd_image_data(image_name)->rbd_stat;
    image_data->sector_size = imagesectorsize;
    image_data->starting_sector = start_sector;
    image_data->num_sectors = num_sectors;
    image_data_lock.unlock();
}

// GPT globally unique identifier
struct gpt_guid {
	uint32_t   time_low;
	uint16_t   time_mid;
	uint16_t   time_hi_and_version;
	uint8_t    clock_seq_hi;
	uint8_t    clock_seq_low;
	uint8_t    node[6];
};

// The GPT Partition entry array contains an array of GPT entries.
struct gpt_entry {
	struct gpt_guid     type;            // Purpose and type of the partition
	struct gpt_guid     partition_guid;  // Unique ID for this partition
	uint64_t            lba_start;       // LBA starting sector for the partition
	uint64_t            lba_end;         // LBA ending sector for the partition
	uint64_t            attrs;
	uint16_t            name[36];
};

// Parse the GPT entries and add any partitions found as files
int get_gpt_partitions(int file_descriptor)
{
    char gpt_signature[9];
    rbd_openimage *open_image = get_open_image(file_descriptor);
    // Read sector 1 and look for a GPT signature at the beginnning
    int ret = rbd_read(open_image->image, imagesectorsize, 8, gpt_signature);
    
    if (ret < 0)
        return ret;
    
    if (ret != 8)
        return -ENOENT;

    gpt_signature[8] = (char)NULL;
    if (strcmp(gpt_signature, "EFI PART") == 0)
    {
        int num_partitions;
        // Get the number of partition entries from the GPT header
        ret = rbd_read(open_image->image, imagesectorsize + 80, 4, (char *)&num_partitions);
        
        if (ret < 0)
            return ret;
        
        if (ret != 4)
            return -ENOENT;
        
        // Allocate an array large enough to hold all GPT entries and read them
        gpt_entry *partitions = (gpt_entry *)malloc(num_partitions * sizeof(gpt_entry));
        ret = rbd_read(open_image->image, 2 * imagesectorsize, num_partitions * sizeof(gpt_entry), (char *)partitions);
        struct gpt_guid empty_guid = {0, 0, 0, 0, 0, {0, 0, 0, 0, 0, 0}};
        
        // Any partition that has a non-empty type GUID gets added as a file
        for (int i = 0; i < num_partitions; i++)
            if (memcmp((void *)&partitions[i].type, (void *)&empty_guid, sizeof(gpt_guid)) != 0)
                add_partition(i + 1, open_image->name.c_str(), open_image->image, partitions[i].lba_start, partitions[i].lba_end - partitions[i].lba_start + 1);
    }
    
    return 0;
}

// MBR partition entry structure
struct mbr_partition
{
    uint8_t boot_flag;      // Flag indicating whether the partition is bootable
    uint8_t chs_begin[3];   // CHS value for the beginning of the partition
    uint8_t part_type;      // Partition type
    uint8_t chs_end[3];     // CHS value for the end of the partition
    uint32_t start_sector;  // Begining sector for the partition
    uint32_t num_sectors;   // Number of sectors contained in the partition
};

// Parse the MBR partition table and add any partitions found as files
int get_mbr_partitions(int file_descriptor)
{
    uint16_t boot_signature;
    struct mbr_partition partitions[4];
    rbd_openimage *open_image = get_open_image(file_descriptor);
    // Read the last 2 bytes of sector 0 into boot_signature
    int ret = rbd_read(open_image->image, 510, 2, (char *)&boot_signature);
    
    if (ret < 0)
        return ret;
        
    if (ret != 2)
        return -ENOENT;
    
    // Compare to 0xaa55 to see if this looks like a partition table
    if (boot_signature == 0xaa55)
    {
        // Read the partition information into the partitions array
        ret = rbd_read(open_image->image, 446, 64, (char *)partitions);
        
        // A partition type of 0xee indicates a protective MBR for a GPT layout, so look for GPT partitions.
        // Otherwise, any partition with a non-zero type code gets added as a file.
        for (int i = 0; i < 4; i++)
            if (partitions[i].part_type == 0xee)
                get_gpt_partitions(file_descriptor);
            else if (partitions[i].part_type != 0)
                add_partition(i + 1, open_image->name.c_str(), open_image->image, partitions[i].start_sector, partitions[i].num_sectors);
    }
    
    return 0;
}

// Scan the RBD image for partition tables and add any partitions as files
int get_rbd_partitions(int file_descriptor)
{
    return get_mbr_partitions(file_descriptor);
}

int get_rbd_image(std::string image_name)
{
    struct rbd_image_data *rbd = NULL;

    if (image_name.empty())
        return -1;

    rbd_image_t temp_rbd;

    // Get the next file descriptor and open the RBD
    int file_descriptor = get_next_file_descriptor();
    std::size_t snapshot_index = image_name.find('@');
    std::string image_only_name = image_name;
    std::string snapshot_name = "";
    int ret = 0;
    if (snapshot_index == std::string::npos) {
        simple_err("adding rbd", snapshot_index);
        ret = rbd_open(ioctx, image_name.c_str(), &temp_rbd, NULL);
    }
    else {
        simple_err("adding snapshot", snapshot_index);
        image_only_name = image_name.substr(0, snapshot_index);
        snapshot_name = image_name.substr(snapshot_index + 1, image_name.length() - snapshot_index - 1);
        ret = rbd_open_read_only(ioctx, image_only_name.c_str(), &temp_rbd, snapshot_name.c_str());
    }
    if (ret < 0) {
        simple_err("add_image_data: can't open: ", ret);
        return ret;
    }

    // Add the image and filedescriptor to the maps
    rbd_openimage *open_image = get_open_image(file_descriptor);
    open_images_lock.lock();
    open_image->name = image_name;
    open_image->image = temp_rbd;
    open_images_lock.unlock();
    rbd = get_rbd_image_data(image_name);
    image_data_lock.lock();
    rbd->image_name = image_only_name;
    rbd->snapshot_name = snapshot_name;
    rbd->sector_size = imagesectorsize;
    rbd->starting_sector = 0;
    image_data_lock.unlock();
    if (map_partitions) {
        ret = get_rbd_partitions(file_descriptor);

        if (ret < 0)
            return ret;
    }

    // Stat the RBD to populate the file info
    rbd_stat(open_image->image, &(rbd->rbd_stat.rbd_info),
             sizeof(rbd_image_info_t));
    image_data_lock.lock();
    rbd->rbd_stat.valid = 1;
    rbd->num_sectors = rbd->rbd_stat.rbd_info.size / rbd->sector_size;
    image_data_lock.unlock();

    if (expose_snapshots && (snapshot_index == std::string::npos)) {
        rbd_snap_info_t *snaps;
        int max_snaps = 0, snap_count = 0;

        do {
            snaps = (rbd_snap_info_t *)malloc(sizeof(*snaps) * max_snaps);
            snap_count = rbd_snap_list(open_image->image, snaps, &max_snaps);
            if (snap_count < 0)
                free(snaps);
        } while (snap_count == -ERANGE);

        for (int i = 0; i < snap_count; i++)
            get_rbd_image(image_name + '@' + snaps[i].name);

        rbd_snap_list_end(snaps);
    }

    return file_descriptor;
}

// Search for partitions in an image and add them to the 
// Build the image data map from a retrieved list of images
void
build_image_data_from_list(char *list_buffer, size_t list_buffer_len)
{
    char *ip;
    
    image_data_lock.lock();
    clear_rbd_image_data();
    free(rbd_image_list.buf);
    // Save the image list for a future comparison
    rbd_image_list.buf = malloc(list_buffer_len);
    rbd_image_list.buf_len = list_buffer_len;
    memcpy(rbd_image_list.buf, list_buffer, list_buffer_len);

    for (ip = list_buffer; ip < &list_buffer[list_buffer_len]; ip += strlen(ip) + 1)  {
        // Add the image to the map if there is room and we are either not restricting by name or the name matches
        if (((num_images == 0) ||
                (get_rbd_image_data_count() < num_images)) &&
                (mount_image_name.empty() ||
                ((mount_image_name.length() > 0) &&
                (mount_image_name.compare(ip) == 0)))) {
            fprintf(stderr, "%s, ", ip);
            get_rbd_image(ip);
        }
    }
    image_data_lock.unlock();
    fprintf(stderr, "\n");
}

// Enumerate the images in the pool
void
enumerate_images()
{
    int ret;
    char *ibuf = NULL;
    size_t ibuf_len = 0;

    // Determine what size the buffer needs to be
    ret = rbd_list(ioctx, ibuf, &ibuf_len);
    if (ret == -ERANGE) {
        assert(ibuf_len > 0);
        ibuf = (char*) malloc(ibuf_len);
        if (!ibuf) {
            simple_err("Failed to get ibuf", -ENOMEM);
            return;
        }
    } else if (ret < 0) {
        simple_err("Failed to get ibuf_len", ret);
        return;
    }

    // Retrieve the image list
    ret = rbd_list(ioctx, ibuf, &ibuf_len);
    if (ret < 0) {
        simple_err("Failed to populate ibuf", ret);
        free(ibuf);
        return;
    }

    fprintf(stderr, "pool %s: ", pool_name.c_str());
    // Only rebuild the image data if the image list has changed since last time
    if ((ibuf_len != rbd_image_list.buf_len) || (memcmp(ibuf, rbd_image_list.buf, ibuf_len) != 0))
        build_image_data_from_list(ibuf, ibuf_len);
    else
        fprintf(stderr, "\n");
}

// Open an image and return a file descriptor
int
open_rbd_image(const char *image_name)
{
    int file_descriptor = get_next_file_descriptor();
    struct rbd_openimage *open_image = get_open_image(file_descriptor);
    open_images_lock.lock();
    open_image->name = image_name;
    int ret = rbd_open(ioctx, get_rbd_image_data(image_name)->image_name.c_str(), &open_image->image, NULL);
    open_images_lock.unlock();
    if (ret < 0) {
        return ret;
    }
    return file_descriptor;
}

static int count_images(void)
{
    readdir_lock.lock();
    enumerate_images();
    readdir_lock.unlock();

    return get_rbd_image_data_count();
}

extern "C" {

static int rbdfs_getattr(const char *path, struct stat *stbuf)
{
	int fd;
	time_t now;

	if (!gotrados)
		return -ENXIO;

	if (path[0] == 0)
		return -ENOENT;

	memset(stbuf, 0, sizeof(struct stat));

	if (strcmp(path, "/") == 0) {

		now = time(NULL);
		stbuf->st_mode = S_IFDIR + 0755;
		stbuf->st_nlink = 2+count_images();
		stbuf->st_uid = getuid();
		stbuf->st_gid = getgid();
		stbuf->st_size = 1024;
		stbuf->st_blksize = 1024;
		stbuf->st_blocks = 1;
		stbuf->st_atime = now;
		stbuf->st_mtime = now;
		stbuf->st_ctime = now;

		return 0;
	}

	if (!in_opendir) {
		readdir_lock.lock();
		enumerate_images();
		readdir_lock.unlock();
	}
	fd = open_rbd_image(path + 1);
	if (fd < 0)
		return -ENOENT;

	now = time(NULL);
	stbuf->st_mode = S_IFREG | 0666;
	stbuf->st_nlink = 1;
	stbuf->st_uid = getuid();
	stbuf->st_gid = getgid();
	stbuf->st_size = rbdsize(fd);
	stbuf->st_blksize = rbdblksize(fd);
	stbuf->st_blocks = rbdblkcnt(fd);
	stbuf->st_atime = now;
	stbuf->st_mtime = now;
	stbuf->st_ctime = now;

	return 0;
}


static int rbdfs_open(const char *path, struct fuse_file_info *fi)
{
	int fd;

	if (!gotrados)
		return -ENXIO;

	if (path[0] == 0)
		return -ENOENT;

	readdir_lock.lock();
	enumerate_images();
	readdir_lock.unlock();
	fd = open_rbd_image(path + 1);
	if (fd < 0)
		return -ENOENT;

	fi->fh = fd;
	return 0;
}

static int rbdfs_read(const char *path, char *buf, size_t size,
			off_t offset, struct fuse_file_info *fi)
{
    std::string rbd_name(path + 1);
    struct rbd_image_data *image_data = get_rbd_image_data(rbd_name);
    
    if (offset + size > image_data->num_sectors * imagesectorsize)
        return -EINVAL;
    
    size_t numread;

    if (!gotrados)
            return -ENXIO;

    wait_for_in_flight_writes(rbd_name, 0);
    numread = 0;
    while (size > 0) {
        ssize_t ret;
        off_t partition_offset = image_data->starting_sector * image_data->sector_size;

        ret = rbd_read(get_open_image(fi->fh)->image, partition_offset + offset, size, buf);

        if (ret <= 0)
                break;
        buf += ret;
        size -= ret;
        offset += ret;
        numread += ret;
    }

    return numread;
}

// Callback for rbd_aio_write()
static void finish_aio_write(rbd_completion_t comp, void *data)
{
    struct in_flight_write_data *write_data = (in_flight_write_data*)data;
    // Remove the write op from the in-flight write set and free memory
    remove_in_flight_write(write_data->rbd_name, write_data->write_num);
    free(write_data->rbd_name);
    free(write_data->buffer);
    free(write_data);
}

static int rbdfs_write(const char *path, const char *buf, size_t size,
			 off_t offset, struct fuse_file_info *fi)
{
    return -EROFS;
    // Find the open RBD using its file descriptor
    rbd_openimage *open_image = get_open_image(fi->fh);
    struct rbd_image_data *image_data = get_rbd_image_data(open_image->name);
    
    if (map_partitions && offset + size > image_data->num_sectors * imagesectorsize)
        return -EOPNOTSUPP;
    
    if (!gotrados)
            return -ENXIO;

    // Resize if necessary
    if (!map_partitions && (size_t)(offset + size) > rbdsize(fi->fh)) {
        int r;
        fprintf(stderr, "rbdfs_write resizing %s to 0x%" PRIxMAX "\n",
            path, offset+size);
        r = rbd_resize(open_image->image, offset+size);
        if (r < 0)
            return r;

        r = rbd_stat(open_image->image, &image_data->rbd_stat.rbd_info, sizeof(rbd_image_info_t));
        if (r < 0)
            return r;
    }
    rbd_completion_t comp;
    off_t partition_offset = image_data->starting_sector * image_data->sector_size;
    struct in_flight_write_data *write_data = (in_flight_write_data*)malloc(sizeof(in_flight_write_data));
    write_data->rbd_name = strdup(image_data->image_name.c_str());
    write_data->write_num = next_in_flight_write++;
    write_data->offset = partition_offset + offset;
    write_data->buffer = (char*)malloc(size);
    // Copying the buffer is necessary for async writes since FUSE allocated it
    memcpy(write_data->buffer, buf, size);
    insert_in_flight_write(write_data->rbd_name, write_data->write_num);
    int r = rbd_aio_create_completion((void*)write_data, &finish_aio_write, &comp);
    if (r < 0)
        return r;
    r = rbd_aio_write(open_image->image, write_data->offset, size, write_data->buffer, comp);
    // Return an error if rbd_aio_write() failed, otherwise return the write size
    if (r < 0)
        return r;
    return size;
}

static void rbdfs_statfs_image_cb(void *num, const char *image)
{
	int	fd;

	((uint64_t *)num)[0]++;

	fd = open_rbd_image(image);
	if (fd >= 0)
		((uint64_t *)num)[1] += rbdsize(fd);
}

static void
iter_images(void *cookie,
	    void (*iter)(void *cookie, const char *image))
{
	readdir_lock.lock();
	image_data_lock.lock();
	for (rbd_image_data_set::iterator image = rbd_image_data.begin(); image != rbd_image_data.end(); image++)
		iter(cookie, (image->first).c_str());
        image_data_lock.unlock();
	readdir_lock.unlock();
}

static int rbdfs_statfs(const char *path, struct statvfs *buf)
{
	uint64_t num[2];

	if (!gotrados)
		return -ENXIO;

	num[0] = 1;
	num[1] = 0;
	readdir_lock.lock();
	enumerate_images();
	readdir_lock.unlock();
	iter_images(num, rbdfs_statfs_image_cb);

#define	RBDFS_BSIZE	4096
	buf->f_bsize = RBDFS_BSIZE;
	buf->f_frsize = RBDFS_BSIZE;
	buf->f_blocks = num[1] / RBDFS_BSIZE;
	buf->f_bfree = 0;
	buf->f_bavail = 0;
	buf->f_files = num[0];
	buf->f_ffree = 0;
	buf->f_favail = 0;
	buf->f_fsid = 0;
	buf->f_flag = 0;
	buf->f_namemax = PATH_MAX;

	return 0;
}

static int rbdfs_fsync(const char *path, int datasync,
			 struct fuse_file_info *fi)
{
    if (!gotrados)
        return -ENXIO;
    //rbd_flush(get_open_image(fi->fh)->image);
    return 0;
}

static int rbdfs_opendir(const char *path, struct fuse_file_info *fi)
{
    // only one directory, so global "in_opendir" flag should be fine
    readdir_lock.lock();
    in_opendir++;
    enumerate_images();
    readdir_lock.unlock();
    return 0;
}

struct rbdfs_readdir_info {
    void *buf;
    fuse_fill_dir_t filler;
};

static void rbdfs_readdir_cb(void *_info, const char *name)
{
    struct rbdfs_readdir_info *info = (struct rbdfs_readdir_info*) _info;

    info->filler(info->buf, name, NULL, 0);
}

static int rbdfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			   off_t offset, struct fuse_file_info *fi)
{
    struct rbdfs_readdir_info info = { buf, filler };

    if (!gotrados)
            return -ENXIO;
    if (!in_opendir)
            fprintf(stderr, "in readdir, but not inside opendir?\n");

    if (strcmp(path, "/") != 0)
            return -ENOENT;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    iter_images(&info, rbdfs_readdir_cb);

    return 0;
}
static int rbdfs_releasedir(const char *path, struct fuse_file_info *fi)
{
    // see opendir comments
    readdir_lock.lock();
    in_opendir--;
    readdir_lock.unlock();
    return 0;
}

void *
rbdfs_init(struct fuse_conn_info *conn)
{
    int ret;

    // init cannot fail, so if we fail here, gotrados remains at 0,
    // causing other operations to fail immediately with ENXIO

    ret = connect_to_cluster(&cluster);
    if (ret < 0)
            exit(90);

    pool_name = rbd_options.pool_name;
    mount_image_name = rbd_options.image_name;
    expose_snapshots = rbd_options.expose_snapshots;
    map_partitions = rbd_options.map_partitions;
    // Convert num_images, and max_writes_in_flight to integers instead of strings
    num_images = rbd_options.num_images;
    max_writes_in_flight = rbd_options.max_writes_in_flight;
    ret = rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
    if (ret < 0)
            exit(91);
#if FUSE_VERSION >= FUSE_MAKE_VERSION(2, 8)
    conn->want |= FUSE_CAP_BIG_WRITES;
#endif
    gotrados = 1;

    // init's return value shows up in fuse_context.private_data,
    // also to void (*destroy)(void *); useful?
    return NULL;
}

void
rbdfs_destroy(void *unused)
{
    if (!gotrados)
        return;
    clear_in_flight_writes();
    open_images_lock.lock();
    for (rbd_open_image_map::iterator open_image = rbd_open_images.begin();
            open_image != rbd_open_images.end();
            open_image++) {
        // Don't try to treat invalid "files" as rbd images in the event that the OS has done something goofy
        if (open_image->second.image != NULL) {
            //wait_for_in_flight_writes(open_image->second.name, 0);
            //rbd_flush(open_image->second.image);
            rbd_close(open_image->second.image);
        }
    }
    rbd_open_images.clear();
    open_images_lock.unlock();

    image_data_lock.lock();
    clear_rbd_image_data();
    free(rbd_image_list.buf);
    rbd_image_list.buf = NULL;
    rbd_image_list.buf_len = 0;
    image_data_lock.unlock();

    rados_ioctx_destroy(ioctx);
    rados_shutdown(cluster);
}

int
rbdfs_checkname(const char *checkname)
{
    const char *extra[] = {"@", "/"};
    std::string strCheckName(checkname);
    
    if (strCheckName.empty())
        return -EINVAL;

    unsigned int sz = sizeof(extra) / sizeof(const char*);
    for (unsigned int i = 0; i < sz; i++)
    {
        std::string ex(extra[i]);
        if (std::string::npos != strCheckName.find(ex))
            return -EINVAL;
    }

    return 0;
}

// return -errno on error.  fi->fh is not set until open time

int
rbdfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    return -EROFS;
    int r;
    int order = imageorder;

    r = rbdfs_checkname(path+1);
    if (r != 0)
    {
    return r;  
    }

    r = rbd_create2(ioctx, path+1, imagesize, imagefeatures, &order);
    return r;
}

int
rbdfs_rename(const char *path, const char *destname)
{
    return -EROFS;
    int r;

    r = rbdfs_checkname(destname+1);
    if (r != 0)
    {
      return r;
    }

    if (strcmp(path, "/") == 0)
        return -EINVAL;

    return rbd_rename(ioctx, path+1, destname+1);
}

int
rbdfs_utime(const char *path, struct utimbuf *utime)
{
    return -EROFS;
	// called on create; not relevant
	return 0;
}

int
rbdfs_unlink(const char *path)
{
    return -EROFS;
    std::string rbd_name(path + 1);
    open_images_lock.lock();
    for (rbd_open_image_map::iterator open_image = rbd_open_images.begin();
            open_image != rbd_open_images.end();
            open_image++) {
	if (open_image->second.name == rbd_name) {
            wait_for_in_flight_writes(rbd_name, 0);
            rbd_flush(open_image->second.image);
            rbd_close(open_image->second.image);
            rbd_open_images.erase(open_image->first);
	}
    }
    open_images_lock.unlock();
    return rbd_remove(ioctx, path+1);
}


int
rbdfs_truncate(const char *path, off_t size)
{
    return -EROFS;
    if (map_partitions)
        return -EOPNOTSUPP;

    int fd;
    int r;

    if ((fd = open_rbd_image(path+1)) < 0)
            return -ENOENT;

    rbd_openimage *open_image = get_open_image(fd);

    fprintf(stderr, "truncate %s to %" PRIdMAX " (0x%" PRIxMAX ")\n",
      path, size, size);
    r = rbd_resize(open_image->image, size);
    if (r < 0)
        return r;

    r = rbd_stat(open_image->image, &get_rbd_image_data(open_image->name)->rbd_stat.rbd_info,
        sizeof(rbd_image_info_t));
    if (r < 0)
            return r;
    return 0;
}

/**
 * set an xattr on path, with name/value, length size.
 * Presumably flags are from Linux, as in XATTR_CREATE or 
 * XATTR_REPLACE (both "set", but fail if exist vs fail if not exist.
 *
 * We accept xattrs only on the root node.
 *
 * All values converted with strtoull, so can be expressed in any base
 */

struct rbdfuse_attr {
	char *attrname;
	uint64_t *attrvalp;
} attrs[] = {
    { (char*) "user.rbdfuse.imagesize", &imagesize },
    { (char*) "user.rbdfuse.imageorder", &imageorder },
    { (char*) "user.rbdfuse.imagefeatures", &imagefeatures },
    { NULL, NULL }
};

int
rbdfs_setxattr(const char *path, const char *name, const char *value,
	       size_t size,
	       int flags
#if defined(DARWIN)
	       ,uint32_t pos
#endif
    )
{
    return -EROFS;
	struct rbdfuse_attr *ap;
	if (strcmp(path, "/") != 0)
		return -EINVAL;

	for (ap = attrs; ap->attrname != NULL; ap++) {
		if (strcmp(name, ap->attrname) == 0) {
			*ap->attrvalp = strtoull(value, NULL, 0);
			fprintf(stderr, "rbd-fuse: %s set to 0x%" PRIx64 "\n",
				ap->attrname, *ap->attrvalp);
			return 0;
		}
	}
	return -EINVAL;
}

int
rbdfs_getxattr(const char *path, const char *name, char *value,
		 size_t size
#if defined(DARWIN)
	       ,uint32_t position
#endif
  )
{
	struct rbdfuse_attr *ap;
	char buf[128];
	// allow gets on other files; ls likes to ask for things like
	// security.*

	for (ap = attrs; ap->attrname != NULL; ap++) {
		if (strcmp(name, ap->attrname) == 0) {
			snprintf(buf, 128, "%" PRIu64, *ap->attrvalp);
			if (value != NULL && size >= strlen(buf))
				strncpy(value, buf, size);
			fprintf(stderr, "rbd-fuse: get %s\n", ap->attrname);
			return (strlen(buf));
		}
	}
	return 0;
}

int
rbdfs_listxattr(const char *path, char *list, size_t len)
{
	struct rbdfuse_attr *ap;
	size_t required_len = 0;

	if (strcmp(path, "/") != 0)
		return -EINVAL;

	for (ap = attrs; ap->attrname != NULL; ap++)
		required_len += strlen(ap->attrname) + 1;
	if (len >= required_len) {
		for (ap = attrs; ap->attrname != NULL; ap++) {
			snprintf(list, len, "%s", ap->attrname);
			list += strlen(ap->attrname) + 1;
		}
	}
	return required_len;
}

const static struct fuse_operations rbdfs_oper = {
  getattr:    rbdfs_getattr,
  readlink:   0,
  getdir:     0,
  mknod:      0,
  mkdir:      0,
  unlink:     rbdfs_unlink,
  rmdir:      0,
  symlink:    0,
  rename:     rbdfs_rename,
  link:       0,
  chmod:      0,
  chown:      0,
  truncate:   rbdfs_truncate,
  utime:      rbdfs_utime,
  open:	      rbdfs_open,
  read:	      rbdfs_read,
  write:      rbdfs_write,
  statfs:     rbdfs_statfs,
  flush:      0,
  release:    0,
  fsync:      rbdfs_fsync,
  setxattr:   rbdfs_setxattr,
  getxattr:   rbdfs_getxattr,
  listxattr:  rbdfs_listxattr,
  removexattr: 0,
  opendir:    rbdfs_opendir,
  readdir:    rbdfs_readdir,
  releasedir: rbdfs_releasedir,
  fsyncdir:   0,
  init:	      rbdfs_init,
  destroy:    rbdfs_destroy,
  access:     0,
  create:     rbdfs_create,
  /* skip unimplemented */
};

} /* extern "C" */

enum {
	KEY_HELP,
	KEY_VERSION
};

static struct fuse_opt rbdfs_opts[] = {
    FUSE_OPT_KEY("-h", KEY_HELP),
    FUSE_OPT_KEY("--help", KEY_HELP),
    FUSE_OPT_KEY("-V", KEY_VERSION),
    FUSE_OPT_KEY("--version", KEY_VERSION),
    {"-c %s", offsetof(struct rbd_options, ceph_config), 0},
    {"--configfile=%s", offsetof(struct rbd_options, ceph_config), 0},
    {"-e", offsetof(struct rbd_options, expose_snapshots), 1},
    {"--expose-snapshots", offsetof(struct rbd_options, expose_snapshots), 1},
    {"-p %s", offsetof(struct rbd_options, pool_name), 0},
    {"--poolname=%s", offsetof(struct rbd_options, pool_name), 0},
    {"-r %s", offsetof(struct rbd_options, image_name), 0},
    {"--image=%s", offsetof(struct rbd_options, image_name), 0},
    {"-m", offsetof(struct rbd_options, map_partitions), 1},
    {"--map-partitions", offsetof(struct rbd_options, map_partitions), 1},
    {"-n %i", offsetof(struct rbd_options, num_images), 0},
    {"--num-images=%i", offsetof(struct rbd_options, num_images), 0},
    {"-w %i", offsetof(struct rbd_options, max_writes_in_flight), 0},
    {"--max-writes-in-flight=%i", offsetof(struct rbd_options, max_writes_in_flight), 0},
    FUSE_OPT_END
};

static void usage(const char *progname)
{
	fprintf(stderr,
"Usage: %s mountpoint [options]\n"
"\n"
"General options:\n"
"    -h   --help                   print help\n"
"    -V   --version                print version\n"
"    -c   --configfile             ceph configuration file [/etc/ceph/ceph.conf]\n"
"    -e   --expose-snapshots       expose image snapshots as read-only files\n"
"    -p   --poolname               rados pool name [rbd]\n"
"    -r   --image                  RBD image name []\n"
"    -m   --map-partitions         read image partition tables and create partition files in addition to image files\n"
"    -n   --num-images             number of images to mount [128]\n"
"    -w   --max-writes-in-flight   maximum number of writes in-flight before blocking [1024]\n"
"\n", progname);
}

static int rbdfs_opt_proc(void *data, const char *arg, int key,
			    struct fuse_args *outargs)
{
	if (key == KEY_HELP) {
		usage(outargs->argv[0]);
		fuse_opt_add_arg(outargs, "-ho");
		fuse_main(outargs->argc, outargs->argv, &rbdfs_oper, NULL);
		exit(1);
	}

	if (key == KEY_VERSION) {
		fuse_opt_add_arg(outargs, "--version");
		fuse_main(outargs->argc, outargs->argv, &rbdfs_oper, NULL);
		exit(0);
	}
    
	return 1;
}

void
simple_err(const char *msg, int err)
{
	fprintf(stderr, "%s: %s\n", msg, strerror(-err));
	return;
}

int
connect_to_cluster(rados_t *pcluster)
{
	int r;

	r = rados_create(pcluster, NULL);
	if (r < 0) {
		simple_err("Could not create cluster handle", r);
		return r;
	}
	rados_conf_parse_env(*pcluster, NULL);
	r = rados_conf_read_file(*pcluster, rbd_options.ceph_config);
	if (r < 0) {
		simple_err("Error reading Ceph config file", r);
		goto failed_shutdown;
	}
	r = rados_connect(*pcluster);
	if (r < 0) {
		simple_err("Error connecting to cluster", r);
		goto failed_shutdown;
	}

	return 0;

failed_shutdown:
	rados_shutdown(*pcluster);
	return r;
}

int main(int argc, char *argv[])
{
    // Initialize the cached image list to empty
    image_data_lock.lock();
    rbd_image_list.buf = NULL;
    rbd_image_list.buf_len = 0;
    image_data_lock.unlock();

	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

	if (fuse_opt_parse(&args, &rbd_options, rbdfs_opts, rbdfs_opt_proc) == -1) {
		exit(1);
	}

	return fuse_main(args.argc, args.argv, &rbdfs_oper, NULL);
}
