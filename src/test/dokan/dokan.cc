/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Cloudbase Solutions
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <windows.h>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <sys/socket.h>
#include <direct.h>

#include "gtest/gtest.h"

#include "common/SubProcess.h"
#include "common/run_cmd.h"
#include "include/uuid.h"

#define DEFAULT_MOUNTPOINT "X:\\"
#define MOUNT_POLL_ATTEMPT 10
#define MOUNT_POLL_INTERVAL_MS 1000
#define TEST_VOL_SERIAL "1234567890"
#define MByte 1048576

namespace fs = std::filesystem;
using namespace std::chrono_literals;

std::string get_uuid() {
    uuid_d suffix;
    suffix.generate_random();

    return suffix.to_string();
}

std::string to_upper(std::string& in) {
    std::string out = in;

    std::transform(
        out.begin(), out.end(), out.begin(),
        [](unsigned char c){
          return std::toupper(c);
        });

    return out;
}

bool move_eof(HANDLE handle, LARGE_INTEGER offset) {

    // Move file pointer to FILE_BEGIN + offset
    if (!SetFilePointerEx(handle, offset, NULL, FILE_BEGIN)) {
        std::cerr << "Setting file pointer failed. err: "
                  << GetLastError() << std::endl;
        return false;
    }

    if (!SetEndOfFile(handle)) {
        std::cerr << "Setting EOF failed. err: " << GetLastError() << std::endl;
        return false;
    }

    return true;
}

void write_file(std::string file_path, std::string data) {
    std::ofstream file;
    file.open(file_path);

    ASSERT_TRUE(file.is_open())
        << "Failed to open file: " << file_path;
    file << data;
    file.flush();

    file.close();
}

void expect_write_failure(std::string file_path) {
    std::ofstream file;
    file.open(file_path);

    ASSERT_FALSE(file.is_open());
}

std::string read_file(std::string file_path) {
    std::ifstream file;
    file.open(file_path);
    std::string content((std::istreambuf_iterator<char>(file)),
                         std::istreambuf_iterator<char>());
    file.close();

    return content;
}

void check_write_file(std::string file_path, std::string data) {
    write_file(file_path, data);
    ASSERT_EQ(read_file(file_path), data);
}

int wait_for_mount(std::string mount_path) {
    std::cerr << "Waiting for mount: " << mount_path << std::endl;

    int attempts = 0;
    do {
        attempts++;
        if (attempts < MOUNT_POLL_ATTEMPT)
            Sleep(MOUNT_POLL_INTERVAL_MS);
    } while (!fs::exists(mount_path)
             && attempts < MOUNT_POLL_ATTEMPT);

    if (!fs::exists(mount_path)) {
        std::cerr << "Timed out waiting for ceph-dokan mount: " 
                  << mount_path << std::endl;
        return -ETIMEDOUT;
    }

    std::cerr << "Successfully mounted: " << mount_path << std::endl;

    return 0;
}

void map_dokan(SubProcess** mount, const char* mountpoint) {
    SubProcess* new_mount = new SubProcess("ceph-dokan");

    new_mount->add_cmd_args("map", "--win-vol-name", "TestCeph",
                            "--win-vol-serial", TEST_VOL_SERIAL,
                            "-l", mountpoint, NULL);

    *mount = new_mount;
    ASSERT_EQ(new_mount->spawn(), 0);
    ASSERT_EQ(wait_for_mount(mountpoint), 0);
}

void map_dokan_read_only(
    SubProcess** mount,
    const char* mountpoint
) {
    SubProcess* new_mount = new SubProcess("ceph-dokan");
    new_mount->add_cmd_args("map", "--win-vol-name", "TestCeph",
                            "--win-vol-serial", TEST_VOL_SERIAL,
                            "--read-only", "-l", mountpoint, NULL);

    *mount = new_mount;
    ASSERT_EQ(new_mount->spawn(), 0);
    ASSERT_EQ(wait_for_mount(mountpoint), 0);
    std::cerr << mountpoint << " mounted in read-only mode"
              << std::endl;
}

void map_dokan_with_maxpath(
    SubProcess** mount,
    const char* mountpoint,
    uint64_t max_path_len)
{
    SubProcess* new_mount = nullptr;

    bool expect_failure = max_path_len < 256 || max_path_len > 4096;
    if (expect_failure) {
        new_mount = new SubProcessTimed(
            "ceph-dokan",
            SubProcess::CLOSE, SubProcess::CLOSE, SubProcess::CLOSE,
            MOUNT_POLL_ATTEMPT * MOUNT_POLL_INTERVAL_MS / 1000);
    } else {
        new_mount = new SubProcess("ceph-dokan");
    }

    new_mount->add_cmd_args("map", "--debug", "--dokan-stderr",
                            "--win-vol-name", "TestCeph",
                            "--win-vol-serial", TEST_VOL_SERIAL,
                            "--max-path-len",
                            (std::to_string(max_path_len)).c_str(),
                            "-l", mountpoint, NULL);

    *mount = new_mount;
    ASSERT_EQ(new_mount->spawn(), 0);
    if (expect_failure) {
        ASSERT_NE(0, new_mount->join());
    } else {
        ASSERT_EQ(wait_for_mount(mountpoint), 0);
    }
}

void map_dokan_case_insensitive(SubProcess** mount, const char* mountpoint,
                                bool force_lowercase=false) {
    SubProcess* new_mount = new SubProcess("ceph-dokan");

    new_mount->add_cmd_args("map", "--win-vol-name", "TestCeph",
                            "--win-vol-serial", TEST_VOL_SERIAL,
                            "-l", mountpoint, "--case-insensitive", NULL);
    if (force_lowercase) {
        new_mount->add_cmd_args("--force-lowercase", NULL);
    }

    *mount = new_mount;
    ASSERT_EQ(new_mount->spawn(), 0);
    ASSERT_EQ(wait_for_mount(mountpoint), 0);
}

void unmap_dokan(SubProcess* mount, const char* mountpoint) {
    std::string ret = run_cmd("ceph-dokan", "unmap", "-l",
                              mountpoint, (char*)NULL);
                              
    ASSERT_EQ(ret, "") << "Failed unmapping: " << mountpoint;
    std::cerr<< "Unmounted: " << mountpoint << std::endl;

    ASSERT_EQ(mount->join(), 0);
}

int get_volume_max_path(std::string mountpoint){
    char volume_name[MAX_PATH + 1] = { 0 };
    char file_system_name[MAX_PATH + 1] = { 0 };
    DWORD serial_number = 0;
    DWORD max_component_len = 0;
    DWORD file_system_flags = 0;
    if (GetVolumeInformation(
            mountpoint.c_str(),
            volume_name,
            sizeof(volume_name),
            &serial_number,
            &max_component_len,
            &file_system_flags,
            file_system_name,
            sizeof(file_system_name)) != TRUE) {
        std::cerr << "GetVolumeInformation() failed, error: "
                  << GetLastError() << std::endl;
    }

    return max_component_len;
}

static SubProcess* shared_mount = nullptr;

class DokanTests : public testing::Test
{
protected:

    static void SetUpTestSuite() {
        map_dokan(&shared_mount, DEFAULT_MOUNTPOINT);
    }

    static void TearDownTestSuite() {
        if (shared_mount) {
            unmap_dokan(shared_mount, DEFAULT_MOUNTPOINT);
        }
        shared_mount = nullptr;
    }
};

TEST_F(DokanTests, test_mount) {
    std::string mountpoint = "Y:\\";
    SubProcess* mount = nullptr;
    map_dokan(&mount, mountpoint.c_str());
    unmap_dokan(mount, mountpoint.c_str());
}

TEST_F(DokanTests, test_mount_read_only) {
    std::string mountpoint = "Z:\\";
    std::string data = "abc123";
    std::string success_file_path = "ro_success_" + get_uuid();
    std::string failed_file_path = "ro_fail_" + get_uuid();

    SubProcess* mount = nullptr;
    map_dokan(&mount, mountpoint.c_str());

    check_write_file(mountpoint + success_file_path, data);
    ASSERT_TRUE(fs::exists(mountpoint + success_file_path));

    unmap_dokan(mount, mountpoint.c_str());

    mount = nullptr;
    map_dokan_read_only(&mount, mountpoint.c_str());

    expect_write_failure(mountpoint + failed_file_path);
    ASSERT_FALSE(fs::exists(mountpoint + failed_file_path));

    ASSERT_TRUE(fs::exists(mountpoint + success_file_path));
    ASSERT_EQ(read_file(mountpoint + success_file_path), data);

    // The actual exception message is runtime dependent.
    EXPECT_THROW({
        fs::remove(mountpoint + success_file_path);
    }, fs::filesystem_error);
    unmap_dokan(mount, mountpoint.c_str());

    map_dokan(&mount, mountpoint.c_str());
    ASSERT_TRUE(fs::exists(mountpoint + success_file_path));
    ASSERT_TRUE(fs::remove(mountpoint + success_file_path));
    unmap_dokan(mount, mountpoint.c_str());
}

TEST_F(DokanTests, test_delete_on_close) {
    std::string file_path = DEFAULT_MOUNTPOINT"file_" + get_uuid();
    HANDLE hFile = CreateFile(
        file_path.c_str(),
        GENERIC_WRITE, // open for writing
        0,             // sharing mode, none in this case
        0,             // use default security descriptor
        CREATE_NEW,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_DELETE_ON_CLOSE,
        0);

    ASSERT_NE(hFile, INVALID_HANDLE_VALUE)
        << "Could not open file: "
        << DEFAULT_MOUNTPOINT"test_create.txt "
        << "err: " << GetLastError() << std::endl;
        
    ASSERT_NE(CloseHandle(hFile), 0);
    
    // FILE_FLAG_DELETE_ON_CLOSE is used
    ASSERT_FALSE(fs::exists(file_path));
}

TEST_F(DokanTests, test_io) {
    std::string data = "abcdef";
    std::string file_path = "test_io_" + get_uuid();

    std::string mountpoint = "I:\\";
    SubProcess* mount = nullptr;
    map_dokan(&mount, mountpoint.c_str());

    check_write_file(mountpoint + file_path, data);
    ASSERT_TRUE(fs::exists(mountpoint + file_path));
    unmap_dokan(mount, mountpoint.c_str());

    mountpoint = "O:\\";
    mount = nullptr;
    map_dokan(&mount, mountpoint.c_str());

    ASSERT_TRUE(fs::exists(mountpoint + file_path));
    EXPECT_EQ(data, read_file(mountpoint + file_path));
    ASSERT_TRUE(fs::remove((mountpoint + file_path).c_str()));
    ASSERT_FALSE(fs::exists(mountpoint + file_path));
    unmap_dokan(mount, mountpoint.c_str());
}

TEST_F(DokanTests, test_subfolders) {
    std::string base_dir_path = DEFAULT_MOUNTPOINT"base_dir_"
                                + get_uuid() + "\\";
    std::string sub_dir_path = base_dir_path
                               + "test_sub_dir" + get_uuid();
    std::string base_dir_file = base_dir_path 
                                + "file_" + get_uuid();
    std::string sub_dir_file = sub_dir_path 
                                + "file_" + get_uuid();

    std::string data = "abc";

    ASSERT_EQ(fs::create_directory(base_dir_path), true);
    ASSERT_TRUE(fs::exists(base_dir_path));

    ASSERT_EQ(fs::create_directory(sub_dir_path), true);
    ASSERT_TRUE(fs::exists(sub_dir_path));

    check_write_file(base_dir_file, data);
    ASSERT_TRUE(fs::exists(base_dir_file));

    check_write_file(sub_dir_file, data);
    ASSERT_TRUE(fs::exists(sub_dir_file));

    ASSERT_TRUE(fs::remove((sub_dir_file).c_str()))
        << "Failed to remove file: " << sub_dir_file;
    ASSERT_FALSE(fs::exists(sub_dir_file));

    // Remove empty dir
    ASSERT_TRUE(fs::remove((sub_dir_path).c_str()))
        << "Failed to remove directory: " << sub_dir_path;
    ASSERT_FALSE(fs::exists(sub_dir_file));

    ASSERT_NE(fs::remove_all((base_dir_path).c_str()), 0)
        << "Failed to remove directory: " << base_dir_path;
    ASSERT_FALSE(fs::exists(sub_dir_file));
}

TEST_F(DokanTests, test_find_files) {
    std::string basedir_path = "X:/find_" + get_uuid();
    std::string subdir_path = basedir_path + "/dir_" + get_uuid();
    std::string file1_path = basedir_path + "/file1_" + get_uuid();
    std::string file2_path = subdir_path + "/file2_" + get_uuid();

    ASSERT_TRUE(
        fs::create_directories(subdir_path)
    );

    std::ofstream{file1_path};
    std::ofstream{file2_path};

    std::vector<std::string> paths;

    for (const auto & entry : 
         fs::recursive_directory_iterator(basedir_path)
    ) {
        paths.push_back(entry.path().generic_string());
    }

    ASSERT_NE(std::find(begin(paths), end(paths), subdir_path), end(paths));
    ASSERT_NE(std::find(begin(paths), end(paths), file1_path), end(paths));
    ASSERT_NE(std::find(begin(paths), end(paths), file2_path), end(paths));

    // clean-up
    ASSERT_NE(fs::remove_all(basedir_path), 0);
}

TEST_F(DokanTests, test_move_file) {
    std::string dir1_path = DEFAULT_MOUNTPOINT
                            "test_mv_1_" + get_uuid() + "\\";
    std::string dir2_path = DEFAULT_MOUNTPOINT
                            "test_mv_2_" + get_uuid() + "\\";
    std::string file_name = "mv_file_" + get_uuid();
    std::string data = "abcd";

    ASSERT_TRUE(fs::create_directory(dir1_path));
    ASSERT_TRUE(fs::create_directory(dir2_path));

    check_write_file(dir1_path + file_name, data);

    fs::rename(dir1_path + file_name, dir2_path + file_name);

    ASSERT_TRUE(fs::exists(dir2_path + file_name));
    ASSERT_FALSE(fs::exists(dir1_path + file_name));

    ASSERT_EQ(data, read_file(dir2_path + file_name));

    // clean-up
    ASSERT_NE(fs::remove_all(dir1_path),0);
    ASSERT_NE(fs::remove_all(dir2_path),0);
}

TEST_F(DokanTests, test_max_path) {
    std::string mountpoint = "P:\\";
    std::string extended_mountpoint = "\\\\?\\" + mountpoint;
    SubProcess* mount = nullptr;
    char dir[200] = { 0 };
    char file[200] = { 0 };
    std::string data = "abcd1234";

    memset(dir, 'd', sizeof(dir) - 1);
    memset(file, 'f', sizeof(file) - 1);

    uint64_t max_path_len = 4096;
    
    map_dokan_with_maxpath(&mount,
                           mountpoint.c_str(),
                           max_path_len);
    EXPECT_EQ(get_volume_max_path(extended_mountpoint),
              max_path_len);

    std::string long_dir_path = extended_mountpoint;

    std::string dir_names[15];

    for (int i = 0; i < 15; i++) {
        std::string crt_dir = std::string(dir) + "_"
                              + get_uuid() + "\\";
        long_dir_path.append(crt_dir);
        int stat = _mkdir(long_dir_path.c_str());
        ASSERT_EQ(stat, 0) << "Error creating directory " << i
                           << ": " << GetLastError() << std::endl;
        dir_names[i] = crt_dir;
    }
    std::string file_path = long_dir_path + "\\" + std::string(file)
                            + "_" + get_uuid();

    check_write_file(file_path, data);

    // clean-up
    // fs::remove is unable to handle long Windows paths
    EXPECT_NE(DeleteFileA(file_path.c_str()), 0);

    for (int i = 14; i >= 0; i--) {
        std::string remove_dir = extended_mountpoint;
        for (int j = 0; j <= i; j++) {
            remove_dir.append(dir_names[j]);
        }
        
        EXPECT_NE(RemoveDirectoryA(remove_dir.c_str()), 0);
    }

    unmap_dokan(mount, mountpoint.c_str());

    // value exceeds 32767, so a failure is expected
    max_path_len = 32770;
    map_dokan_with_maxpath(&mount,
                           mountpoint.c_str(),
                           max_path_len);
    ASSERT_FALSE(fs::exists(mountpoint));

    // value is below 256, so a failure is expected
    max_path_len = 150;
    map_dokan_with_maxpath(&mount,
                           mountpoint.c_str(),
                           max_path_len);
    ASSERT_FALSE(fs::exists(mountpoint));

    // default value
    map_dokan(&mount, mountpoint.c_str());
    EXPECT_EQ(get_volume_max_path(mountpoint.c_str()), 256);

    unmap_dokan(mount, mountpoint.c_str());
}

TEST_F(DokanTests, test_set_eof) {
    std::string file_path = DEFAULT_MOUNTPOINT"test_eof_"
                            + get_uuid();
    HANDLE hFile = CreateFile(
        file_path.c_str(),
        GENERIC_WRITE, // open for writing
        0,             // sharing mode, none in this case
        0,             // use default security descriptor
        CREATE_NEW,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_DELETE_ON_CLOSE,
        0);

    ASSERT_NE(hFile, INVALID_HANDLE_VALUE)
        << "Could not open file: "
        << DEFAULT_MOUNTPOINT"test_create.txt "
        << "err: " << GetLastError() << std::endl;

    LARGE_INTEGER offset;
    offset.QuadPart = 2 * MByte; // 2MB

    LARGE_INTEGER file_size;

    ASSERT_TRUE(move_eof(hFile, offset));
    ASSERT_NE(GetFileSizeEx(hFile, &file_size), 0);
    EXPECT_EQ(file_size.QuadPart, offset.QuadPart);

    offset.QuadPart = MByte; // 1MB

    ASSERT_TRUE(move_eof(hFile, offset));
    ASSERT_NE(GetFileSizeEx(hFile, &file_size), 0);
    EXPECT_EQ(file_size.QuadPart, offset.QuadPart);

    ASSERT_NE(CloseHandle(hFile), 0);
    
    // FILE_FLAG_DELETE_ON_CLOSE is used
    ASSERT_FALSE(fs::exists(file_path));
}

TEST_F(DokanTests, test_set_alloc_size) {
    std::string file_path = DEFAULT_MOUNTPOINT"test_alloc_size_"
                            + get_uuid();
    HANDLE hFile = CreateFile(
        file_path.c_str(),
        GENERIC_WRITE, // open for writing
        0,             // sharing mode, none in this case
        0,             // use default security descriptor
        CREATE_NEW,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_DELETE_ON_CLOSE,
        0);

    ASSERT_NE(hFile, INVALID_HANDLE_VALUE)
        << "Could not open file: "
        << DEFAULT_MOUNTPOINT"test_create.txt "
        << "err: " << GetLastError() << std::endl;
    
    LARGE_INTEGER li;
    li.QuadPart = MByte;
    FILE_ALLOCATION_INFO fai;
    fai.AllocationSize = li;
    
    ASSERT_NE(SetFileInformationByHandle(
                hFile,
                FileAllocationInfo,
                &fai,
                sizeof(FILE_ALLOCATION_INFO)
             ),0) << "Error: " << GetLastError();

    LARGE_INTEGER offset;
    offset.QuadPart = 2 * MByte;
    LARGE_INTEGER file_size;

    ASSERT_TRUE(move_eof(hFile, offset));
    ASSERT_NE(GetFileSizeEx(hFile, &file_size), 0);
    EXPECT_EQ(file_size.QuadPart, offset.QuadPart);

    offset.QuadPart = MByte;

    ASSERT_TRUE(move_eof(hFile, offset));
    ASSERT_NE(GetFileSizeEx(hFile, &file_size), 0);
    EXPECT_EQ(file_size.QuadPart, offset.QuadPart);

    ASSERT_NE(CloseHandle(hFile), 0);
    
    // FILE_FLAG_DELETE_ON_CLOSE is used
    ASSERT_FALSE(fs::exists(file_path));
}

TEST_F(DokanTests, test_file_type) {
    std::string test_dir = DEFAULT_MOUNTPOINT"test_info_"
                            + get_uuid() + "\\";
    std::string file_path = test_dir + "file_"
                            + get_uuid();
    std::string dir_path = test_dir + "dir_"
                           + get_uuid() + "\\";

    ASSERT_TRUE(fs::create_directory(test_dir));

    std::ofstream{file_path};
    ASSERT_TRUE(fs::create_directory(dir_path));

    ASSERT_TRUE(fs::is_regular_file(fs::status(file_path)));
    ASSERT_TRUE(fs::is_directory(fs::status(dir_path)));

    // clean-up
    fs::remove_all(test_dir);

}

TEST_F(DokanTests, test_volume_info) {
    char volume_name[MAX_PATH + 1] = { 0 };
    char file_system_name[MAX_PATH + 1] = { 0 };
    DWORD serial_number = 0;
    DWORD max_component_len = 0;
    DWORD file_system_flags = 0;

    ASSERT_EQ(
        GetVolumeInformation(
            DEFAULT_MOUNTPOINT,
            volume_name,
            sizeof(volume_name),
            &serial_number,
            &max_component_len,
            &file_system_flags,
            file_system_name,
            sizeof(file_system_name)),TRUE) 
        << "GetVolumeInformation() failed, error: "
        << GetLastError() << std::endl;

    ASSERT_STREQ(volume_name, "TestCeph") 
        << "Received: " << volume_name << std::endl;
    ASSERT_STREQ(file_system_name, "Ceph")
        << "Received: " << file_system_name << std::endl;
    ASSERT_EQ(max_component_len, 256);
    ASSERT_EQ(serial_number, std::stoi(TEST_VOL_SERIAL))
        << "Received: " << serial_number << std::endl;

    // Consider adding specific flags 
    // and check for them
    // ASSERT_EQ(file_system_flags, 271);
}

TEST_F(DokanTests, test_get_free_space) {
    std::error_code ec;
    const std::filesystem::space_info si = 
        std::filesystem::space(DEFAULT_MOUNTPOINT, ec);
    ASSERT_EQ(ec.value(), 0);

    ASSERT_NE(static_cast<std::intmax_t>(si.capacity), 0);
    ASSERT_NE(static_cast<std::intmax_t>(si.free), 0);
    ASSERT_NE(static_cast<std::intmax_t>(si.available), 0);
}

TEST_F(DokanTests, test_file_timestamp) {
    std::string file1 = DEFAULT_MOUNTPOINT"test_time1_"
                            + get_uuid();
    std::string file2 = DEFAULT_MOUNTPOINT"test_time2_"
                            + get_uuid();
    std::string file3 = DEFAULT_MOUNTPOINT"test_time3_"
                            + get_uuid();

    std::ofstream{file1};
    Sleep(1000);
    std::ofstream{file2};
    Sleep(1000);
    std::ofstream{file3};

    int64_t file1_creation = fs::last_write_time(file1)
                                 .time_since_epoch().count();
    int64_t file2_creation = fs::last_write_time(file2)
                                 .time_since_epoch().count();
    int64_t file3_creation = fs::last_write_time(file3)
                                 .time_since_epoch().count();

    EXPECT_LT(file1_creation, file2_creation);
    EXPECT_LT(file2_creation, file3_creation);

    // add 1h to file 1 creation time
    fs::file_time_type file1_time = fs::last_write_time(file1);

    fs::last_write_time(file1, file1_time + 1h);

    int64_t file1_new_time = fs::last_write_time(file1)
                                 .time_since_epoch().count();

    EXPECT_EQ((file1_time + 1h).time_since_epoch().count(),
              file1_new_time);
    EXPECT_GT(file1_new_time, file2_creation);
    EXPECT_GT(file1_new_time, file3_creation);

    ASSERT_TRUE(fs::remove(file1));
    ASSERT_TRUE(fs::remove(file2));
    ASSERT_TRUE(fs::remove(file3));
}

TEST_F(DokanTests, test_delete_disposition) {
    std::string file_path = DEFAULT_MOUNTPOINT"test_disp_"
                            + get_uuid();
    HANDLE hFile = CreateFile(file_path.c_str(), 
                               GENERIC_ALL, // required for delete
                               0, // exclusive access
                               NULL, 
                               CREATE_ALWAYS,
                               0, 
                               NULL);
    
    ASSERT_NE(hFile, INVALID_HANDLE_VALUE)
        << "Could not open file: " << file_path
        << "err: " << GetLastError() << std::endl;
  
    FILE_DISPOSITION_INFO fdi;
    fdi.DeleteFile = TRUE; // marking for deletion

    ASSERT_NE(
        SetFileInformationByHandle(
            hFile,
            FileDispositionInfo,
            &fdi,
            sizeof(FILE_DISPOSITION_INFO)), 0);

    ASSERT_NE(CloseHandle(hFile), 0);
    ASSERT_FALSE(fs::exists(file_path));    
}

bool check_create_disposition(std::string path, DWORD disposition) {
    HANDLE hFile = CreateFile(path.c_str(), 
                              GENERIC_WRITE,
                              0, // exclusive access
                              NULL,
                              disposition,
                              0, 
                              NULL);

    if(hFile == INVALID_HANDLE_VALUE) {
        return false;
    }

    if(CloseHandle(hFile) == 0) {
        return false;
    }

    return true;
}

TEST_F(DokanTests, test_create_dispositions) {
    std::string file_path = DEFAULT_MOUNTPOINT"test_create_"
                            + get_uuid();
    std::string non_existant_file = DEFAULT_MOUNTPOINT
                                    "test_create_" + get_uuid();
  
    EXPECT_TRUE(
        check_create_disposition(file_path, CREATE_NEW));
    
    // CREATE_ALWAYS with existing file
    EXPECT_TRUE(
        check_create_disposition(file_path, CREATE_ALWAYS));
    EXPECT_EQ(GetLastError(), ERROR_ALREADY_EXISTS);

    // CREATE_NEW with existing file
    EXPECT_FALSE(
        check_create_disposition(file_path, CREATE_NEW));
    EXPECT_EQ(GetLastError(), ERROR_FILE_EXISTS);

    // OPEN_EXISTING with existing file
    EXPECT_TRUE(
        check_create_disposition(file_path, OPEN_EXISTING));

    ASSERT_FALSE(fs::exists(non_existant_file));
    // OPEN_EXISTING with non-existant file
    EXPECT_FALSE(
        check_create_disposition(non_existant_file, OPEN_EXISTING));
    EXPECT_EQ(GetLastError(), ERROR_FILE_NOT_FOUND);

    // OPEN_ALWAYS with existing file
    EXPECT_TRUE(
        check_create_disposition(file_path, OPEN_ALWAYS));
    EXPECT_EQ(GetLastError(), ERROR_ALREADY_EXISTS);

    ASSERT_FALSE(fs::exists(non_existant_file));
    // OPEN_ALWAYS with non-existant file
    EXPECT_TRUE(
        check_create_disposition(non_existant_file, OPEN_ALWAYS));
    EXPECT_EQ(GetLastError(), 0);
    
    ASSERT_TRUE(fs::remove(non_existant_file));

    // TRUNCATE_EXISTING with existing file
    EXPECT_TRUE(
        check_create_disposition(file_path, TRUNCATE_EXISTING));

    // TRUNCATE_EXISTING with non-existant file
    EXPECT_FALSE(
        check_create_disposition(non_existant_file,
                                 TRUNCATE_EXISTING));
    EXPECT_EQ(GetLastError(), ERROR_FILE_NOT_FOUND);

    // clean-up
    ASSERT_TRUE(fs::remove(file_path));
}

TEST_F(DokanTests, test_case_sensitive) {
    std::string test_dir = DEFAULT_MOUNTPOINT"test_dir" + get_uuid() + "\\";
    std::string lower_file_path = test_dir + "file_" + get_uuid();
    std::string upper_file_path = to_upper(lower_file_path);

    ASSERT_TRUE(fs::create_directory(test_dir));
    std::ofstream{lower_file_path};

    ASSERT_TRUE(fs::exists(lower_file_path));
    ASSERT_FALSE(fs::exists(upper_file_path));

    // clean-up
    fs::remove_all(test_dir);
}

void test_case_insensitive(bool force_lowercase) {
    std::string mountpoint = "Q:\\";
    std::string test_dir = mountpoint + "test_dir" + get_uuid() + "/";
    std::string file_name = "file_" + get_uuid();
    std::string lower_file_path = test_dir + file_name;
    std::string upper_file_path = to_upper(lower_file_path);

    SubProcess* mount = nullptr;
    map_dokan_case_insensitive(&mount, mountpoint.c_str(), force_lowercase);

    ASSERT_TRUE(fs::create_directory(test_dir));
    std::ofstream{upper_file_path};

    ASSERT_TRUE(fs::exists(lower_file_path));
    ASSERT_TRUE(fs::exists(upper_file_path));

    std::vector<std::string> paths;
    for (const auto & entry : fs::recursive_directory_iterator(test_dir)) {
        paths.push_back(entry.path().filename().generic_string());
    }

    bool found_lowercase = std::find(
        begin(paths), end(paths), file_name) != end(paths);
    bool found_uppercase = std::find(
        begin(paths), end(paths), to_upper(file_name)) != end(paths);

    ASSERT_EQ(found_lowercase, force_lowercase);
    ASSERT_NE(found_uppercase, force_lowercase);

    // clean-up
    fs::remove_all(test_dir);

    unmap_dokan(mount, mountpoint.c_str());
}

TEST_F(DokanTests, test_case_insensitive_force_lower) {
    test_case_insensitive(true);
}

TEST_F(DokanTests, test_case_insensitive_force_upper) {
   test_case_insensitive(false);
}
