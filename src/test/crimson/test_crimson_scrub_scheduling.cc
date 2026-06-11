// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "include/rados/librados.h"
#include <errno.h>

// parameters for the test
#define TEST_POOL_NAME "crimson-test-pool"
#define TEST_OBJ_PREFIX "scrub-test-obj"
#define NUM_OBJS 50
#define OBJ_SIZE 4096

// execute a ceph CLI command and capture its output (simplified, not robust)
static int exec_ceph_cmd(const char *cmd, char *out_buf, size_t buf_len) {
    char full_cmd[512];
    snprintf(full_cmd, sizeof(full_cmd), "ceph %s --format json", cmd);
    FILE *fp = popen(full_cmd, "r");
    if (!fp) return -1;
    if (out_buf && buf_len > 0) {
        fread(out_buf, 1, buf_len - 1, fp);
    }
    return pclose(fp);
}
static int remove_scrub_settings_global(rados_t cluster) {
    printf("--- step 1: remove Scrub time window (global) ---\n");
    struct {
        const char *name;
    } configs[] = {
        {"osd_scrub_begin_hour"},
        {"osd_scrub_end_hour"},
        {"osd_scrub_min_interval"},
        {"osd_scrub_max_interval"},
        {"osd_deep_scrub_interval"}
    };
    int num = sizeof(configs) / sizeof(configs[0]);

    for (int i = 0; i < num; i++) {
        char cmd_json[256];
        snprintf(cmd_json, sizeof(cmd_json),
                 "{\"prefix\":\"config rm\", \"who\":\"global\", \"name\":\"%s\"}",
                 configs[i].name);
        const char *cmd_arr[] = {cmd_json, NULL};
        char *outbuf = NULL, *errbuf = NULL;
        size_t outlen = 0, errlen = 0;

        int ret = rados_mon_command(cluster, cmd_arr, 1,
                                    NULL, 0, &outbuf, &outlen, &errbuf, &errlen);
        if (ret < 0) {
            fprintf(stderr, "command failed: %s\n  error: %s\n", cmd_json, errbuf ? errbuf : strerror(-ret));
            free(errbuf);
            free(outbuf);
            return ret;
        }
        printf("success: %s\n", cmd_json);
        free(outbuf);
        free(errbuf);
    }
    return 0;
}
// configure scrub settings to allow scrubbing at any time and with short intervals
static int config_scrub_settings_global(rados_t cluster) {
    printf("--- step 1: configure Scrub time window (global) allow 24-hour execution---\n");
    struct {
        const char *name;
        const char *value;
    } configs[] = {
        {"osd_scrub_begin_hour", "0"},
        {"osd_scrub_end_hour", "23"},
        {"osd_scrub_min_interval", "1"},
        {"osd_scrub_max_interval", "1"},
        {"osd_deep_scrub_interval", "1"}
    };
    int num = sizeof(configs) / sizeof(configs[0]);

    for (int i = 0; i < num; i++) {
        char cmd_json[256];
        snprintf(cmd_json, sizeof(cmd_json),
                 "{\"prefix\":\"config set\", \"who\":\"global\", \"name\":\"%s\", \"value\":\"%s\"}",
                 configs[i].name, configs[i].value);
        const char *cmd_arr[] = {cmd_json, NULL};
        char *outbuf = NULL, *errbuf = NULL;
        size_t outlen = 0, errlen = 0;

        int ret = rados_mon_command(cluster, cmd_arr, 1,
                                    NULL, 0, &outbuf, &outlen, &errbuf, &errlen);
        if (ret < 0) {
            fprintf(stderr, "command failed: %s\n  error: %s\n", cmd_json, errbuf ? errbuf : strerror(-ret));
            free(errbuf);
            free(outbuf);
            return ret;
        }
        printf("success: %s\n", cmd_json);
        free(outbuf);
        free(errbuf);
    }
    return 0;
}

static int remove_pool(rados_t cluster, const char *pool_name) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd),
             "{\"prefix\":\"osd pool delete\", \"pool\":\"%s\", \"yes-i-really-really-mean-it\":true}",
             pool_name);

    const char *cmd_arr[] = {cmd, NULL};
    char *outbuf = NULL, *errbuf = NULL;
    size_t outlen = 0, errlen = 0;

    int ret = rados_mon_command(cluster, cmd_arr, 1, NULL, 0, &outbuf, &outlen, &errbuf, &errlen);

    if (ret < 0) {
        if (ret == -ENOENT) {
            printf("Pool '%s' does not exist.\n", pool_name);
            rados_buffer_free(errbuf);
            return 0;
        }
        fprintf(stderr, "Failed to delete pool '%s'.\n", pool_name);
        if (errbuf) fprintf(stderr, "Error: %s\n", errbuf);
        rados_buffer_free(errbuf);
        return ret;
    }

    printf("Pool '%s' deleted successfully.\n", pool_name);
    rados_buffer_free(outbuf);

    return 0;
}
static int create_pool_with_pg_num(rados_t cluster, const char *pool_name, int pg_num) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd),
             "{\"prefix\":\"osd pool create\", \"pool\":\"%s\", \"pg_num\":%d, \"pgp_num\":%d}",
             pool_name, pg_num, pg_num);

    const char *cmd_arr[] = {cmd, NULL};
    char *outbuf = NULL, *errbuf = NULL;
    size_t outlen = 0, errlen = 0;

    int ret = rados_mon_command(cluster, cmd_arr, 1, NULL, 0, &outbuf, &outlen, &errbuf, &errlen);

    if (ret < 0) {
        if (ret == -EEXIST) {
            printf("Pool '%s' already exists.\n", pool_name);
            rados_buffer_free(errbuf);
            return 0;
        }
        fprintf(stderr, "Failed to create pool '%s' with %d PGs.\n", pool_name, pg_num);
        if (errbuf) fprintf(stderr, "Error: %s\n", errbuf);
        rados_buffer_free(errbuf);
        return ret;
    }

    printf("Pool '%s' created successfully with %d PGs.\n", pool_name, pg_num);
    rados_buffer_free(outbuf);

    return 0;
}

// creating test pool
static int create_pool(rados_ioctx_t *io_ctx, rados_t cluster) {
    printf("--- step 2: creating test pool '%s' ---\n", TEST_POOL_NAME);
    int ret = create_pool_with_pg_num(cluster, TEST_POOL_NAME, 8);  // default PG num for testing
    if (ret < 0 && ret != -EEXIST) { // -EEXIST means pool already exists, which is fine for our test
        fprintf(stderr, "failed to create pool '%s': %s\n", TEST_POOL_NAME, strerror(-ret));
        return ret;
    }
    ret = rados_ioctx_create(cluster, TEST_POOL_NAME, io_ctx);
    if (ret < 0) {
        fprintf(stderr, "failed to create IO context for pool '%s': %s\n", TEST_POOL_NAME, strerror(-ret));
        return ret;
    }
    printf("pool '%s' is ready.\n\n", TEST_POOL_NAME);
    return 0;
}

// write test data to the pool
static int write_test_data(rados_ioctx_t io_ctx) {
    printf("--- step 3: write SeaStore %d objects (each %d bytes) ---\n", NUM_OBJS, OBJ_SIZE);
    char *data = new char[OBJ_SIZE];
    if (!data) return -ENOMEM;
    memset(data, 'X', OBJ_SIZE); // fill with dummy data
    
    for (int i = 0; i < NUM_OBJS; i++) {
        char oid[128];
        snprintf(oid, sizeof(oid), "%s-%04d", TEST_OBJ_PREFIX, i);
        // using sync write to ensure data is on disk for scrub to find it
        int ret = rados_write_full(io_ctx, oid, data, OBJ_SIZE);
        if (ret < 0) {
            fprintf(stderr, "failed to write object %s: %s\n", oid, strerror(-ret));
            free(data);
            return ret;
        }
        if ((i + 1) % 10 == 0) printf("  already writen %d/%d objects...\n", i + 1, NUM_OBJS);
    }
    free(data);
    printf("data written complete! \n\n");
    return 0;
}

// get some obj's pg id
static int get_pgid(rados_ioctx_t io_ctx, const char *oid, char *pgid_buf, size_t buf_len) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "osd map %s %s", TEST_POOL_NAME, oid);
    char out[1024] = {0};
    if (exec_ceph_cmd(cmd, out, sizeof(out)) < 0) return -1;

    const char *search = "\"pgid\":\"";
    const char *start = strstr(out, search);
    if (!start) return -1;
    start += strlen(search);
    const char *end = strchr(start, '\"');
    if (!end) return -1;
    int len = end - start;
    if (len >= (int)buf_len) len = buf_len - 1;
    strncpy(pgid_buf, start, len);
    pgid_buf[len] = '\0';
    return 0;
}

// wait for scrub to complete by monitoring PG's last_scrub_stamp
static int wait_for_scrub_complete(const char *pgid, int timeout_sec) {
    printf("--- step 5: wait for scrub to complete (timeout: %d seconds) ---\n", timeout_sec);
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "tell %s query", pgid);
    char last_scrub_stamp[64] = {0};
    int change_count = 0; 
    int slept = 0;
    while (slept < timeout_sec) {
        char out[4096] = {0};
        if (exec_ceph_cmd(cmd, out, sizeof(out)) == 0) {
            const char *search = "\"last_scrub_stamp\":\"";
            const char *start = strstr(out, search);
            char current_stamp[64] = {0};
            if (start) {
                start += strlen(search);
                const char *end = strchr(start, '\"');
                if (end && strlen(start) > 0) {
                    size_t len = end - start;
                    if (len >= sizeof(current_stamp)) len = sizeof(current_stamp) - 1;
                    strncpy(current_stamp, start, len);
                    current_stamp[len] = '\0';
                }
            }
            if (strcmp(current_stamp, last_scrub_stamp) != 0) {

                if (last_scrub_stamp[0] != '\0') {
                    change_count++;
                    printf("  stamp changed to '%s' (change #%d)\n", 
                           current_stamp[0] ? current_stamp : "(none)", change_count);
                } else if (current_stamp[0] != '\0') {
                    change_count++;
                    printf("  stamp appeared: '%s' (change #%d)\n", current_stamp, change_count);
                }
                strcpy(last_scrub_stamp, current_stamp);
                
                if (change_count >= 5) {
                    printf("  Scrub completed (observed 3 stamp changes).\n\n");
                    return 0;
                }
            }
        } else {
              fprintf(stderr, "  Warning: failed to execute 'ceph %s'\n", cmd);
        }

        sleep(5);
        slept += 5;
        printf("  waiting... (%dseconds)\n", slept);
    }
    fprintf(stderr, "waiting for scrub to complete timed out.\n");
    return -1;
}

// read and verify all objects to check if scrub preserved data integrity
static int verify_test_data(rados_ioctx_t io_ctx) {
    printf("--- step 6: verify all written data ---\n");
    char *expected_data = new char[OBJ_SIZE];
    if (!expected_data) return -ENOMEM;
    memset(expected_data, 'X', OBJ_SIZE);
    char *read_buf = new char[OBJ_SIZE];
    if (!read_buf) {
        free(expected_data);
        return -ENOMEM;
    }
    
    int error_count = 0;
    for (int i = 0; i < NUM_OBJS; i++) {
        char oid[128];
        snprintf(oid, sizeof(oid), "%s-%04d", TEST_OBJ_PREFIX, i);
        int ret = rados_read(io_ctx, oid, read_buf, OBJ_SIZE, 0);
        if (ret != OBJ_SIZE) {
            fprintf(stderr, "read obj %s failed or length mismatch (ret=%d)\n", oid, ret);
            error_count++;
            continue;
        }
        if (memcmp(read_buf, expected_data, OBJ_SIZE) != 0) {
            fprintf(stderr, "object %s data corrupted!\n", oid);
            error_count++;
        }
        if ((i + 1) % 10 == 0) printf("  already verify %d/%d objects...\n", i+1, NUM_OBJS);
    }
    free(expected_data);
    free(read_buf);
    
    if (error_count == 0) {
        printf("All data verification passed! Scrub test successful.\n\n");
        return 0;
    } else {
        printf("Data verification failed, %d objects have issues.\n", error_count);
        return -1;
    }
}

static int finalcleanup(rados_t cluster) {
    printf("--- cleanup: remove test pool '%s' ---\n", TEST_POOL_NAME);
    int r1 = remove_scrub_settings_global(cluster);
    int r2 = remove_pool(cluster, TEST_POOL_NAME);
    return r1 || r2;
}

int main(int argc, char const *argv[]) {
    rados_t cluster;
    rados_ioctx_t io_ctx = NULL;
    int ret;
    const char *sample_oid = "scrub-test-obj-0000";

    printf("\n========== Crimson OSD + SeaStore Scrub  ==========\n\n");
    
    // connect to cluster
    ret = rados_create(&cluster, NULL);
    if (ret < 0) {
        fprintf(stderr, "can't create cluster handle: %s\n", strerror(-ret));
        return EXIT_FAILURE;
    }
    // where the teuthology save ceph.conf?
    ret = rados_conf_read_file(cluster, "./ceph.conf");
    if (ret < 0) {
        fprintf(stderr, "failed to read config file: %s\n", strerror(-ret));
        rados_shutdown(cluster);
        return EXIT_FAILURE;
    }
    ret = rados_connect(cluster);
    if (ret < 0) {
        fprintf(stderr, "failed to connect to cluster: %s\n", strerror(-ret));
        rados_shutdown(cluster);
        return EXIT_FAILURE;
    }
    printf("connected to cluster\n\n");
    
    // config scrub parameters
    if (config_scrub_settings_global(cluster) < 0) goto cleanup;
    
    // create pool
    if (create_pool(&io_ctx, cluster) < 0) goto cleanup;
    
    // write test data
    if (write_test_data(io_ctx) < 0) goto cleanup;
    
    // get some obj's PG ID，checking Scrub scheduling
    char pgid[128];
    if (get_pgid(io_ctx, sample_oid, pgid, sizeof(pgid)) < 0) {
        fprintf(stderr, "can't get PG ID。\n");
        goto cleanup;
    }
    printf("object  %s located at PG: %s\n\n", sample_oid, pgid);
    
    sleep(2); // give the cluster some time to start processing
    
    // wait for Scrub to complete
    if (wait_for_scrub_complete(pgid, 300) < 0) goto cleanup;
    
    //verify data integrity after scrub
    if (verify_test_data(io_ctx) < 0) goto cleanup;
    
    printf("========== test passed ==========\n");
    ret = EXIT_SUCCESS;
    goto cleanup;
    
cleanup:
    int r = finalcleanup(cluster);
    if (r < 0) {
        fprintf(stderr, "final cleanup failed.\n");
        if (ret == EXIT_SUCCESS) ret = EXIT_FAILURE;
    }    
    if (io_ctx) rados_ioctx_destroy(io_ctx);
    rados_shutdown(cluster);
    return ret;
}
