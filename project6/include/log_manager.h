#ifndef PRO_6_LOG_MANAGER_H
#define PRO_6_LOG_MANAGER_H

#include "disk_space_layer.h"
#include "trx_manager.h"

#include <cstdio>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <fcntl.h>

enum record_type {BEGIN, UPDATE, COMMIT, ROLLBACK, COMPENSATE};

typedef struct general_log_record {
    int64_t lsn;
    int64_t prev_lsn;
    int trx_id;
    record_type type;
}general_log_record;

typedef struct update_log_record {
    int64_t lsn;
    int64_t prev_lsn;
    int trx_id;
    record_type type;
    int table_id;
    pagenum_t pagenum;
    int offset;
    int length;
    char old_image[VALUE_SIZE];
    char new_image[VALUE_SIZE];
}update_log_record;

typedef struct compensate_log_record {
    int64_t lsn;
    int64_t prev_lsn;
    int trx_id;
    record_type type;
    int table_id;
    pagenum_t pagenum;
    int offset;
    int length;
    char old_image[VALUE_SIZE];
    char new_image[VALUE_SIZE];
    int64_t next_undo_lsn;
}compensate_log_record;

typedef struct log_record {
    union {
        general_log_record general;
        update_log_record update;
        compensate_log_record comp;
    };
}log_record;

// Log Buffer
list<log_record> log_buffer;

// APIs for log functions in layered architecture
int act_begin(int trx_id, int64_t lsn);
int act_update(pagenum_t pagenum, int trx_id, int64_t lsn);
int act_commit(int trx_id, int64_t lsn);
int act_rollback();
int act_compensate();
int analysis_pass(int flag, int log_num, char* log_path, char* logmsg_path);
int redo_pass(int flag, int log_num, char* log_path, char* logmsg_path);
int undo_pass(int flag, int log_num, char* log_path, char* logmsg_path);

#endif //PRO_6_LOG_MANAGER_H
