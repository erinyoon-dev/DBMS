#include "log_manager.h"

// log buffer
// write log records every 10MB
int log_write() {

}

// log operations for db functions
// Type 0: BEGIN
int act_begin(int trx_id, int64_t lsn) {

    // append trx_id to active trx's map
    active_trx_last_lsn[trx_id] = lsn;

    // make new log record and set values
    log_record new_log;
    new_log.general.lsn = lsn;
    new_log.general.type = BEGIN;
    new_log.general.trx_id = trx_id;
    new_log.general.prev_lsn = NULL;

    // append new log record to Log Buffer
    log_buffer.push_back(new_log);
    return 0;

}

// Type 1: UPDATE
int act_update(pagenum_t pagenum, int trx_id, int64_t lsn) {

    // access to the buffer for update page => todo: how to access buffer layer?

    // make new log record and set values
    log_record new_log;
    new_log.update.lsn = lsn;
    new_log.update.type = UPDATE;
    new_log.update.trx_id = trx_id;
    new_log.update.pagenum = pagenum;

    // save old & new image to log => todo: make a structure that save values size of 120 bytes
    new_log.update.old_image = ;
    new_log.update.new_image = ;
    new_log.update.prev_lsn = active_trx_last_lsn.find(trx_id)->second;
    // update last LSN
    active_trx_last_lsn[trx_id] = lsn;

    // append new log record to Log Buffer
    log_buffer.push_back(new_log);

    return 0;

}

// Type 2: COMMIT
int act_commit(int trx_id, int64_t lsn) {

    // make new log record and set values
    log_record new_log;
    new_log.general.lsn = lsn;
    new_log.general.type = COMMIT;
    new_log.general.trx_id = trx_id;
    new_log.general.prev_lsn = active_trx_last_lsn.find(trx_id)->second;

    // append new log record to Log Buffer
    log_buffer.push_back(new_log);

    // remove trx from active trx list
    active_trx_last_lsn.erase(trx_id);

    // force
    // write all contents of Log Buffer to Stable Log
    // make Log Buffer to empty state

    return 0;
}

// Type 3: ROLLBACK
int act_rollback(int trx_id, int64_t lsn) {

}

// Type 4: COMPENSATE
int act_compensate() {

}

// recovery parts
// ANALYSIS PASS determines
// 1) next redo LSN
// 2) list of trx(that be rolled back) in undo pass
// 3) trx's last LSN
int analysis_pass(int flag, int log_num, char* log_path, char* logmsg_path) {

    // open stable log
    FILE* log_fp = fopen(log_path, "a+");
    // open log message file
    FILE* fp = fopen(logmsg_path, "a+");
    fprintf(fp, "[ANALYSIS] Analysis pass start\n");

    // scan all log records to find dirty pages and trx's status


    fprintf(fp, "[ANALYSIS] Analysis success. Winner: %d %d .., Loser: %d %d ....\n", winners, losers);
}

// REDO PASS
// restore DB to its state as the time of the system failure
int redo_pass(int flag, int log_num, char* log_path, char* logmsg_path) {

    // open stable log
    FILE* log_fp = fopen(log_path, "a+");
    // open log message file
    FILE* fp = fopen(logmsg_path, "a+");
    fprintf(fp, "[REDO] Redo pass start\n");

    // Type 0: BEGIN
    fprintf(fp, "LSN %lu [BEGIN] Transaction id %d\n", lsn, trx_id);
    // Type 1: UPDATE
    fprintf(fp, "LSN %lu [UPDATE] Transaction id %d redo apply\n", lsn, trx_id);
    // Type 2: COMMIT
    fprintf(fp, "LSN %lu [COMMIT] Transaction id %d\n", lsn, trx_id);
    // Type 3: ROLLBACK
    fprintf(fp, "LSN %lu [ROLLBACK] Transaction id %d\n", lsn, trx_id);
    // Type 4: COMPENSATE
    fprintf(fp, "LSN %lu [CLR] next undo lsn %lu\n", lsn, next_undo_lsn);

    // Consider-Redo
    fprintf(fp, "LSN %lu [CONSIDER-REDO] Transaction id %d\n", lsn, trx_id);
    // Consider-Undo
    fprintf(fp, "LSN %lu [CONSIDER-UNDO] Transaction id %d\n", lsn, trx_id);

    fprintf(fp, "[REDO] Redo pass end\n");
}

// UNDO PASS
// loser trx is rolled back reverse
int undo_pass(int flag, int log_num, char* log_path, char* logmsg_path) {

    // open stable log
    FILE* log_fp = fopen(log_path, "a+");
    // open log message file
    FILE* fp = fopen(logmsg_path, "a+");
    fprintf(fp, "[UNDO] Undo pass start\n");

    // update next undo LSN to make it max
    // if non-CLR(~CLR) is in the trx during undo pass,

    // Type 0: BEGIN
    fprintf(fp, "LSN %lu [BEGIN] Transaction id %d\n", lsn, trx_id);
    // Type 1: UPDATE
    fprintf(fp, "LSN %lu [UPDATE] Transaction id %d undo apply\n", lsn, trx_id);
    // Type 2: COMMIT
    fprintf(fp, "LSN %lu [COMMIT] Transaction id %d\n", lsn, trx_id);
    // Type 3: ROLLBACK
    fprintf(fp, "LSN %lu [ROLLBACK] Transaction id %d\n", lsn, trx_id);
    // Type 4: COMPENSATE
    fprintf(fp, "LSN %lu [CLR] next undo lsn %lu\n", lsn, next_undo_lsn);

    // Consider-Redo
    fprintf(fp, "LSN %lu [CONSIDER-REDO] Transaction id %d\n", lsn, trx_id);
    // Consider-Undo
    fprintf(fp, "LSN %lu [CONSIDER-UNDO] Transaction id %d\n", lsn, trx_id);

    fprintf(fp, "[UNDO] UNdo pass end\n");
}