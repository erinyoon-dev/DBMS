/*
 * db.c
 * FILE* I/O
 */
#define _CRT_SECURE_NO_WARNINGS

#include "../include/db.h"

void usage_1 (void) {
    printf ("Enter any of the following commands after the prompt > :\n"
            "\ts <n>         -- Start DB with buffer pool of size <n>.\n"
            "\to <p>         -- Open table which is in pathname <p>. Max table number is 10.\n"
            "\tc <t>         -- Close table which has table id <t>.\n"
            "\ti <t> <k> <v> -- Insert <k> as key and <v> as value in table <t>.\n"
            "\tj <t>         -- Auto Insert 1-999 as key in table <t>.\n"
            "\tf <t> <k>     -- Find the value under key <k> and print value.\n"
            "\td <t> <k>     -- Delete key <k> and its associated value in table <t>.\n"
            "\tb <t> 		 -- Auto Delete odd key in range 1-999 from table <t>.\n"
            "\tp <t> <p>     -- Shows page which has page number <p>.\n"
            "\tq             -- Shutdown DB\n"
            "\t?             -- Print this help message.\n");
}

void print_page(int table_id, pagenum_t pagenum){
    buf_print_page(table_id, pagenum);
}

// Initialize buffer pool with given number and buffer manager.
int init_db (int num_buf) {

    init_index(num_buf);
}

// Destroy frame manager.
int shutdown_db() {
    return shutdown_buf();
}

int open_table(char* pathname) {
    return open_idx_table(pathname);
}

int close_table(int table_id) {
    return close_idx_table(table_id);
}

int db_insert(int table_id, int64_t key, char * value) {
    if (!index_check_table(table_id)) {
        printf("table id %d is not opened\n", table_id);
        return -1;
    }
    return idx_insert(table_id, key, value);
}

int db_find(int table_id, int64_t key, char* ret_val) {
    if (!index_check_table(table_id)) {
        printf("table id %d is not opened\n", table_id);
        return -1;
    }
    return idx_find(table_id, key, ret_val);
}

int db_delete(int table_id, int64_t key) {
    if (!index_check_table(table_id)) {
        printf("table id %d is not opened\n", table_id);
        return -1;
    }
    return idx_delete(table_id, key);
}
