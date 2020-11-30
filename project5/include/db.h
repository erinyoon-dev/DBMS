//
// Created by 윤세령 on 2020/11/04.
//
#ifndef DB_H
#define DB_H

#include "index_layer.h"

/* ================
 * Debuging
 * ================
 */
void usage_1 (void);
void print_page (int table_id, pagenum_t pagenum);

/* ================
 * API services
 * ================
 */
int init_db (int num_buf);
int close_table(int table_id);
int shutdown_db();
int open_table(char * pathname);
int db_insert(int table_id, int64_t key, char * value);
int db_find(int table_id, int64_t key, char* ret_val, int trx_id);
int db_update(int table_id, int64_t key, char* values, int trx_id);
int db_delete(int table_id, int64_t key);


#endif //DB_H
