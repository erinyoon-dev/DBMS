//
// Created by 윤세령 on 2020/11/04.
//
#ifndef PROJ3_DB_H
#define PROJ3_DB_H

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
int db_find(int table_id, int64_t key, char* ret_val);
int db_delete(int table_id, int64_t key);


#endif //PROJ3_DB_H
