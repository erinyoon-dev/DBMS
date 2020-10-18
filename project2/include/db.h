#ifndef DB_MANAGEMENT
#define DB_MANAGEMENT

#include "bpt.h"

/* ================
 * Debuging
 * ================
 */
void print_page (pagenum_t pagenum);

/* ================
 * API services
 * ================
 */
int open_table(char * pathname);
int db_insert(int64_t key, char * value);
int db_find(int64_t key, char * ret_val);
int db_delete(int64_t key);

#endif /* db.h */