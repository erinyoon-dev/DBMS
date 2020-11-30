#ifndef PROJECT_5_TRX_MANAGER_H
#define PROJECT_5_TRX_MANAGER_H
#include "lock_manager.h"

enum trx_state {NONE, RUNNING, WAITING};

// for rollback when the trx is ABORTED
struct log {
    int table_id;
    int64_t key;
    char value[VALUE_SIZE];
    pagenum_t pagenum;
    int key_index;
};
typedef struct log undo_log;

struct trx {
    int trx_id;
    trx_state state;
    lock_t* lock_list;
};
typedef struct trx trx_t;

extern map<int, trx*> trx_manager;

/* APIs for Transaction */
trx_t* make_trx();
trx_t* get_trx(int trx_id);
void set_trx_state(int trx_id, trx_state state);
int trx_isEmpty(int trx_id);
void trx_insert(int trx_id, lock_t* lock);
//bool isConflict(lock_t* lock, lock_t* new_lock);
int trx_isDeadlock(int trx_id, lock_t *lock);
int trx_rollback(int trx_id, int table_id, int64_t key, int key_index, char* ret_val);
int trx_abort(int trx_id);
int trx_begin(void);
int trx_commit(int trx_id);

#endif //PROJECT_5_TRX_MANAGER_H
