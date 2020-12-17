#ifndef PRO_6_TRX_MANAGER_H
#define PRO_6_TRX_MANAGER_H

#include "lock_manager.h"
#include <stdint.h>
#include <map>

using namespace std;

enum trx_state {NONE, RUNNING, WAITING};

struct trx {
    int trx_id;
    trx_state state;
    int64_t last_lsn; // the latest LSN issued by the trx
    lock_t* lock_list;
    lock_t* wait_lock;
    bool isAborted;
};
typedef struct trx trx_t;
map<int, trx*> trx_manager;
map<int, int64_t> active_trx_last_lsn; // map for last LSN with running transactions

/* APIs for Transaction */
trx_t* make_trx();
trx_t* get_trx(int trx_id);
void set_trx_state(int trx_id, trx_state state);
bool isConflict(lock_t* lock, lock_t* new_lock);
int trx_isDeadlock(int trx_id, lock_t *lock);
int trx_rollback(int trx_id, int table_id, int64_t key, int key_index, char* ret_val);
int trx_abort(int trx_id);
int trx_begin(void);
int trx_commit(int trx_id);

#endif //PRO_6_TRX_MANAGER_H
