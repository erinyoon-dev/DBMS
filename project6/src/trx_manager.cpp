#include "trx_manager.h"
#include "lock_manager.h"

//static map<int, trx*> trx_manager;
//static map<int, list<undo_log*> > rollback;

static pthread_mutex_t trx_manager_latch = PTHREAD_MUTEX_INITIALIZER;

static int transaction_id = 1;

trx_t* make_trx() {

    trx_t* trx = (trx_t *)malloc(sizeof(trx_t));
    trx->trx_id = transaction_id;
    trx->lock_list = NULL;
    trx->isAborted = false;
    trx->state = NONE;
    transaction_id++;
    return trx;
}

trx_t* get_trx(int trx_id) {

    pthread_mutex_lock(&trx_manager_latch);
    trx_t* trx;

    if(!trx_manager.count(trx_id))
        trx = NULL;
    else
        trx = trx_manager[trx_id];

    pthread_mutex_unlock(&trx_manager_latch);
    return trx;
}

void set_trx_state(int trx_id, trx_state state) {

    trx_t *trx = get_trx(trx_id);
    trx->state = state;
}

//void set_trx_waitLock(int trx_id) {
//
//    lock_t *tmp = trx_manager[trx_id]->wait_lock;
//    trx_manager[trx_id]->wait_lock = tmp->trx_next_lock;
//
//}

bool isConflict(lock_t* lock, lock_t* new_lock) {

    // Case 1: 직전 lock과 같은 trx가 lock acquire하려는 경우
    if (lock->owner_trx_id == new_lock->owner_trx_id) {
        // 1-1: same trx, same mode - don't have to link. acquire
        if (lock->lock_mode == SHARED && new_lock->lock_mode == SHARED)
            return false;
            // 1-2: same trx, different mode
        else
            return true;
    }
        // Case 2: different trx
    else {
        if (lock->lock_mode == EXCLUSIVE) {
            trx_manager[new_lock->owner_trx_id]->state = WAITING;
            trx_manager[new_lock->owner_trx_id]->wait_lock = lock;
            return true;
        }
            // front trx is S mode
        else {
            // and no conflict before front trx
            if (trx_manager[lock->owner_trx_id]->state == RUNNING) {
                if (new_lock->lock_mode == SHARED)
                    return false;
                else {
                    trx_manager[new_lock->owner_trx_id]->state = WAITING;
                    trx_manager[new_lock->owner_trx_id]->wait_lock = lock;
                    return true;
                }
            }
                // there is conflict before front trx
            else if (trx_manager[lock->owner_trx_id]->state == WAITING) {
                lock_t* tmp_lock = lock->prev;
                // find X lock and set it to new lock's wait list
                while (tmp_lock != NULL && trx_manager[tmp_lock->owner_trx_id]->state == WAITING)
                    tmp_lock = tmp_lock->prev;

                trx_manager[new_lock->owner_trx_id]->state = WAITING;
                trx_manager[new_lock->owner_trx_id]->wait_lock = tmp_lock;
                return true;
            }
                // trx which has NONE state predecessor lock
            else {
                return false;
            }
        }
    }
}

//// 해당 trx의 rollback list 맨 뒤에 rollback data를 추가
//int trx_rollback(int trx_id, int table_id, int64_t key, int key_index, char* ret_val) {
//
//    trx_t* trx;
//    undo_log* log;
//
//    /*creat log*/
//    log = new undo_log;
//    log->table_id = table_id;
//    log->key = key;
//    log->key_index = key_index;
//    memcpy(&log->value, ret_val, sizeof(ret_val));
//    rollback[trx_id].push_back(log);
//
//    return 0;
//}

//int trx_abort(int trx_id) {
//
//    trx_t* trx = get_trx(trx_id);
//    if (trx == NULL)
//        return -1;
//
//    lock_t *tmp_lock = trx->lock_list;
//    while (tmp_lock->trx_next_lock != NULL) {
//        undo_log* log = rollback[trx_id].back();
//        // rollback undo log
//        idx_undo(log->table_id, log->key, log->key_index, log->pagenum,log->value);
//        rollback[trx_id].pop_back();
//        tmp_lock = tmp_lock->trx_next_lock;
//    }
//    // erase trx in trx_manager
//    trx_manager.erase(trx_id);
//}

// Allocate a transaction structure and initialize it.
// Return a unique transaction id(>= 1) if success, otherwise return 0.
int trx_begin(void) {

    pthread_mutex_lock(&trx_manager_latch);
    trx_t* trx = make_trx();
    if (trx->trx_id == 0) {
        pthread_mutex_unlock(&trx_manager_latch);
        return 0;
    }
    trx_manager[trx->trx_id] = trx;
    pthread_mutex_unlock(&trx_manager_latch);
    printf("trx_id %d\n", trx->trx_id);
    return trx->trx_id;
}

// Clean up the transaction with given trx_id (transaction id)
// and its related information that has been used in your lock manager.
// Return the completed transaction id if success, otherwise return 0.
int trx_commit(int trx_id) {

    pthread_mutex_lock(&trx_manager_latch);
    // release all locks related with committed trx
    while (trx_manager[trx_id]->lock_list != NULL) {
        lock_release(trx_manager[trx_id]->lock_list);
        trx_manager[trx_id]->lock_list = trx_manager[trx_id]->lock_list->trx_next_lock;
    }
    // erase trx from trx manager
    trx_manager.erase(trx_id);
    if (trx_manager.count(trx_id)) {
        pthread_mutex_unlock(&trx_manager_latch);
        return 0;
    }
    pthread_mutex_unlock(&trx_manager_latch);
    return trx_id;

}