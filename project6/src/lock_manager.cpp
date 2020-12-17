//
// Created by 윤세령 on 2020/11/19.
//
#include "lock_manager.h"
#include "trx_manager.h"
#include "index_layer.h"

static map<pair<int, int64_t>, hash_entry> hash_table;


static pthread_mutex_t lock_table_latch;

lock_t* make_lock() {

    lock_t* lock = (lock_t *)malloc(sizeof(lock_t));
    lock->prev = NULL;
    lock->next = NULL;
    lock->cond = (pthread_cond_t)PTHREAD_COND_INITIALIZER;
    lock->lock_mode = 0;
    lock->trx_next_lock = NULL;
    lock->owner_trx_id = 0;

    return lock;
}

int check_lock_mode(lock_t* lock, lock_t* new_lock) {
    if (lock->lock_mode >= new_lock->lock_mode) {
        return 0;
    }
    else {
        return 1;
    }
}

// lock_acquire()에서 호출해서 현재 얻고자 하는 레코드 락이 deadlock을 유발하는지 검사
int trx_isDeadlock(int trx_id, lock_t *lock) {

    lock_t *tmp_lock = lock->prev;
    // deadlock is detected when trx is waiting itself.
    // so check trx's wait lock list to detect deadlock.
    if (tmp_lock == NULL)
        return false;

    while (tmp_lock != NULL) {
        if (tmp_lock->owner_trx_id == trx_id)
            return true;
        tmp_lock = tmp_lock->prev;
    }
    return false;

}

// Initialize any data structures required for implementing lock table.
// if success, return 0.
int init_lock_table() {

    lock_table_latch = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
    return 0;

}

// Allocate and append a new lock object to the lock list of the record having the key.
// 1) If there is a predecessor’s conflicting lock object in the lock list,
//    sleep until the predecessor to release its lock.
// 2) If there is no predecessor’s conflicting lock object, return the address of the new lock object.
// lock_mode: 0 (SHARED) or 1 (EXCLUSIVE)
lock_t* lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode) {

    pthread_mutex_lock(&lock_table_latch);
    lock_t *new_lock = make_lock();

    printf("TRX %d:: lock acquire start\n", trx_id);
    // Case 1: if there are no lock manager and trx manager, initialize and append it.
    if (hash_table.find(make_pair(table_id, key)) == hash_table.end()) {
        hashEntry entry;
        entry.table_id = table_id;
        entry.key = key;
        entry.head = new_lock;
        entry.tail = new_lock;
        hash_table[make_pair(table_id, key)] = entry;
        new_lock->hashEntry = &hash_table[make_pair(table_id, key)];

        if (trx_manager[trx_id]->lock_list == NULL) {
            trx_manager[trx_id]->lock_list = new_lock;
        }
        else {
            lock_t *tmp = trx_manager[trx_id]->lock_list;
            while (tmp->trx_next_lock != NULL) {
                tmp = tmp->trx_next_lock;
            }
            tmp->trx_next_lock = new_lock;
        }
        new_lock->lock_mode = lock_mode;
        new_lock->trx_next_lock = NULL;
        new_lock->owner_trx_id = trx_id;

        pthread_mutex_unlock(&lock_table_latch);
        printf("TRX %d:: lock acquire success\n", trx_id);
        return new_lock;
    }

    // append new lock to lock list
    new_lock->hashEntry = &hash_table.find(make_pair(table_id, key))->second;
    new_lock->hashEntry->tail->next = new_lock;
    new_lock->prev = new_lock->hashEntry->tail;
    new_lock->hashEntry->tail = new_lock;
    new_lock->next = NULL;
    new_lock->owner_trx_id = trx_id;
    new_lock->lock_mode = lock_mode;
    // append new lock to trx list
    if (trx_manager[trx_id]->lock_list == NULL) {
        trx_manager[trx_id]->lock_list = new_lock;
    }
    else {
        lock_t *tmp = trx_manager[trx_id]->lock_list;
        while (tmp->trx_next_lock != NULL) {
            tmp = tmp->trx_next_lock;
        }
        tmp->trx_next_lock = new_lock;
    }

    lock_t *lock = new_lock->prev;

    // Case 2: no predecessor - return new lock's address
    if (lock == NULL) {
        pthread_mutex_unlock(&lock_table_latch);
        printf("TRX %d:: lock acquire success\n", trx_id);
        return new_lock;
    }

    // if old lock's mode is stronger than new lock's mode,
    // disconnect new lock from lock list
    // and do not acquire
    if (!check_lock_mode(lock, new_lock)) {
        if (lock->owner_trx_id == trx_id) {
            lock->hashEntry->tail = lock->hashEntry->tail->prev;
            lock->hashEntry->tail->next = NULL;
            new_lock->prev = NULL;
            return lock;
        }
        else {
        }

    }

    // Case 3: 해당 trx에서 conflict가 발생하는지? 발생한다면 직전 lock때문인지 전의 락 때문인지?
    if (isConflict(new_lock->prev, new_lock)) {
        if (trx_isDeadlock(trx_id, new_lock)) {
            trx_abort(trx_id);
            return NULL;
        }
        else {
            printf("TRX %d:: sleeping\n", trx_id);
            set_trx_state(trx_id, WAITING);
            pthread_cond_wait(&new_lock->cond, &lock_table_latch);
            set_trx_state(trx_id,RUNNING);
            pthread_mutex_unlock(&lock_table_latch);
            printf("TRX %d:: lock acquire success\n", trx_id);
            return new_lock;
        }
    }

    // Case 3: trx has already acquired at same record
    while (lock->prev != NULL) {
        if (lock->owner_trx_id == trx_id) {
            // 3-1: same trx, same mode - don't have to link. acquire
            if (lock->lock_mode == lock_mode) {
                lock->hashEntry->tail = lock->hashEntry->tail->prev;
                lock->hashEntry->tail->next = NULL;
//                new_lock->trx_next_lock = NULL;
                new_lock->prev = NULL;
                free(new_lock);
                pthread_mutex_unlock(&lock_table_latch);
                printf("TRX %d:: old lock acquired\n", trx_id);
                return lock;
            }
            // 3-2: same trx, different mode
            else {
                // 1) X-S: OK
                if (lock->lock_mode == EXCLUSIVE && new_lock->lock_mode == SHARED) {
                    lock->hashEntry->tail = lock->hashEntry->tail->prev;
                    lock->hashEntry->tail->next = NULL;
//                    new_lock->trx_next_lock = NULL;
                    new_lock->prev = NULL;
                    free(new_lock);
                    pthread_mutex_unlock(&lock_table_latch);
                    printf("TRX %d:: old lock acquired\n", trx_id);
                    return lock;
                }
                // 2) S - ... - X: Aborted but this(S) lock can be upgraded
                else {
                    // sleep until other trx's lock between lock and new_lock is released
                    printf("TRX %d:: sleeping\n", trx_id);
                    set_trx_state(trx_id, WAITING);
                    pthread_cond_wait(&new_lock->cond, &lock_table_latch);
//                    set_trx_waitLock(trx_id);
                    set_trx_state(trx_id, RUNNING);
                    pthread_mutex_unlock(&lock_table_latch);
//                    printf("TRX %d:: lock acquire success\n", trx_id);
                    return new_lock;
                }
            }
        }
        lock = lock->prev;
    }
    // Case 4: trx has predecessors by different trx
    if (lock->prev == NULL) {
        if (isConflict(new_lock->prev, new_lock)) {
            if (trx_isDeadlock(trx_id, new_lock)) {
                trx_abort(trx_id);
                return NULL;
            }
            // Conflict - sleep new lock's trx
            else {
                printf("TRX %d:: sleeping\n", trx_id);
                set_trx_state(trx_id, WAITING);
                pthread_cond_wait(&new_lock->cond, &lock_table_latch);
                set_trx_state(trx_id,RUNNING);
                pthread_mutex_unlock(&lock_table_latch);
                printf("TRX %d:: lock acquire success\n", trx_id);
                return new_lock;
            }
        }
        // not conflict
        else {
            set_trx_state(trx_id, RUNNING);
            pthread_mutex_unlock(&lock_table_latch);
            printf("TRX %d:: lock acquire success\n", trx_id);
            return new_lock;
        }
    }
}

// Remove the lock_obj from the lock list.
// 1) If there is a successor's lock waiting for the thread releasing the lock,
//    wake up the sucessor.
// if success, return 0.
// otherwise, return -1.
int lock_release(lock_t* lock_obj) {

    pthread_mutex_lock(&lock_table_latch);
    if (lock_obj == NULL) return -1;

    // Case 1: if hash entry doesn't have any lock obj, erase entry from lock manager
    if (lock_obj->next == NULL) {
        hash_table.erase(make_pair(lock_obj->hashEntry->table_id, lock_obj->hashEntry->key));
    }

    // Count a number of the record's RUNNING trx
    lock_t *tmp_lock = lock_obj->hashEntry->head;
    int cnt = 0;
    while (tmp_lock->next != NULL) {
        if (trx_manager[tmp_lock->owner_trx_id]->state == RUNNING)
            cnt++;
        tmp_lock = tmp_lock->next;
    }
    // upgrade from S to X
    if (cnt == 1) {
        if (lock_obj->owner_trx_id == lock_obj->next->owner_trx_id && lock_obj->next->lock_mode == EXCLUSIVE) {
            pthread_cond_signal(&lock_obj->next->cond);
            lock_obj->hashEntry->head = lock_obj->next;
            lock_obj->hashEntry->head->prev = NULL;
            free(lock_obj);
            pthread_mutex_unlock(&lock_table_latch);
            return 0;
        }
    }
    // if RUNNING trx is more than 1, check mode and wait
    else {
        // Case 2-1: if released lock is head
        if (lock_obj->hashEntry->head == lock_obj) {
            // wake up all S locks behind lock_obj
            if (lock_obj->lock_mode == EXCLUSIVE) {
                lock_t *tmp = lock_obj->next;
                while (tmp->lock_mode == SHARED) {
                    pthread_cond_signal(&tmp->cond);
                    tmp = tmp->next;
                }
            }
            lock_obj->hashEntry->head = lock_obj->next;
            lock_obj->hashEntry->head->prev = NULL;
        }
        // Case 2-2: there are predecessor lock, wait
    }
    free(lock_obj);
    pthread_mutex_unlock(&lock_table_latch);
    return 0;
}