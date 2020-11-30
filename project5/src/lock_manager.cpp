//
// Created by 윤세령 on 2020/11/19.
//
#include "lock_manager.h"
#include "trx_manager.h"

map<pair<int, int64_t>, hash_entry> hash_table;
pthread_mutex_t lock_table_latch;
map<int, trx*> trx_manager;

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
    lock_t * new_lock = make_lock();
    // printf("TRX %d:: lock acquire start\n", trx_id);

    // Case 1: if there are no lock manager and trx manager,
    //         initialize and append it.
    if (hash_table.find(make_pair(table_id, key)) == hash_table.end())
    {
        hashEntry entry;
        entry.table_id = table_id;
        entry.key = key;
        entry.head = new_lock;
        entry.tail = new_lock;
        hash_table[make_pair(table_id, key)] = entry;
        new_lock->hashEntry = &hash_table[make_pair(table_id, key)];

        if (!trx_isEmpty(trx_id)) {
            trx_insert(trx_id, new_lock);
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
//        set_trx_state(trx_id, RUNNING);

        pthread_mutex_unlock(&lock_table_latch);
        // printf("TRX %d:: lock acquire success\n", trx_id);
        return new_lock;
    }

    if (trx_isDeadlock(trx_id, new_lock)) {
        // printf("TRX %d:: DEADLOCK detected\n", trx_id);
        return NULL;
    }

    // append new lock to lock list and trx list
    new_lock->hashEntry = &hash_table.find(make_pair(table_id, key))->second;
    new_lock->hashEntry->tail->next = new_lock;
    new_lock->prev = new_lock->hashEntry->tail;
    new_lock->hashEntry->tail = new_lock;
    new_lock->next = NULL;
    new_lock->lock_mode = lock_mode;
    new_lock->trx_next_lock = NULL;
    new_lock->owner_trx_id = trx_id;

    // Case 2: if new lock is the record's first lock,
    //         insert into trx list and acquire
    if (new_lock->hashEntry->head == new_lock)
    {
//        set_trx_state(trx_id, RUNNING);
        if (!trx_isEmpty(trx_id)) {
            trx_insert(trx_id, new_lock);
        }
        else {
            lock_t *tmp = trx_manager[trx_id]->lock_list;
            while (tmp->trx_next_lock != NULL) {
                tmp = tmp->trx_next_lock;
            }
            tmp->trx_next_lock = new_lock;
        }
        pthread_mutex_unlock(&lock_table_latch);
        return new_lock;
    }

    // Case 3: check lock and new_lock's mode
    lock_t *lock = new_lock->prev;
    // 3-1: if lock's mode is stronger than new_lock's mode,
    //      delete new_lock from lock list
    //      and return original lock.
    if (!check_lock_mode(lock, new_lock)) {
        // if lock is X mode and has different trx id,
        // any other trx cannot access this record until lock's trx is committed.
        if (lock->lock_mode == EXCLUSIVE && lock->owner_trx_id != trx_id) {
//            set_trx_state(trx_id, WAITING);
            pthread_cond_wait(&new_lock->cond, &lock_table_latch);
//            set_trx_state(trx_id, RUNNING);
            pthread_mutex_unlock(&lock_table_latch);
            return new_lock;
        }
        lock->hashEntry->tail = lock->hashEntry->tail->prev;
        lock->hashEntry->tail->next = NULL;
        new_lock->prev = NULL;
        pthread_mutex_unlock(&lock_table_latch);
        return lock;
    }
    // 3-2: if new_lock's mode is stronger than lock's mode,
    //      lock upgrade(S->X) and append new lock to trx list
    else {
        if (!trx_isEmpty(trx_id)) {
            trx_insert(trx_id, new_lock);
        }
        else {
            lock_t *tmp = trx_manager[trx_id]->lock_list;
            while (tmp->trx_next_lock != NULL) {
                tmp = tmp->trx_next_lock;
            }
            tmp->trx_next_lock = new_lock;
        }
        // if lock has different trx id, sleep
        if (lock->owner_trx_id != trx_id) {
            if (trx_isDeadlock(trx_id, new_lock)) {
                // printf("TRX %d:: DEADLOCK detected\n", trx_id);
                return NULL;
            }
//            set_trx_state(trx_id, WAITING);
            // printf("TRX %d:: sleeping\n", trx_id);
            // sleep until previous trx is committed.
            pthread_cond_wait(&new_lock->cond, &lock_table_latch);
            // printf("TRX %d:: wake up\n", trx_id);
//            set_trx_state(trx_id, RUNNING);
            pthread_mutex_unlock(&lock_table_latch);
        }
        pthread_mutex_unlock(&lock_table_latch);
        return new_lock;
    }
}

// Remove the lock_obj from the lock list.
// 1) If there is a successor's lock waiting for the thread releasing the lock,
//    wake up the successor.
// if success, return 0.
// otherwise, return -1.
int lock_release(lock_t* lock_obj) {

    pthread_mutex_lock(&lock_table_latch);
    if (lock_obj == NULL) return -1;

    // Case 1: if hash entry doesn't have any lock obj, erase entry from lock manager
    if (lock_obj->next == NULL && lock_obj->prev == NULL) {
        hash_table.erase(make_pair(lock_obj->hashEntry->table_id, lock_obj->hashEntry->key));
    }

    else if (lock_obj->next != NULL) {
        // wake up all S locks behind lock_obj
        if (lock_obj->lock_mode == EXCLUSIVE) {
            lock_t *tmp = lock_obj->next;
            while (tmp->lock_mode == SHARED) {
                pthread_cond_signal(&tmp->cond);
                tmp = tmp->next;
            }
        }
        else {
            if (lock_obj->next->lock_mode == EXCLUSIVE) {
                lock_t *tmp = lock_obj->next;
                while (tmp->lock_mode == SHARED) {
                    pthread_cond_signal(&tmp->cond);
                    tmp = tmp->next;
                }
            }
        }

        if (lock_obj->hashEntry->head == lock_obj) {
            lock_obj->hashEntry->head = lock_obj->next;
            lock_obj->hashEntry->head->prev = NULL;
        }
        else {
            lock_obj->prev->next = lock_obj->next;
            lock_obj->next->prev = lock_obj->prev;
        }
    }
    // else: there are predecessor lock, wait

    free(lock_obj);
    pthread_mutex_unlock(&lock_table_latch);
    return 0;
}