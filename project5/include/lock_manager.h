#ifndef LOCK_MANAGER_H
#define LOCK_MANAGER_H

#include "buffer_layer.h"
#include <stdint.h>
#include <limits.h>
#include <pthread.h>
#include <map>
#include <list>

#define SHARED 0
#define EXCLUSIVE 1
using namespace std;

typedef struct hashEntry hash_entry;
typedef struct lock_t lock_t;

struct lock_t {
    lock_t *prev;
    lock_t *next;
    hash_entry *hashEntry;
    pthread_cond_t cond;
    int lock_mode;
    lock_t *trx_next_lock;
    int owner_trx_id;
};

struct hashEntry
{
    int table_id;
    int64_t key;
    lock_t *head;
    lock_t *tail;
};

/* APIs for lock table */
lock_t* make_lock();
int init_lock_table();
lock_t* lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode);
int lock_release(lock_t* lock_obj);

#endif //LOCK_MANAGER_H
