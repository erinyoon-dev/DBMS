/*
 * Project 4: Lock_table
 * created by seryoung Yoon
 * last modified 14 Oct 2020
 */
#include <lock_table.h>
#include <map>

using namespace std;

struct hashEntry
{
    int table_id;
    int64_t key;
    lock_t *head;
    lock_t *tail;
};
typedef struct hashEntry hash_entry;

struct lock_t
{
    lock_t *prev;
    lock_t *next;
    hash_entry *hashEntry;
    pthread_cond_t cond;
};
typedef struct lock_t lock_t;

map<pair<int, int64_t>, hash_entry> hash_table;
pthread_mutex_t lock_table_latch;

lock_t* make_lock()
{
    lock_t* lock = (lock_t *)malloc(sizeof(lock_t));
    lock->prev = NULL;
    lock->next = NULL;
    lock->cond = PTHREAD_COND_INITIALIZER;

    return lock;
}

// Initialize any data structures required for implementing lock table.
// if success, return 0.
int init_lock_table()
{
    lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
    return 0;
}

// Allocate and append a new lock object to the lock list of the record having the key.
// 1) If there is a predecessor's lock obj in the lock list,
//    sleep until the predecessor to release its lock.
// 2) If there is no predecessor's lock obj, return new lock obj's address.
lock_t* lock_acquire(int table_id, int64_t key)
{
    pthread_mutex_lock(&lock_table_latch);
    lock_t * new_lock = make_lock();
    if (hash_table.find(make_pair(table_id, key)) == hash_table.end())
    {
        hashEntry entry;
        entry.table_id = table_id;
        entry.key = key;
        entry.head = new_lock;
        entry.tail = new_lock;
        hash_table[make_pair(table_id, key)] = entry;
        new_lock->hashEntry = &hash_table[make_pair(table_id, key)];
        pthread_mutex_unlock(&lock_table_latch);
        return new_lock;
    }
    new_lock->hashEntry = &hash_table.find(make_pair(table_id, key))->second;
    new_lock->hashEntry->tail->next = new_lock;
    new_lock->prev = new_lock->hashEntry->tail;
    new_lock->hashEntry->tail = new_lock;
    new_lock->next = NULL;
    if (new_lock->hashEntry->head == new_lock)
    {
        pthread_mutex_unlock(&lock_table_latch);
        return new_lock;
    }
    // sleep
    pthread_cond_wait(&new_lock->cond, &lock_table_latch);
    pthread_mutex_unlock(&lock_table_latch);
    return new_lock;
}

// Remove the lock_obj from the lock list.
// 1) If there is a successor's lock waiting for the thread releasing the lock,
//    wake up the sucessor.
// if success, return 0.
// otherwise, return -1.
int lock_release(lock_t* lock_obj)
{
	pthread_mutex_lock(&lock_table_latch);
	if (lock_obj == NULL) return -1;
    if (lock_obj->next == NULL)
    {
        hash_table.erase(make_pair(lock_obj->hashEntry->table_id, lock_obj->hashEntry->key));
    }
    else
    {
        pthread_cond_signal(&(lock_obj->next->cond));
        lock_obj->hashEntry->head = lock_obj->next;
        lock_obj->hashEntry->head->prev = NULL;
    }
	free(lock_obj);
	pthread_mutex_unlock(&lock_table_latch);
	return 0;
}
