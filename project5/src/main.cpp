#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "lock_manager.h"
#include "trx_manager.h"
#include "db.h"

#define NUM_THREADS 3
#define ABORT -1
#define RECORD_NUMBER 16
#define COUNT 1
using namespace std;

void* thread1(void* args) {

    int table_id, trx_id;
    char values1[120] = "update1";
    char values2[120] = "update2";
    int64_t key1, key2;

    key1 = 1001;
    key2 = 1002;

    table_id = open_table("sample_10000.db");

    trx_id = trx_begin();
    if (!db_find(table_id, key1, values1, trx_id)) {
        printf("TRX %d Find results:: key: %lu, value: %s\n", trx_id, key1, values1);
    }
    if (!db_find(table_id, key2, values2, trx_id)) {
        printf("TRX %d Find results:: key: %lu, value: %s\n", trx_id, key2, values2);
    }
    if (db_update(table_id, key1, values1, trx_id) == ABORT) {
        printf("thread1 key1 DEADLOCK\n");
        return NULL;
    }

    if (db_update(table_id, key2, values2, trx_id) == ABORT) {
        printf("thread1 key2 DEADLOCK\n");
        return NULL;
    }

    trx_commit(trx_id);
    printf("TRX %d done\n", trx_id);
    pthread_exit(NULL);
}

void* thread2(void* args) {

    int table_id, trx_id;
    char values1[120] = "update1";
    char values2[120] = "update2";
    int64_t key1, key2;

    key1 = 1002;
    key2 = 1003;

    table_id = open_table("sample_10000.db");

    trx_id = trx_begin();
    if (!db_find(table_id, key1, values1, trx_id)) {
        printf("TRX %d Find results:: key: %lu, value: %s\n", trx_id, key1, values1);
    }
    if (!db_find(table_id, key2, values2, trx_id)) {
        printf("TRX %d Find results:: key: %lu, value: %s\n", trx_id, key2, values2);
    }
    if (db_update(table_id, key1, values1, trx_id) == ABORT) {
        printf("thread2 key1 DEADLOCK\n");
        return NULL;
    }
    if (db_update(table_id, key2, values2, trx_id) == ABORT) {
        printf("thread2 key2 DEADLOCK\n");
        return NULL;
    }

    trx_commit(trx_id);
    printf("TRX %d done\n", trx_id);
    pthread_exit(NULL);
}


void* thread3(void* args) {

    int table_id, trx_id;
    char values1[120] = "update1";
    char values2[120] = "update2";
    int64_t key1, key2;

    key1 = 1003;
    key2 = 1001;

    table_id = open_table("sample_10000.db");

    trx_id = trx_begin();
    if (!db_find(table_id, key1, values1, trx_id)) {
        printf("TRX %d Find results:: key: %lu, value: %s\n", trx_id, key1, values1);
    }
    if (!db_find(table_id, key2, values2, trx_id)) {
        printf("TRX %d Find results:: key: %lu, value: %s\n", trx_id, key2, values2);
    }
    if (db_update(table_id, key1, values1,trx_id) == ABORT) {
        printf("thread3 key1 DEADLOCK\n");
        return NULL;
    }
    if (db_update(table_id, key2, values2, trx_id) == ABORT) {
        printf("thread3 key2 DEADLOCK\n");
        return NULL;
    }

    trx_commit(trx_id);
    printf("TRX %d done\n", trx_id);
    pthread_exit(NULL);
}


//void* find_thread_func(void* arg)
//{
//    int find_key;
//    int find_table;
//    int select_num;
//    int trx_id = trx_begin();
//    for (int k = 0 ; k < COUNT ; k++){
//        find_key = rand() % RECORD_NUMBER;
//        find_table = rand() % 10 + 1;
//        // trx_id = rand() % 8 + 1;
//        find(find_table, find_key, trx_id);
//    }
//    trx_commit(trx_id);
//
//    printf("FIND THREAD IS DONE.\n");
//
//    return NULL;
//}
//
//void* update_thread_func(void* arg)
//{
//    int update_key;
//    int update_table;
//    int trx_id = trx_begin();
//    for (int k = 0 ; k < COUNT ; k++){
//        update_key = rand() % RECORD_NUMBER;
//        update_table = rand() % 10 + 1;
//        db_update(update_table, update_key, "CHANGED", trx_id);
//    }
//    trx_commit(trx_id);
//
//    printf("UPDATE THREAD IS DONE.\n");
//
//    return NULL;
//}
//
//void* find_update_thread_func(void* arg)
//{
//    int key;
//    int table;
//    int select;
//    int trx_id = trx_begin();
//    printf("%d threads BEGIN\n", trx_id);
//    for (int k = 0 ; k < COUNT; k++){
//        key = rand() % RECORD_NUMBER;
//        table = rand() % 10 + 1;
//        select = rand() % 2;
//        if (select == 0){
//            find(table, key, trx_id);
//        } else {
//            db_update(table, key, "CHANGED", trx_id);
//        }
//    }
//    trx_commit(trx_id);
//
//    printf("%d threads COMMITED\n", trx_id);
//    return NULL;
//}

int main() {

    int i;
//    char* value = "value";
    pthread_t threads[NUM_THREADS];

    init_db(100);
    init_lock_table();

    pthread_create(&threads[0], NULL, thread1, NULL);
    pthread_create(&threads[1], NULL, thread2, NULL);
    pthread_create(&threads[2], NULL, thread3, NULL);

    pthread_join(threads[0], NULL);
    pthread_join(threads[1], NULL);
    pthread_join(threads[2], NULL);

    shutdown_db();

    return 0;
}