#ifndef BUFFER_LAYER_H
#define BUFFER_LAYER_H

#include "disk_space_layer.h"
#include <string>
#include <queue>
#include <iostream>

typedef struct frameM{
    page_t frame;
    int tid;
    pagenum_t pagenum;
    bool isDirty;
//    int pinCount;
    pthread_mutex_t page_latch;
    int buf_index;
    int next;
    int prev;
}frameM;
static pthread_mutex_t buffer_latch;

typedef struct table_list {
    int fd;
    int tid;
    bool isOpened;
}table_list;
//
//static map<pagenum_t, int> hashTable[MAX_TABLE];       // save table information and buffer index
//static table_list tableList[MAX_TABLE];
//static map<string, int> file_name_map;
//static frameM* q;
//
//static int head = 0;
////static frameM *head, *current;
//static int buf_capacity = 0;
//static int num_frames = 0;

// print
void printPoolContent(void);

// for layered architecture
int init_buf(int num_buf);
int open_buf_table(char* pathname);
int find_fd(int table_id);
int get_tableList_tid(int table_id);
void set_tableList_tid(int table_id);
void init_tableList(int table_id);
int close_buf_table(int table_id);
int shutdown_buf(void);

bool buf_check_table(int table_id);

// Buffer Manager API
bool isFull(void);
bool isHeaderPage(int f_index);
int get_LRU_index(int table_id, pagenum_t pagenum);
int get_index_from_buffer(int table_id, pagenum_t pagenum);
void buf_read_page(int table_id, pagenum_t pagenum, page_t* page);
void buf_write_page(int table_id, pagenum_t pagenum, page_t* page);
void buf_print_hash(void);
void link_list(int table_id, pagenum_t pagenum, page_t* page, int p_index);
void printTable(void);
void buf_print_tree (int table_id);
void buf_print_page(int table_id, pagenum_t pagenum);
#endif //BUFFER_LAYER_H
