#ifndef INDEX_LAYER_H
#define INDEX_LAYER_H

#include "buffer_layer.h"

#include <stdbool.h>
#include <string.h>

// PRINT
int IsEmpty(void);
void enqueue (pagenum_t pagenum);
pagenum_t dequeue();
void print_tree(int table_id);
bool index_check_table(int table_id);

// layered architecture
int init_index(int num_buf);
int open_idx_table(char* pathname);
int get_tid(int table_id);
int init_table(int table_id);
int close_idx_table(int table_id);
int find_buf_index(int table_id, pagenum_t pagenum);

// Rollback
void idx_undo(int table_id, int64_t key, int key_index, pagenum_t pagenum, char* value);

// Find
pagenum_t find_leaf(int table_id, int64_t key);
int cut(int length);
int idx_find(int table_id, int64_t key, char* ret_val, int trx_id);

// Update
int idx_update(int table_id, int64_t key, char* values, int trx_id);

// Insert
void make_root_page(int table_id, int64_t key, char * value);
int get_right_page_index(int table_id, page_t * page, pagenum_t right_pagenum);
int insert_into_leaf(int table_id, pagenum_t pagenum, page_t * page, int64_t key, char * value);
int insert_into_leaf_after_splitting(int table_id, pagenum_t pagenum, int64_t key, char * value);
int insert_into_internal(int table_id, pagenum_t pagenum, page_t * page, int right_index, int64_t key, pagenum_t right_pagenum);
int insert_into_internal_after_splitting(int table_id, pagenum_t pagenum, page_t * old_inter, int right_index, int64_t key, pagenum_t right_pagenum);
int insert_into_parent(int table_id, pagenum_t left_pagenum, page_t * left, int64_t key, pagenum_t right_pagenum, page_t * right);
int insert_into_new_root(int table_id, pagenum_t left_pagenum, page_t * left, int64_t key, pagenum_t right_pagenum, page_t * right);
int idx_insert(int table_id, int64_t key, char* value);

// Delete
int get_neighbor_index(int table_id, page_t * parent, pagenum_t pagenum);
pagenum_t get_neighbor_pagenum(int table_id, page_t * parent, pagenum_t pagenum);
pagenum_t remove_entry_from_page(int table_id, int64_t key, pagenum_t pagenum, page_t * page);
int adjust_root(int table_id, pagenum_t pagenum);
int coalesce_pages(int table_id, pagenum_t pagenum, page_t * page, pagenum_t parent_pagenum, page_t * parent,
                   pagenum_t neighbor_pagenum, page_t * neighbor, int neighbor_index);
int redistribute_pages(int table_id, pagenum_t pagenum, page_t * page, pagenum_t parent_pagenum, page_t * parent,
                       pagenum_t neighbor_pagenum, page_t * neighbor, int neighbor_index);
int delete_entry(int table_id, int64_t key, pagenum_t delete_pagenum);
int idx_delete(int table_id, int64_t key);

bool index_check_table(int table_id);

#endif //INDEX_LAYER_H