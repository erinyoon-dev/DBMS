/*
 * DBMS Project2
 * :: on-disk b+ tree implementation
 * by Seryoung Yoon
 * last modified: 15 Oct 2020
 */

#ifndef __BPT_H__
#define __BPT_H__

#include "file.h"
#include <stdbool.h>
#include <fcntl.h>
#include <string.h>

#define false 0
#define true 1

#define PAGE_SIZE 4096

page_t * make_page(void);

/* ================
 * Find
 * ================ */
pagenum_t find_leaf(int64_t key);

/* ================
 * Insert
 * ================ */
int cut(int length);
int get_right_page_index(page_t * page, pagenum_t right_pagenum);
int insert_into_leaf_after_splitting(pagenum_t pagenum, int64_t key, char * value);
int insert_into_internal_after_splitting(pagenum_t pagenum, int right_index, int64_t key, pagenum_t right_pagenum);
int insert_into_parent(pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum);
int insert_into_new_root(pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum);
void make_root_page(int64_t key, char * value);
page_t * insert_into_leaf(page_t * page, int64_t key, char * value);
page_t * insert_into_internal(page_t * page, int right_index, int64_t key, pagenum_t right_pagenum);

/* ================
 * Delete
 * ================ */
pagenum_t get_neighbor_pagenum(page_t * parent, pagenum_t pagenum);
page_t * remove_entry_from_page(int64_t key, pagenum_t pagenum);
int get_neighbor_index(page_t * parent, pagenum_t pagenum);
void adjust_root(pagenum_t pagenum);
void coalesce_pages(pagenum_t pagenum, pagenum_t parent_pagenum, pagenum_t neighbor_pagenum, int neighbor_index);
void redistribute_pages(pagenum_t pagenum, pagenum_t parent_pagenum, pagenum_t neighbor_pagenum, int neighbor_index);
void delete_entry(int64_t key, pagenum_t delete_pagenum);

#endif /* __BPT_H__*/
