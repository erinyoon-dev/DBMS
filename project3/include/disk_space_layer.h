//
// Created by 윤세령 on 2020/11/04.
//

#ifndef PROJ3_DISK_SPACE_LAYER_H
#define PROJ3_DISK_SPACE_LAYER_H

#include <stdint.h>
#include <sys/types.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>
#include <map>
#include <stdbool.h>
#include <limits.h>

#define LEAF_ORDER 3 // 31
#define INTERNAL_ORDER 3 // 248
#define MAX_TABLE 11
#define false 0
#define true 1
#define PAGE_SIZE 4096

using namespace std;

typedef uint64_t pagenum_t;

/* ===================
 * RECORD (LEAF PAGE)
 * =================== */
typedef struct record {
    int64_t key;
    char value[120];
}record;

/* ===================
 * RECORD (INTERNAL PAGE)
 * =================== */
typedef struct internal_record {
    int64_t key;
    int64_t pagenum;
}internal_record;

/* ===================
 * HEADER PAGE
 * =================== */
typedef struct headerPage {
    pagenum_t free_pagenum;
    pagenum_t root_pagenum;
    int64_t num_pages;
    char reserved[4072]; //4096-24
}headerPage;

/* ===================
 * LEAF/INTERNAL/FREE PAGE
 * =================== */
typedef struct generalPage {
    pagenum_t fp_pagenum; // free or parent pagenum
    uint32_t isLeaf; // 8-11byte
    uint32_t num_keys; // 12-15byte
    char reserved[104]; // 16-119byte
    pagenum_t li_pagenum; // leaf or internal pagenum
    union {
        record record[LEAF_ORDER]; // leaf page
        internal_record internal_record[INTERNAL_ORDER]; //internal page
    };
}generalPage;

/* ===================
 * PAGE_T (Type of pages)
 * =================== */
typedef struct page_t {
    // in-memory page structure
    union {
        headerPage h;
        generalPage g;
    };
}page_t;


/* ================
 * File Manager API
 * ================ */
//int open_file_table(char* pathname, int i);
// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int table_id);
// Free an on-disk page to the free page list
void file_free_page(int table_id, pagenum_t pagenum);
// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int table_id, pagenum_t pagenum, page_t* dest);
// Write an in-memory page(src) to the on-disk page
void file_write_page(int table_id, pagenum_t pagenum, const page_t* src);

page_t * make_page(void);
page_t * make_general_page(void);
page_t * make_header_page(void);

#endif //PROJ3_DISK_SPACE_LAYER_H
