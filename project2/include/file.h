#ifndef FILE_MANAGER
#define FILE_MANAGER

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h>

#define LEAF_ORDER 31
#define INTERNAL_ORDER 248

#define PAGE_SIZE 4096

// fd 전역변수로 선언
extern int fd;

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
// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page();
// Free an on-disk page to the free page list
void file_free_page(pagenum_t pagenum);
// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(pagenum_t pagenum, page_t* dest);
// Write an in-memory page(src) to the on-disk page
void file_write_page(pagenum_t pagenum, const page_t* src);

page_t * make_page(void);
page_t * make_general_page(void);
page_t * make_header_page(void);

#endif /* file.h */