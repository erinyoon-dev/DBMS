/*
 * db.c
 * FILE* I/O
 */
#define _CRT_SECURE_NO_WARNINGS

#include "db.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>

int fd;

void usage_1 (void) {
    printf ("Enter any of the following commands after the prompt > :\n"
    "\to <p>     -- open file which is in pathname <p>.\n"
    "\ti <k> <v> -- Insert 1-999 as key.\n"
    "\tj <k> <v> -- Insert <k> (an 64-bit integer) as key and <v> as value.\n"
    "\tf <k>     -- Find the value under key <k> and print value.\n"
    "\td <k>     -- Delete key <k> and its associated value.\n"
    "\tb 		 -- Delete key 1-999.\n"
    "\tp <p>     -- Shows page which has page number <p>.\n"
    "\tq         -- Quit. (Or use Ctrl-D.)\n"
    "\t?         -- Print this help message.\n");
}

void print_page (pagenum_t pagenum) {
    
    int i;
    page_t * page = make_page();
    file_read_page(pagenum, page);
    printf ("parent number : %lu\n", page->g.fp_pagenum);
    printf ("number of keys : %d\n", page->g.num_keys);
    if (page->g.isLeaf) {
        printf ("next leaf page number : %lu\n", page->g.li_pagenum);
        for (i = 0; i < page->g.num_keys; i++) {
            printf("%d | key : %ld , value : %s\n", i, page->g.record[i].key, page->g.record[i].value);
        }
    } else {
        printf ("most left child page number : %lu\n", page->g.li_pagenum);
        for (i = 0; i < page->g.num_keys; i++) {
            printf ("%d | key : %ld , child : %ld\n", i, page->g.internal_record[i].key, page->g.internal_record[i].pagenum);
        }
    }
    free (page);
}

int open_table(char * pathname) {
	if (!access(pathname, F_OK)) {
		// printf("DEBUG::OPEN:: file exist\n");
		fd = open(pathname, O_RDWR, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
		// printf("DEBUG::OPEN:: file open success.\n");
		if (fd == -1) return -1;
		return fd;
	}
	// file is not exist. create new file
	else {
		fd = open(pathname, O_CREAT | O_RDWR, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
		if (fd == -1) return -1;
		// header page allocation
		page_t * hp = make_header_page();
		file_write_page(0, hp);
		free(hp);
		return fd;
	}
}

int db_insert(int64_t key, char * value) {
	if (fd != -1) {
		// insert하려는 key 중복여부 확인
		if (db_find(key, value) == 0) {
			return 1;
		}
		page_t * hp = make_page();
		file_read_page(0, hp);
		// printf("DEBUG::INSERT:: hp->free_pagenum %lu\n", hp->h.free_pagenum);
		pagenum_t root_pagenum = hp->h.root_pagenum;
		// if root is not exist, make new root page
		if (root_pagenum == 0) {
			// printf("MAKE ROOT\n");
			make_root_page(key, value);
			// printf("MAKE ROOT DONE\n");
			free(hp);

			return 0;
		}
		pagenum_t leaf_pagenum = find_leaf(key);
		page_t * leaf = make_page();
		file_read_page(leaf_pagenum, leaf);
		// printf("DEBUG::INSERT :: read leaf pagenum %lu\n", leaf_pagenum);

		if (leaf->g.num_keys < LEAF_ORDER) {
			// leaf에 공간이 남는 경우
			insert_into_leaf(leaf, key, value);
			file_write_page(leaf_pagenum, leaf);
			free(leaf);
			free(hp);
			return 0;
		}
		else {
			// order를 초과해서 split이 필요한 경우
			insert_into_leaf_after_splitting(leaf_pagenum, key, value);
			free(leaf);
			free(hp);
			return 0;
		}
	}
	perror("The table is not opened yet. Please execute \"open <pathname>.\"");
	return 1;
}

int db_find(int64_t key, char * ret_val) {
	if (fd != -1) {
		int i;
		ret_val = (char *)malloc(sizeof(char) * 120);

		pagenum_t find_pagenum = find_leaf(key);
		if (find_pagenum == 0) {
			free(ret_val);
			return -1;
		}

		page_t * page = make_page();
		// 찾고자 하는 offset으로부터 한 페이지 읽어옴
		file_read_page(find_pagenum, page);
		// printf("DEBUG::FIND:: read pagenum %lu\n", find_pagenum);
		for (i = 0; i < page->g.num_keys; i++) {
			if (page->g.record[i].key == key) {
				// printf("DEBUG::FIND:: duplicate key in %lu\n", find_pagenum);
				break;
			}
		}
		if (i == page->g.num_keys) {
			// printf("DEBUG::FIND:: key is not exist in %lu. i = %d\n", find_pagenum, i);
			free(ret_val);
			free(page);
			return -1;
		}
		else {
			strcpy(ret_val, page->g.record[i].value);
			free(ret_val);
			free(page);
			return 0;
		}
	}
	perror("The table is not opened yet. Please execute \"open <pathname>.\"");
	return -1;
}

int db_delete(int64_t key) {
	if (fd != -1) {
		int isExist;
		char value[120];
		pagenum_t key_leafnum;

		// printf("DEBUG::DELETE:: delete key %ld\n", key);
		isExist = db_find(key, value);
		if (isExist != 0) return 1;

		key_leafnum = find_leaf(key);
		if (key_leafnum != 0)
			delete_entry(key, key_leafnum);

		return 0;
	}
	perror("The table is not opened yet. Please execute \"open <pathname>.\"");
	return 1;
}