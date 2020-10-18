/*
 * file.c
 * file manager for Layered Architecture
 */

#include "file.h"
#include <string.h>
#include <stddef.h>
#include <unistd.h>


#define NUM_OF_PAGES 250

int fd = -1;

page_t * make_page(void) {

    page_t * page = (page_t *)malloc(sizeof(page_t));
    return page;
}

page_t * make_general_page(void) {

    page_t * page = (page_t *)malloc(sizeof(page_t));
    page->g.fp_pagenum = 0;
    page->g.isLeaf = 0;
    page->g.num_keys = 0;
    page->g.li_pagenum = 0;
    return page;
}

page_t * make_header_page(void) {

    page_t * page = (page_t *)malloc(sizeof(page_t));
    page->h.free_pagenum = 0;
    page->h.root_pagenum = 0;
    page->h.num_pages = 1;
    return page;
}

pagenum_t file_alloc_page() {

	page_t * hp = make_page();
	file_read_page(0, hp);
	pagenum_t new_pagenum = hp->h.free_pagenum;
	// printf("DEBUG::ALLOC:: new pagenum %lu\n", new_pagenum);
	// pagenum_t num_pages = hp->h.num_pages;

	if (new_pagenum == 0) {
		printf("1MB page creating...\n");
		pagenum_t eof = lseek(fd, 0, SEEK_END);
		eof = eof / PAGE_SIZE;
		// printf("DEBUG::ALLOC:: eof %lu\n", eof);
		// printf("EOF : %lu\n", eof);
		// file_write_page(eof + NUM_OF_PAGES - 1, page);
		// make new free pages
		for (int i = 1; i <= NUM_OF_PAGES; i++) {
			page_t * fp = make_page();
			fp->g.fp_pagenum = i == NUM_OF_PAGES? 0 : eof + i;
			// printf("DEBUG::ALLOC:: fp->g.fp_pagenum %lu\n", fp->g.fp_pagenum);
			pagenum_t free_pagenum = eof + i - 1;
			file_write_page(free_pagenum, fp);
			free(fp);
		}
		hp->h.num_pages++;
		// free(fp);

		// printf("DEBUG::ALLOC:: eof %lu\n", eof);
		hp->h.free_pagenum = eof + 1;
		file_write_page(0, hp);
		// printf("DEBUG::ALLOC:: hp->free_pagenum %lu\n", hp->h.free_pagenum);
		// file_read_page(0, hp);

		// printf("DEBUG::ALLOC:: after hp->free_pagenum %lu\n", hp->h.free_pagenum);
		free(hp);
		// 새로운 페이지를 insert할 new_pagenumber

		return eof;
	}
	// 프리페이지가 있는 경우: 리스트 맨 앞의 프리페이지를 할당하고 
	// insert할 new_pagenum을 반환
	else {
		page_t * newPage = make_page();
		file_read_page(new_pagenum, newPage);
		hp->h.free_pagenum = newPage->g.fp_pagenum;
		file_write_page(0, hp);
		free(hp);
		newPage->g.fp_pagenum = 0;
		newPage->g.isLeaf = 0;
		newPage->g.num_keys = 0;
		newPage->g.li_pagenum = 0;
		file_write_page(new_pagenum, newPage);
		free(newPage);

		return new_pagenum;
	}
}

void file_free_page(pagenum_t pagenum) {
	// 많은 delete로 디스크에 있던 페이지가 프리 페이지가 되었을 때
	page_t * fp = make_page();
	file_read_page(pagenum, fp);
	page_t * hp = make_page();
	file_read_page(0, hp);
	int i;

	// 페이지 정보 초기화
	fp->g.num_keys = 0;
	fp->g.li_pagenum = 0;
	if (fp->g.isLeaf) {
		// 페이지가 leaf 페이지였을 경우
		fp->g.isLeaf = 0;
		for (i = 0; i < LEAF_ORDER; i++) {
			fp->g.record[i].key = 0;
			memset(fp->g.record[i].value, 0, 120);
		}
		// fp->g.record->value = NULL;
		// printf("DEBUG::FREE:: key: %ld, value: %s\n", fp->g.record->key, fp->g.record->value);
	}
	else {
		// 페이지가 internal 페이지였을 경우
		for (i = 0; i < INTERNAL_ORDER; i++) {
			fp->g.internal_record[i].key = 0;
			fp->g.internal_record[i].pagenum = 0;
		}
		// printf("DEBUG::FREE:: key: %ld, pagenum: %lu\n", fp->g.internal_record->key, fp->g.internal_record->pagenum);
	}
	// 페이지를 프리페이지 리스트에 연결
	fp->g.fp_pagenum = hp->h.free_pagenum;
	hp->h.free_pagenum = pagenum;

	file_write_page(pagenum, fp);
	file_write_page(0, hp);
	free(fp);
	free(hp);

	return;
}

void file_read_page(pagenum_t pagenum, page_t* dest) {
	// on-disk상의 페이지를 in-memory 페이지 구조(dest)안으로 불러옴
	// pread(fd, dest, sizeof(page_t), pagenum * PAGE_SIZE);
	lseek(fd, pagenum * PAGE_SIZE, 0);
	read(fd, dest, sizeof(page_t));
	// printf(":::Read pagenum %lu\n", pagenum);
}

// fd 테이블의 pagenum*4096 위치에 src의 내용을 sizeof(page_t)크기만큼 쓴다
void file_write_page(pagenum_t pagenum, const page_t* src) {

	// pwrite(fd, src, sizeof(page_t), pagenum * PAGE_SIZE);
	lseek(fd, pagenum * PAGE_SIZE, 0);
	write(fd, src, sizeof(page_t));
	sync();
	// printf(":::Write pagenum %lu\n", pagenum);
}