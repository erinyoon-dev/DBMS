//
// Created by 윤세령 on 2020/11/04.
//

#include "../include/disk_space_layer.h"
#include "../include/buffer_layer.h"

#define NUM_OF_PAGES 250

page_t * make_page(void) {

    page_t * page = (page_t *)calloc(1, sizeof(page_t));
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

// from proj2
pagenum_t file_alloc_page(int table_id) {
    page_t *hp, *fp, *newPage;
    pagenum_t new_pagenum, free_pagenum, start_pagenum;

    hp = make_page();
    buf_read_page(table_id, 0, hp);

    new_pagenum = hp->h.free_pagenum;
    // case 1: No more freePage - create new pages
    if (new_pagenum == 0) {
        printf("1MB page creating...\n");
        start_pagenum = hp->h.num_pages;    // for faster DBMS(instead of search eof)
        fp = make_page();
        for (int i = 1; i <= NUM_OF_PAGES; i++) {
            fp->g.fp_pagenum = i == NUM_OF_PAGES? 0 : start_pagenum + i;
            free_pagenum = start_pagenum + i - 1;
            file_write_page(table_id, free_pagenum, fp);    // freepage는 지금 당장 버퍼에 필요하지 않으므로 disk I/O
            hp->h.num_pages++;
        }
        free(fp);

        hp->h.free_pagenum = start_pagenum + 1;
        buf_write_page(table_id, 0, hp);
        free(hp);

        return start_pagenum;
    }
        // case 2: there is free page
    else {
        newPage = make_page();

        buf_read_page(table_id, new_pagenum, newPage);
        hp->h.free_pagenum = newPage->g.fp_pagenum;
        buf_write_page(table_id, 0, hp);
        free(hp);

        newPage->g.fp_pagenum = 0;
        newPage->g.isLeaf = 0;
        newPage->g.num_keys = 0;
        newPage->g.li_pagenum = 0;
        buf_write_page(table_id, new_pagenum, newPage);
        free(newPage);

        return new_pagenum;
    }
}

// when page become freePage caused by many deletes
void file_free_page(int table_id, pagenum_t pagenum) {
    page_t * fp = make_page();
    buf_read_page(table_id, pagenum, fp);
    page_t * hp = make_page();
    buf_read_page(table_id, 0, hp);
    int i;

    // initialize page's meta data
    fp->g.num_keys = 0;
    fp->g.li_pagenum = 0;
    // case 1: the page is Leaf
    if (fp->g.isLeaf) {
        fp->g.isLeaf = 0;
        for (i = 0; i < LEAF_ORDER; i++) {
            fp->g.record[i].key = 0;
            memset(fp->g.record[i].value, 0, 120);
        }
    }
    // case 2: the page is Internal
    else {
        for (i = 0; i < INTERNAL_ORDER; i++) {
            fp->g.internal_record[i].key = 0;
            fp->g.internal_record[i].pagenum = 0;
        }
    }
    // link page to free page list
    fp->g.fp_pagenum = hp->h.free_pagenum;
    hp->h.free_pagenum = pagenum;

    buf_write_page(table_id, pagenum, fp);
    buf_write_page(table_id, 0, hp);
    free(fp);
    free(hp);

    return;
}

// on-disk상의 페이지를 in-memory 페이지 구조(dest)안으로 불러옴
void file_read_page(int table_id, pagenum_t pagenum, page_t* dest) {
    int fd = find_fd(table_id);
    lseek(fd, pagenum * PAGE_SIZE, 0);
    read(fd, dest, sizeof(page_t));
    // printf(":::Read pagenum %lu\n", pagenum);
}

// fd 테이블의 pagenum*4096 위치에 src의 내용을 sizeof(page_t)크기만큼 쓴다
void file_write_page(int table_id, pagenum_t pagenum, const page_t* src) {
    int fd = find_fd(table_id);
    lseek(fd, pagenum * PAGE_SIZE, 0);
    write(fd, src, sizeof(page_t));
    // printf(":::Write pagenum %lu\n", pagenum);
}