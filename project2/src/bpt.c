/*
 * DBMS Project2
 * :: on-disk b+ tree implementation
 * by Seryoung Yoon
 * last modified: 16 Oct 2020
 *
 */

#include "bpt.h"
#define MAX 1000000

// FIND
pagenum_t find_leaf(int64_t key) {
    int i = 0;
    page_t * hp = make_page();
    file_read_page(0, hp);
    // printf("DEBUG::FIND_LEAF:: hp->free_pagenum %lu\n", hp->h.free_pagenum);
    page_t * page = make_page();
    pagenum_t find_pagenum = hp->h.root_pagenum;
    file_read_page(find_pagenum, page);
    // printf("DEBUG::FIND_LEAF:: read root pagenum %lu\n", find_pagenum);

    if (find_pagenum == 0) {
        free(hp);
        free(page);
        perror("Root is not exist.");
        return 0;
    }
    while (!page->g.isLeaf) {
        i = 0;
        // printf("DEBUG::FIND_LEAF:: read child pagenum %lu\n", find_pagenum);
        file_read_page(find_pagenum, page);
        while (i < page->g.num_keys) {
            // printf("DEBUG::FIND_LEAF:: page->num_keys == %u\n", page->g.num_keys);
            if (key >= page->g.internal_record[i].key)
                i++;
            else 
                break;
        }
        // key의 범위에 따라 이동
        // one more pagenum으로 이동
        if (i == 0) {
            find_pagenum = page->g.li_pagenum;
        }
        // 자식페이지로 이동
        else {
            find_pagenum = page->g.internal_record[i - 1].pagenum;
        }
        file_read_page(find_pagenum, page);
        // printf("DEBUG::FIND_LEAF:: read child pagenum %lu\n", find_pagenum);
    }
    // printf("DEBUG::FIND_LEAF:: page is leaf pagenum %lu\n", find_pagenum);
    free(page);
    free(hp);

    return find_pagenum;
}

/* Finds the appropriate place to
 * split a node that is too big into two.
 */
int cut( int length ) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}

// INSERTION

void make_root_page(int64_t key, char * value) {

    page_t * root = make_general_page();
    page_t * hp = make_page();
    pagenum_t root_pagenum = file_alloc_page();
    file_read_page(0, hp);
    // printf("ROOT: free pagenum %lu\n", hp->h.free_pagenum);
    // printf("ROOT: root pagenum %lu\n", root_pagenum);
    hp->h.root_pagenum = root_pagenum;
    root->g.record[0].key = key;
    strcpy(root->g.record[0].value, value);
    root->g.isLeaf = true;
    root->g.num_keys = 1;

    file_write_page(0, hp);
    file_write_page(root_pagenum, root);
    free(root);
    free(hp);

    return;
}

int get_right_page_index(page_t * page, pagenum_t right_pagenum) {

    int right_index = 0;
    page_t * right = make_page();
    file_read_page(right_pagenum, right);
    if (!right->g.isLeaf)
        while (right_index < page->g.num_keys && right->g.internal_record[0].key > page->g.internal_record[right_index].key)
            right_index++;
    else {
        while (right_index < page->g.num_keys && right->g.record[0].key > page->g.internal_record[right_index].key)
            right_index++;
    }
    
    free(right);

    return right_index;
}

page_t * insert_into_leaf(page_t * page, int64_t key, char * value) {

    int i, insertion_point;
    insertion_point = 0;

    while (insertion_point < page->g.num_keys &&
        page->g.record[insertion_point].key < key)
        insertion_point++;

    for (i = page->g.num_keys; i > insertion_point; i--) {
        memcpy(page->g.record + i, page->g.record + i - 1, sizeof(record));
    }
    page->g.record[insertion_point].key = key;
    strcpy(page->g.record[insertion_point].value, value);
    page->g.num_keys++;

    return page;
}

int insert_into_leaf_after_splitting(pagenum_t pagenum, int64_t key, char * value) {

    // printf("DEBUG::SPLIT:: pagenum %lu\n", pagenum);
    page_t * new_leaf = make_general_page();
    new_leaf->g.isLeaf = true;
    page_t * leaf = make_page();
    int insertion_index, split, i, j;
    int64_t new_key;
    pagenum_t new_leaf_pagenum = file_alloc_page();

    file_read_page(pagenum, leaf);
    // printf("DEBUG::SPLIT:: leaf->fp_pagenum %lu\n", leaf->g.fp_pagenum);
    record * record = (struct record *)malloc(sizeof(struct record) * (LEAF_ORDER + 1));

    insertion_index = 0;
    while (insertion_index < LEAF_ORDER && leaf->g.record[insertion_index].key < key)
        insertion_index++;

    // printf("DEBUG::SPLIT:: insertion_index %d\n", insertion_index);
    for (i = 0, j = 0; i < leaf->g.num_keys; i++, j++) {
        if (j == insertion_index) j++;
        record[j].key = leaf->g.record[i].key;
        strcpy(record[j].value, leaf->g.record[i].value);
    }

    record[insertion_index].key = key;
    strcpy(record[insertion_index].value, value);

    leaf->g.num_keys = 0;
    split = cut(LEAF_ORDER);

    for (i = 0; i < split; i++) {
        strcpy(leaf->g.record[i].value, record[i].value);
        leaf->g.record[i].key = record[i].key;
        leaf->g.num_keys++;
    }

    for (i = split, j = 0; i < LEAF_ORDER + 1; i++, j++) {
        strcpy(new_leaf->g.record[j].value, record[i].value);
        new_leaf->g.record[j].key = record[i].key;
        new_leaf->g.num_keys++;
    }

    free(record);

    new_leaf->g.li_pagenum = leaf->g.li_pagenum;
    leaf->g.li_pagenum = new_leaf_pagenum;
    new_leaf->g.fp_pagenum = leaf->g.fp_pagenum;
    file_write_page(pagenum, leaf);
    free(leaf);

    // printf("SPLIT:: leaf->fp_pagenum %lu\n", leaf->g.fp_pagenum);
    new_key = new_leaf->g.record[0].key;

    // printf("SPLIT:: leaf->fp_pagenum %lu\n", leaf->g.fp_pagenum);
    file_write_page(new_leaf_pagenum, new_leaf);
    free(new_leaf);

    return insert_into_parent(pagenum, new_key, new_leaf_pagenum);
}

page_t * insert_into_internal(page_t * page, int right_index, int64_t key, pagenum_t right_pagenum) {

    int i;

    for (i = page->g.num_keys; i > right_index; i--) {
        page->g.internal_record[i].key = page->g.internal_record[i - 1].key;
        page->g.internal_record[i].pagenum = page->g.internal_record[i - 1].pagenum;
    }
    page->g.internal_record[right_index].key = key;
    page->g.internal_record[right_index].pagenum = right_pagenum;
    page->g.num_keys++;

    return page;
}

int insert_into_internal_after_splitting(pagenum_t pagenum, int right_index, int64_t key, pagenum_t right_pagenum) {

    int i, j, split;
    int64_t k_prime;
    page_t * new_inter = make_general_page();
    page_t * old_inter = make_page();
    page_t * child = make_page();
    file_read_page(pagenum, old_inter);
    internal_record * record = (struct internal_record *)malloc(sizeof(struct internal_record) * (INTERNAL_ORDER + 1));

    for (i = 0, j = 0; i < old_inter->g.num_keys; i++, j++) {
        if (j == right_index) j++;
        record[j].pagenum = old_inter->g.internal_record[i].pagenum;
        record[j].key = old_inter->g.internal_record[i].key;
    }
    record[right_index].pagenum = right_pagenum;
    record[right_index].key = key;

    split = cut(INTERNAL_ORDER);
    old_inter->g.num_keys = 0;
    for (i = 0; i < split; i++) {
        old_inter->g.internal_record[i].pagenum = record[i].pagenum;
        old_inter->g.internal_record[i].key = record[i].key;
        old_inter->g.num_keys++;
    }
    k_prime = record[split].key;
    for (++i, j = 0; i < INTERNAL_ORDER + 1; i++, j++) {
        new_inter->g.internal_record[j].pagenum = record[i].pagenum;
        new_inter->g.internal_record[j].key = record[i].key;
        new_inter->g.num_keys++;
    }
    new_inter->g.li_pagenum = record[split].pagenum;
    free(record);
    pagenum_t new_inter_pagenum = file_alloc_page();
    pagenum_t child_pagenum = new_inter->g.li_pagenum;
    file_read_page(child_pagenum, child);
    child->g.fp_pagenum = new_inter_pagenum;
    file_write_page(child_pagenum, child);
    for (i = 0; i < new_inter->g.num_keys; i++) {
        child_pagenum = new_inter->g.internal_record[i].pagenum;
        file_read_page(child_pagenum, child);
        child->g.fp_pagenum = new_inter_pagenum;
        file_write_page(child_pagenum, child);
    }
    free(child);

    new_inter->g.fp_pagenum = old_inter->g.fp_pagenum;
    file_write_page(pagenum, old_inter);
    file_write_page(new_inter_pagenum, new_inter);
    free(old_inter);
    free(new_inter);

    return insert_into_parent(pagenum, k_prime, new_inter_pagenum);
}

int insert_into_parent(pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum) {

    int i, j, right_index;
    page_t * left = make_page();
    file_read_page(left_pagenum, left);
    page_t * page = make_page();
    pagenum_t pagenum = left->g.fp_pagenum;
    file_read_page(pagenum, page);

    if (pagenum == 0) {
        free(left);
        free(page);
        // printf("DEBUG::PARENT:: insert to new root\n");
        return insert_into_new_root(left_pagenum, key, right_pagenum);
    }
    // printf("DEBUG::PARENT:: right_pagenum %lu\n", right_pagenum);
    right_index = get_right_page_index(page, right_pagenum);
    // printf("DEBUG::PARENT:: right_index %d\n", right_index);

    if (page->g.num_keys < INTERNAL_ORDER) {
        page = insert_into_internal(page, right_index, key, right_pagenum);
        file_write_page(pagenum, page);
        // printf("DEBUG::PARENT:: parent_pagenum %lu\n", pagenum);
        free(page);
        free(left);
        return 0;
    }
    free(left);
    free(page);
    return insert_into_internal_after_splitting(pagenum, right_index, key, right_pagenum);
}

int insert_into_new_root(pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum) {

    page_t * root = make_general_page();
    pagenum_t root_pagenum = file_alloc_page();
    // printf("DEBUG::NEW_ROOT:: left: %lu, right: %lu\n", left_pagenum, right_pagenum);
    // printf("DEBUG::NEW_ROOT:: root pagenum %lu\n", root_pagenum);
    // printf("DEBUG::NEW_ROOT:: hp->free_pagenum %lu\n", hp->h.free_pagenum);

    page_t * hp = make_page();
    page_t * left = make_page();
    page_t * right = make_page();
    file_read_page(0, hp);
    file_read_page(left_pagenum, left);
    file_read_page(right_pagenum, right);

    root->g.fp_pagenum = 0;
    root->g.li_pagenum = left_pagenum;
    root->g.internal_record[0].key = key;
    root->g.internal_record[0].pagenum = right_pagenum;
    root->g.num_keys++;

    hp->h.root_pagenum = root_pagenum;
    left->g.fp_pagenum = root_pagenum;
    right->g.fp_pagenum = root_pagenum;

    file_write_page(root_pagenum, root);
    file_write_page(0, hp);
    file_write_page(left_pagenum, left);
    file_write_page(right_pagenum, right);

    free(root);
    free(hp);
    free(left);
    free(right);

    return 0;
}

// DELETION.

int get_neighbor_index(page_t * parent, pagenum_t pagenum) {

    int i;
    // case : one more pagenum
    if (parent->g.li_pagenum == pagenum)
        return -2;

    for (i = 0; i <= parent->g.num_keys; i++) {
        if (parent->g.internal_record[i].pagenum == pagenum)
            break;
    }
    // case : neighbor is one more pagenum
    if (i == 0)
        return -1;
    // else
    else
        return i - 1;
}

pagenum_t get_neighbor_pagenum(page_t * parent, pagenum_t pagenum) {

    int neighbor_index = get_neighbor_index(parent, pagenum);
    if (neighbor_index == -1)
        return parent->g.li_pagenum;
    else if (neighbor_index == -2)
        return parent->g.internal_record[0].pagenum;
    else
        return parent->g.internal_record[neighbor_index].pagenum;
}

page_t * remove_entry_from_page(int64_t key, pagenum_t pagenum) {

    int i;
    page_t * page = make_page();
    file_read_page(pagenum, page);

    if (page->g.isLeaf) {
        i = 0;
        while (page->g.record[i].key != key)
            i++;
        for (++i; i < page->g.num_keys; i++) {
            page->g.record[i - 1] = page->g.record[i];
        }
    }
    else {
        i = 0;
        while (page->g.internal_record[i].key != key)
            i++;
        for (++i; i < page->g.num_keys; i++) {
            page->g.internal_record[i - 1] = page->g.internal_record[i];
        }
    }
    page->g.num_keys--;

    return page;
}

void adjust_root(pagenum_t pagenum) {

    page_t * hp = make_page();
    page_t * root = make_page();
    file_read_page(pagenum, root);
    file_read_page(0, hp);

    // nonempty root
    if (root->g.num_keys > 0) {
        free(hp);
        free(root);
        return;
    }
    // empty root
    if (!root->g.isLeaf) {
        page_t * new_root = make_page();
        pagenum_t new_root_pagenum = root->g.li_pagenum;
        file_read_page(new_root_pagenum, new_root);
        // printf("DEBUG::NEW_ROOT:: pagenum: %lu\n", new_root_pagenum);
        hp->h.root_pagenum = new_root_pagenum;
        new_root->g.fp_pagenum = 0;
        file_write_page(new_root_pagenum, new_root);
        free(new_root);
        file_free_page(pagenum);
    }
    else {
        hp->h.root_pagenum = 0;
        file_free_page(pagenum);
    }
    file_write_page(0, hp);
    free(hp);
    free(root);
    return;
}

void coalesce_pages(pagenum_t pagenum, pagenum_t parent_pagenum, pagenum_t neighbor_pagenum, int neighbor_index) {
    
    int i, j;
    int64_t k_prime;
    page_t * page = make_page();
    page_t * neighbor = make_page();
    page_t * parent = make_page();
    file_read_page(pagenum, page);
    file_read_page(parent_pagenum, parent);
    file_read_page(neighbor_pagenum, neighbor);

    // neighbor is on right
    if (neighbor_index == -2) {
        k_prime = parent->g.internal_record[0].key;
        free(parent);
        if (page->g.isLeaf) {
            // printf("DEBUG::COAL:: one more leaf page coalesce. k_prime %ld\n", k_prime);
            for (i = 0, j = page->g.num_keys; i < neighbor->g.num_keys; i++, j++) {
                page->g.record[j] = neighbor->g.record[i];
                // printf("COAL::PAGE[%d] %ld\n", j, neighbor->g.record[i].key);
                // printf("COAL::NEIGHBOR[%d] %ld\n", i, neighbor->g.record[i].key);
            }
            page->g.li_pagenum = neighbor->g.li_pagenum;
            page->g.num_keys += neighbor->g.num_keys;
        }
        else {
            // printf("DEBUG::COAL:: one more inter page coalesce. k_prime %ld\n", k_prime);
            page->g.internal_record[page->g.num_keys].key = k_prime;
            page->g.internal_record[page->g.num_keys].pagenum = neighbor->g.li_pagenum;

            page_t * child = make_page();
            pagenum_t child_pagenum = neighbor->g.li_pagenum;
            file_read_page(child_pagenum, child);
            child->g.fp_pagenum = pagenum;
            file_write_page(child_pagenum, child);

            for (i = 0, j = page->g.num_keys + 1; i < neighbor->g.num_keys; i++, j++) {
                page->g.internal_record[j] = neighbor->g.internal_record[i];

                child_pagenum = neighbor->g.internal_record[i].pagenum;
                file_read_page(child_pagenum, child);
                child->g.fp_pagenum = pagenum;
                file_write_page(child_pagenum, child);
            }
            free(child);
            page->g.num_keys += (neighbor->g.num_keys + 1);
        }
        file_write_page(pagenum, page);
        file_free_page(neighbor_pagenum);
        free(neighbor);
        free(page);
        delete_entry(k_prime, parent_pagenum);
        // printf("DEBUG::RECURSIVE_DELETE:: delayed merge start at %lu\n", parent_pagenum);
    }

    // neighbor is on left
    else {
        if (neighbor_index == -1)
            k_prime = parent->g.internal_record[0].key;
        else
            k_prime = parent->g.internal_record[neighbor_index - 1].key;
        // printf("DEBUG::K_PRIME %ld\n", k_prime);
        free(parent);

        if (page->g.isLeaf) {
            // printf("DEBUG::COAL:: left leaf page coalesce. k_prime %ld\n", k_prime);
            for (i = 0, j = neighbor->g.num_keys; i < page->g.num_keys; i++, j++) {
                neighbor->g.record[j] = page->g.record[i];
                // printf("COAL::PAGE[%d] %ld\n", i, neighbor->g.record[i].key);
                // printf("COAL::NEIGHBOR[%d] %ld\n", j, neighbor->g.record[i].key);
            }
            neighbor->g.li_pagenum = page->g.li_pagenum;
            neighbor->g.num_keys += page->g.num_keys;
        }
        else {
            // printf("DEBUG::COAL:: left inter page coalesce. k_prime %ld\n", k_prime);
            neighbor->g.internal_record[neighbor->g.num_keys].key = k_prime;
            neighbor->g.internal_record[neighbor->g.num_keys].pagenum = page->g.li_pagenum;

            page_t * child = make_page();
            pagenum_t child_pagenum = page->g.li_pagenum;
            file_read_page(child_pagenum, child);
            child->g.fp_pagenum = neighbor_pagenum;
            file_write_page(child_pagenum, child);

            for (i = 0, j = neighbor->g.num_keys + 1; i < page->g.num_keys; i++, j++) {
                neighbor->g.internal_record[j] = page->g.internal_record[i];

                child_pagenum = page->g.internal_record[i].pagenum;
                file_read_page(child_pagenum, child);
                child->g.fp_pagenum = neighbor_pagenum;
                file_write_page(child_pagenum, child);
            }
            free(child);
            neighbor->g.num_keys += (page->g.num_keys + 1);
        }
        file_write_page(neighbor_pagenum, neighbor);
        file_free_page(pagenum);
        free(neighbor);
        free(page);
        delete_entry(k_prime, parent_pagenum);
        // printf("DEBUG::RECURSIVE_DELETE:: delayed merge start at %lu\n", parent_pagenum);
    }
    return;
}

void redistribute_pages(pagenum_t pagenum, pagenum_t parent_pagenum, pagenum_t neighbor_pagenum, int neighbor_index) {

    int i, j;
    int64_t k_prime;
    page_t * child = make_page();
    page_t * page = make_page();
    page_t * neighbor = make_page();
    page_t * parent = make_page();
    file_read_page(pagenum, page);
    file_read_page(parent_pagenum, parent);
    file_read_page(neighbor_pagenum, neighbor);
    int delay_value = neighbor->g.num_keys / 2;

    if (page->g.isLeaf) {
        printf("DEBUG::REDISTRIBUTE:: leaf page %lu don't redistribute.\n", pagenum);
        return;
    }

    // neighbor is on right
    if (neighbor_index == -2) {
        k_prime = parent->g.internal_record[0].key;
        
        page->g.internal_record[page->g.num_keys].key = k_prime;
        page->g.internal_record[page->g.num_keys].pagenum = neighbor->g.li_pagenum;

        printf("DEBUG::REDISTRIBUTE:: one more page. k_prime %ld\n", k_prime);

        pagenum_t child_pagenum = neighbor->g.li_pagenum;
        file_read_page(child_pagenum, child);
        child->g.fp_pagenum = pagenum;
        file_write_page(child_pagenum, child);
        for (i = 0, j = page->g.num_keys + 1; j < delay_value; i++, j++) {
            page->g.internal_record[j] = neighbor->g.internal_record[i];
            child_pagenum = neighbor->g.internal_record[i].pagenum;
            file_read_page(child_pagenum, child);
            child->g.fp_pagenum = pagenum;
            file_write_page(child_pagenum, child);
        }

        parent->g.internal_record[0].key = neighbor->g.internal_record[i].key;
        neighbor->g.li_pagenum = neighbor->g.internal_record[i].pagenum;
        for (++i, j = 0; i < neighbor->g.num_keys; i++, j++) {
            neighbor->g.internal_record[j] = neighbor->g.internal_record[i];
        }
        free(child);
    }
    // neighbor is on left
    else {
        k_prime = parent->g.internal_record[parent->g.num_keys - 1].key;
        // 페이지의 맨 뒤 key자리에 k_prime을 넣는다.
        page->g.internal_record[delay_value].key = k_prime;
        // 기존 one more pagenum을 가장 뒤로 민다.
        page->g.internal_record[delay_value].pagenum = page->g.li_pagenum;
        // 새로운 one more pagenum = 왼쪽 neighbor의 맨 끝
        page->g.li_pagenum = neighbor->g.internal_record[delay_value].pagenum;

        printf("DEBUG::REDISTRIBUTE:: left neighbor page. k_prime %ld\n", k_prime);

        pagenum_t child_pagenum = neighbor->g.internal_record[delay_value].pagenum;
        file_read_page(child_pagenum, child);
        child->g.fp_pagenum = pagenum;
        file_write_page(child_pagenum, child);
        // neighbor->num_keys/2개만큼 한번에 넣는다.
        for (i = delay_value + 1, j = 0; i < neighbor->g.num_keys; i++, j++) {
            page->g.internal_record[j] = neighbor->g.internal_record[i];
            child_pagenum = neighbor->g.internal_record[i].pagenum;
            file_read_page(child_pagenum, child);
            child->g.fp_pagenum = pagenum;
            file_write_page(child_pagenum, child);
        }
        free(child);
        parent->g.internal_record[parent->g.num_keys - 1].key = neighbor->g.internal_record[delay_value].key;
    }

    page->g.num_keys += delay_value;
    neighbor->g.num_keys -= delay_value;

    file_write_page(pagenum, page);
    file_write_page(parent_pagenum, parent);
    file_write_page(neighbor_pagenum, neighbor);
    free(page);
    free(parent);
    free(neighbor);

    return;
}

void delete_entry(int64_t key, pagenum_t delete_pagenum) {

    int capacity, neighbor_index;
    page_t * hp = make_page();
    file_read_page(0, hp);

    page_t * page = remove_entry_from_page(key, delete_pagenum);

    if (delete_pagenum == hp->h.root_pagenum) {
        file_write_page(delete_pagenum, page);
        free(page);
        free(hp);
        return adjust_root(delete_pagenum);
    }

    // delete를 수행하고 나서 키의 개수가 1개 이상이면 merge를 수행하지 않으므로 함수 종료
    if (page->g.num_keys != 0) {
        file_write_page(delete_pagenum, page);
        free(page);
        free(hp);
        return;
    }
    
    printf("DEBUG::DELETE:: there are no key. delayed merge start at %lu\n", delete_pagenum);
    page_t * parent = make_page();
    pagenum_t parent_pagenum = page->g.fp_pagenum;
    file_read_page(parent_pagenum, parent);

    neighbor_index = get_neighbor_index(parent, delete_pagenum);
    // printf("DEBUG::DELETE:: neighbor index %d\n", neighbor_index);
    page_t * neighbor = make_page();
    pagenum_t neighbor_pagenum = get_neighbor_pagenum(parent, delete_pagenum);
    // printf("DEBUG::DELETE:: neighbor pagenum %lu\n", neighbor_pagenum);
    file_read_page(neighbor_pagenum, neighbor);

    capacity = page->g.isLeaf ? cut(LEAF_ORDER) : cut(INTERNAL_ORDER);
    // printf("DEBUG::DELETE:: capacity: %d\n", capacity);

    
    if (neighbor->g.num_keys >= capacity) {
        // printf("DEBUF::DELETE:: neighbor_num_keys %d\n", neighbor->g.num_keys);
        if (!page->g.isLeaf) {
            // printf("DEBUG::DELETE:: REDISTRIBUTE START at %lu\n", delete_pagenum);
            // redistribute
            file_write_page(delete_pagenum, page);
            free(hp);
            free(page);
            free(neighbor);
            free(parent);
            return redistribute_pages(delete_pagenum, parent_pagenum, neighbor_pagenum, neighbor_index);
        }
    }
    // printf("DEBUG::DELETE:: COALESCE START at %lu\n", delete_pagenum);
    // Leaf 페이지거나, internal페이지에서 coalesce를 못하는 경우만 제외하고
    // 나머지는 모두 coalesce함
    file_write_page(delete_pagenum, page);
    free(hp);
    free(page);
    free(neighbor);
    free(parent);
    return coalesce_pages(delete_pagenum, parent_pagenum, neighbor_pagenum, neighbor_index);
}