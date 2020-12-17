//
// Created by 윤세령 on 2020/11/04.
//

#include "index_layer.h"
#include "lock_manager.h"


// for PRINT
int front = -1;
int q_size = 0;
int rear = -1;
pagenum_t Queue[INT_MAX];

int IsEmpty(void) {
    if(front==rear) //front와 rear가 같으면 큐는 비어있는 상태
        return 1;
    else return 0;
}

void enqueue (pagenum_t pagenum) {
    rear = (rear + 1) % INT_MAX;
    Queue[rear]=pagenum;
    q_size++;
}

pagenum_t dequeue() {
    front = (front+1) % INT_MAX;
    q_size--;
    return Queue[front];
}

void print_tree(int table_id) {
    int i;
    page_t * page = make_page();
    page_t * hp = make_page();
    buf_read_page(table_id,  0, hp);

    if (hp->h.root_pagenum == 0) {
        printf("Tree is empty\n");
        free(page);
        free(hp);
        return;
    }

    enqueue(hp->h.root_pagenum);
    printf("\n");

    while (!IsEmpty()) {
        int temp_size = q_size;

        while (temp_size) {
            pagenum_t pagenum = dequeue();

            buf_read_page(table_id,  pagenum, page);

            if (page->g.isLeaf) {
                for (i = 0; i < page->g.num_keys; i++) {
                    printf("(%ld, %s) ", page->g.record[i].key, page->g.record[i].value);
                }
                printf(" | ");
            }

            else {
                if (page->g.num_keys > 0){
                    printf("[%lu] ", page->g.li_pagenum);
                    enqueue(page->g.li_pagenum);
                }

                for (i = 0; i < page->g.num_keys; i++) {
                    printf("%ld [%lu] ", page->g.internal_record[i].key, page->g.internal_record[i].pagenum);
                    enqueue(page->g.internal_record[i].pagenum);
                }

                printf(" | ");
            }

            temp_size--;
        }
        printf("\n");
    }
    printf("\n");

    free(page);
    free(hp);
}
// for layered architecture
int init_index(int num_buf) {
    return init_buf(num_buf);
}

int open_idx_table(char* pathname) {
    return open_buf_table(pathname);
}

int get_tid(int table_id) {
    return get_tableList_tid(table_id);
}

// void set_tid(int table_id) {
//     set_tableList_tid(table_id);
// }

int init_table(int table_id) {
    init_tableList(table_id);
    return 0;
}

int close_idx_table(int table_id) {
    return close_buf_table(table_id);
}

int find_buf_index(int table_id, pagenum_t pagenum) {
    return get_index_from_buffer(table_id, pagenum);
}

// ROLLBACK
void idx_undo(int table_id, int64_t key, int key_index, pagenum_t pagenum, char* value) {
    page_t * page = make_page();
    buf_read_page(table_id, pagenum, page);
    memcpy(page->g.record[key_index].value, value, sizeof(value));
    buf_write_page(table_id, pagenum, page);
    free(page);
}

// FIND
pagenum_t find_leaf(int table_id, int64_t key) {

//    printf("find leaf start\n");

    int i = 0;
    page_t * hp = make_page();
    buf_read_page(table_id, 0, hp);
    // printf("DEBUG::FIND_LEAF:: hp->free_pagenum %lu\n", hp->h.free_pagenum);
    page_t * page = make_page();
    pagenum_t find_pagenum = hp->h.root_pagenum;
    pagenum_t tmp_pagenum = find_pagenum;
    buf_read_page(table_id, find_pagenum, page);
    // printf("DEBUG::FIND_LEAF:: read root pagenum %lu\n", find_pagenum);

    if (find_pagenum == 0) {
        free(hp);
        free(page);
        printf("Root is not exist.\n");
        return 0;
    }
    while (!page->g.isLeaf) {
        i = 0;
        // printf("DEBUG::FIND_LEAF:: read child pagenum %lu\n", find_pagenum);
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
        buf_read_page(table_id, find_pagenum, page);
        tmp_pagenum = find_pagenum;
    }
    // printf("DEBUG::FIND_LEAF:: page is leaf pagenum %lu\n", find_pagenum);
    free(page);
    free(hp);

    return find_pagenum;
}

int idx_find(int table_id, int64_t key, char* ret_val, int trx_id) {

    printf("TRX %d:: index_find start\n", trx_id);
    int i;
    pagenum_t find_pagenum = find_leaf(table_id, key);

    if (find_pagenum == 0) {
        return -1;
    }

    page_t * page = make_page();
    buf_read_page(table_id, find_pagenum, page);

    // acquire record lock
    lock_t* lock = lock_acquire(table_id, key, trx_id, SHARED);
    if (lock == NULL)
        return -1;

    for (i = 0; i < page->g.num_keys; i++) {
        if (page->g.record[i].key == key) {
            // printf("DEBUG::FIND:: duplicate key in %lu\n", find_pagenum);
            break;
        }
    }
    if (i == page->g.num_keys) {
        // printf("DEBUG::FIND:: key is not exist in %lu. i = %d\n", find_pagenum, i);
        free(page);
        return -1;
    }
    else {
        // ERROR in insert
        strcpy(ret_val, page->g.record[i].value);
        set_trx_state(trx_id, NONE);
//        set_trx_waitLock(trx_id);
        free(page);
        printf("TRX %d:: index_find end\n", trx_id);
        return 0;
    }
    perror("The table is not opened yet. Please execute \"open <pathname>.\"");
    return -1;

}

/* Finds the appropriate place to
 * split a node that is too big into two.
 */
int cut(int length) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}

/* Find the matching key and modify the values.
 * 1) return 0 (SUCCESS): operation is successfully done
 * and the transaction can continue the next operation.
 * 2) return non-zero (FAILED): operation is failed (e.g., deadlock detected)
 * and the transaction should be aborted
 */
int idx_update(int table_id, int64_t key, char* values, int trx_id) {

    if(get_trx(trx_id) == NULL) {
        return -1;
    }

    printf("TRX %d:: index_update start\n", trx_id);
    int i;
    pagenum_t find_pagenum = find_leaf(table_id, key);

    if(find_pagenum == 0) {
        printf("TRX %d:: find pagenum is 0\n", trx_id);
        return -1;
    }

    page_t * page = make_page();
    buf_read_page(table_id, find_pagenum, page);

    // acquire record lock
    lock_t* lock = lock_acquire(table_id, key, trx_id, EXCLUSIVE);

    for (i = 0; i < page->g.num_keys; i++) {
        if (page->g.record[i].key == key)
            break;
    }
    // Case 1: there is no record that matched with key.
    if (i == page->g.num_keys) {
        printf("TRX %d:: There is no record that matched with key\n", trx_id);
        free(page);
        return -1;
    }

    // Case 2: there is a matched record
    // add undo log
    trx_rollback(trx_id, table_id, key, i, page->g.record[i].value);
    // update
    memcpy(page->g.record[i].value, values, sizeof(values));
    buf_write_page(table_id, find_pagenum, page);

    set_trx_state(trx_id,NONE);
//    set_trx_waitLock(trx_id);

    return 0;
}

// INSERTION
int idx_insert(int table_id, int64_t key, char* value) {
    // insert하려는 key 중복여부 확인
//    if (idx_find(table_id, key, value) == 0) {
//        return 1;
//    }
    page_t * hp = make_page();
    buf_read_page(table_id, 0, hp);
    // printf("DEBUG::INSERT:: hp->free_pagenum %lu\n", hp->h.free_pagenum);
    pagenum_t root_pagenum = hp->h.root_pagenum;
    free(hp);
//    unPin(table_id, 0);

    // if root is not exist, make new root page
    if (root_pagenum == 0) {
        // printf("MAKE ROOT\n");
        make_root_page(table_id, key, value);
        // printf("MAKE ROOT DONE\n");
        return 0;
    }
    pagenum_t leaf_pagenum = find_leaf(table_id, key);

    if (leaf_pagenum == 0){
        return -1;
    }

    page_t * leaf = make_page();
    buf_read_page(table_id, leaf_pagenum, leaf);
    // printf("DEBUG::INSERT:: read leaf pagenum %lu\n", leaf_pagenum);

    if (leaf->g.num_keys < LEAF_ORDER) {
        // leaf에 공간이 남는 경우
        return insert_into_leaf(table_id, leaf_pagenum, leaf, key, value);
    }
    else {
        // order를 초과해서 split이 필요한 경우
        return insert_into_leaf_after_splitting(table_id, leaf_pagenum, key, value);
    }

    perror("The table is not opened yet. Please execute \"open <pathname>.\"");
    return 1;
}

void make_root_page(int table_id, int64_t key, char * value) {
    page_t * root = make_general_page();
    page_t * hp = make_page();
    pagenum_t root_pagenum = file_alloc_page(table_id);
    buf_read_page(table_id, 0, hp);
    // printf("ROOT: free pagenum %lu\n", hp->h.free_pagenum);
    // printf("ROOT: root pagenum %lu\n", root_pagenum);

    hp->h.root_pagenum = root_pagenum;
    root->g.record[0].key = key;
    strcpy(root->g.record[0].value, value);
    root->g.isLeaf = true;
    root->g.num_keys = 1;
    root->g.fp_pagenum = 0;

    buf_write_page(table_id, 0, hp);
//    unPin(table_id, 0);
//    unPin(table_id, 0);

    buf_write_page(table_id, root_pagenum, root);
//    unPin(table_id, root_pagenum);
    free(root);
    free(hp);

    return;
}

int get_right_page_index(int table_id, page_t * page, pagenum_t right_pagenum) {

    int right_index = 0;
    page_t * right = make_page();
    buf_read_page(table_id, right_pagenum, right);
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

int insert_into_leaf(int table_id, pagenum_t pagenum, page_t * page, int64_t key, char * value) {

    int i, insertion_point;
    insertion_point = 0;

    while (insertion_point < page->g.num_keys &&
           page->g.record[insertion_point].key < key)
        insertion_point++;

    for (i = page->g.num_keys; i > insertion_point; i--) {
        memcpy(page->g.record + i, page->g.record + (i - 1), sizeof(record));
    }
    page->g.record[insertion_point].key = key;
    strcpy(page->g.record[insertion_point].value, value);
    page->g.num_keys++;

    buf_write_page(table_id, pagenum, page);
//    unPin(table_id, pagenum);
//    unPin(table_id, pagenum);

    free(page);
    return 0;
}

int insert_into_leaf_after_splitting(int table_id, pagenum_t pagenum, int64_t key, char * value) {

    // printf("DEBUG::SPLIT:: pagenum %lu\n", pagenum);
    page_t * new_leaf = make_general_page();
    new_leaf->g.isLeaf = true;

    page_t * leaf = make_page();
    int insertion_index, split, i, j;
    int64_t new_key;
    pagenum_t new_leaf_pagenum = file_alloc_page(table_id);

    buf_read_page(table_id, pagenum, leaf);
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
    buf_write_page(table_id, pagenum, leaf);

    // printf("SPLIT:: leaf->fp_pagenum %lu\n", leaf->g.fp_pagenum);
    new_key = new_leaf->g.record[0].key;

    // printf("SPLIT:: leaf->fp_pagenum %lu\n", leaf->g.fp_pagenum);
    buf_write_page(table_id, new_leaf_pagenum, new_leaf);
//    unPin(table_id, pagenum);
//    unPin(table_id, pagenum);
//    unPin(table_id, new_leaf_pagenum);
    return insert_into_parent(table_id, pagenum, leaf, new_key, new_leaf_pagenum, new_leaf);
}

int insert_into_internal(int table_id, pagenum_t pagenum, page_t * page, int right_index, int64_t key, pagenum_t right_pagenum) {
    int i;

    for (i = page->g.num_keys; i > right_index; i--) {
        page->g.internal_record[i].key = page->g.internal_record[i - 1].key;
        page->g.internal_record[i].pagenum = page->g.internal_record[i - 1].pagenum;
    }
    page->g.internal_record[right_index].key = key;
    page->g.internal_record[right_index].pagenum = right_pagenum;
    page->g.num_keys++;
    buf_write_page(table_id, pagenum, page);
//    unPin(table_id, pagenum);
//    unPin(table_id, pagenum);
    free(page);

    return 0;
}

int insert_into_internal_after_splitting(int table_id, pagenum_t pagenum, page_t * old_inter, int right_index, int64_t key, pagenum_t right_pagenum) {

    int i, j, split;
    int64_t k_prime;
    page_t * new_inter = make_general_page();
    page_t * child = make_page();

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
    new_inter->g.li_pagenum = record[split].pagenum;
    for (++i, j = 0; i < INTERNAL_ORDER + 1; i++, j++) {
        new_inter->g.internal_record[j].pagenum = record[i].pagenum;
        new_inter->g.internal_record[j].key = record[i].key;
        new_inter->g.num_keys++;
    }
    free(record);

    pagenum_t new_inter_pagenum = file_alloc_page(table_id);
    pagenum_t child_pagenum = new_inter->g.li_pagenum;
    buf_read_page(table_id, child_pagenum, child);
    child->g.fp_pagenum = new_inter_pagenum;
    buf_write_page(table_id, child_pagenum, child);
//    unPin(table_id, child_pagenum);
//    unPin(table_id, child_pagenum);
    for (i = 0; i < new_inter->g.num_keys; i++) {
        child_pagenum = new_inter->g.internal_record[i].pagenum;
        buf_read_page(table_id, child_pagenum, child);
        child->g.fp_pagenum = new_inter_pagenum;
        buf_write_page(table_id, child_pagenum, child);
    }
    free(child);

    new_inter->g.fp_pagenum = old_inter->g.fp_pagenum;
    buf_write_page(table_id, pagenum, old_inter);
//    unPin(table_id, pagenum);
//    unPin(table_id, pagenum);

    buf_write_page(table_id, new_inter_pagenum, new_inter);
//    unPin(table_id, new_inter_pagenum);

    return insert_into_parent(table_id, pagenum, old_inter, k_prime, new_inter_pagenum, new_inter);
}


int insert_into_parent(int table_id, pagenum_t left_pagenum, page_t * left, int64_t key, pagenum_t right_pagenum, page_t * right) {

    int i, j, right_index;
    page_t * page;
    pagenum_t pagenum = left->g.fp_pagenum;

    if (pagenum == 0) {
        // printf("DEBUG::PARENT:: insert to new root\n");
        return insert_into_new_root(table_id, left_pagenum, left, key, right_pagenum, right);
    }
    // printf("DEBUG::PARENT:: right_pagenum %lu\n", right_pagenum);
    page = make_page();
    buf_read_page(table_id, pagenum, page);

    right_index = get_right_page_index(table_id, page, right_pagenum);
    // printf("DEBUG::PARENT:: right_index %d\n", right_index);

    free(left);
    free(right);
    if (page->g.num_keys < INTERNAL_ORDER) {
        // printf("DEBUG::PARENT:: parent_pagenum %lu\n", pagenum);
        return insert_into_internal(table_id, pagenum, page, right_index, key, right_pagenum);
    }
    return insert_into_internal_after_splitting(table_id, pagenum, page, right_index, key, right_pagenum);
}

int insert_into_new_root(int table_id, pagenum_t left_pagenum, page_t * left, int64_t key, pagenum_t right_pagenum, page_t * right) {

    page_t * root = make_general_page();
    pagenum_t root_pagenum = file_alloc_page(table_id);
    // printf("DEBUG::NEW_ROOT:: left: %lu, right: %lu\n", left_pagenum, right_pagenum);
    // printf("DEBUG::NEW_ROOT:: root pagenum %lu\n", root_pagenum);
    // printf("DEBUG::NEW_ROOT:: hp->free_pagenum %lu\n", hp->h.free_pagenum);

    page_t * hp = make_page();
    buf_read_page(table_id, 0, hp);

    root->g.fp_pagenum = 0;
    root->g.li_pagenum = left_pagenum;
    root->g.internal_record[0].key = key;
    root->g.internal_record[0].pagenum = right_pagenum;
    root->g.num_keys++;
    root->g.isLeaf = 0;

    hp->h.root_pagenum = root_pagenum;
    left->g.fp_pagenum = root_pagenum;
    right->g.fp_pagenum = root_pagenum;

    buf_write_page(table_id, root_pagenum, root);
    buf_write_page(table_id, 0, hp);
    buf_write_page(table_id, left_pagenum, left);
    buf_write_page(table_id, right_pagenum, right);

//    unPin(table_id, root_pagenum);
//    unPin(table_id, left_pagenum);
//    unPin(table_id, right_pagenum);
//    unPin(table_id, 0);
//    unPin(table_id, 0);

    free(root);
    free(hp);
    free(left);
    free(right);

    return 0;
}

int get_neighbor_index(int table_id, page_t * parent, pagenum_t pagenum) {

    int i;
    // case : one more pagenum
    if (parent->g.li_pagenum == pagenum)
        return -2;

    for (i = 0; i < parent->g.num_keys; i++) {
        if (parent->g.internal_record[i].pagenum == pagenum){
            // case : neighbor is one more pagenum
            if (i == 0)
                return -1;
                // else
            else
                return i - 1;
        }
    }
    printf("Search for nonexistent pointer to pointer in parent.\n");
    exit(EXIT_FAILURE);
}

pagenum_t get_neighbor_pagenum(int table_id, page_t * parent, pagenum_t pagenum) {

    int neighbor_index = get_neighbor_index(table_id, parent, pagenum);
    if (neighbor_index == -1)
        return parent->g.li_pagenum;
    else if (neighbor_index == -2)
        return parent->g.internal_record[0].pagenum;
    else
        return parent->g.internal_record[neighbor_index].pagenum;
}

pagenum_t remove_entry_from_page(int table_id, int64_t key, pagenum_t pagenum, page_t * page) {
    int i;

    if (page->g.isLeaf) {
        i = 0;
        while (page->g.record[i].key != key)
            i++;
        for (++i; i < page->g.num_keys; i++) {
            memcpy(page->g.record + (i - 1), page->g.record + i, sizeof(struct record));
        }
    }
    else {
        i = 0;
        while (page->g.internal_record[i].key != key)
            i++;
        for (++i; i < page->g.num_keys; i++) {
            memcpy(page->g.internal_record + (i - 1), page->g.internal_record + i, sizeof(struct internal_record));
        }
    }
    page->g.num_keys--;
    buf_write_page(table_id, pagenum, page);
//    unPin(table_id, pagenum);

    return pagenum;
}

int adjust_root(int table_id, pagenum_t pagenum) {

    page_t * root = make_page();
    buf_read_page(table_id, pagenum, root);

    // nonempty root
    if (root->g.num_keys > 0) {
//        unPin(table_id, pagenum);
        free(root);
        return 0;
    }
    // empty root

    page_t * hp = make_page();
    buf_read_page(table_id, 0, hp);

    page_t * new_root = make_page();
    pagenum_t new_root_pagenum;
    if (!root->g.isLeaf) {
        new_root_pagenum = root->g.li_pagenum;
        buf_read_page(table_id, new_root_pagenum, new_root);
        // printf("DEBUG::NEW_ROOT:: pagenum: %lu\n", new_root_pagenum);
        hp->h.root_pagenum = new_root_pagenum;
        new_root->g.fp_pagenum = 0;
        buf_write_page(table_id, new_root_pagenum, new_root);
//        unPin(table_id, new_root_pagenum);
//        unPin(table_id, new_root_pagenum);
    }
    else {
        hp->h.root_pagenum = 0;
    }

    file_free_page(table_id, pagenum);
    buf_write_page(table_id, 0, hp);
//    unPin(table_id, 0);
//    unPin(table_id, 0);
//    unPin(table_id, pagenum);

    free(hp);
    free(root);
    free(new_root);

    return 0;
}

int coalesce_pages(int table_id, pagenum_t pagenum, page_t * page, pagenum_t parent_pagenum, page_t * parent,
                   pagenum_t neighbor_pagenum, page_t * neighbor, int neighbor_index) {

    int i, j, neighbor_insertion_index, n_end;
    page_t * tmp;
    pagenum_t tmp_pagenum;

    int64_t k_prime;

    if (neighbor_index == -2){
        tmp = page;
        page = neighbor;
        neighbor = tmp;

        tmp_pagenum = pagenum;
        pagenum = neighbor_pagenum;
        neighbor_pagenum = tmp_pagenum;

        k_prime = parent->g.internal_record[0].key;
    }
    else if (neighbor_index == -1)
        k_prime = parent->g.internal_record[0].key;
    else
        k_prime = parent->g.internal_record[neighbor_index + 1].key;
    // printf("DEBUG::K_PRIME %ld\n", k_prime);


    neighbor_insertion_index = neighbor->g.num_keys;

    if (!page->g.isLeaf){
        // Append k_prime.
        neighbor->g.internal_record[neighbor_insertion_index].key = k_prime;
        neighbor->g.internal_record[neighbor_insertion_index].pagenum = page->g.li_pagenum;
        neighbor->g.num_keys++;

        n_end = page->g.num_keys;

        for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++){
            memcpy(neighbor->g.internal_record + i, page->g.internal_record + j, sizeof(struct internal_record));
            neighbor->g.num_keys++;
            page->g.num_keys--;
        }

        tmp = make_page();
        tmp_pagenum = neighbor->g.li_pagenum;
        buf_read_page(table_id, tmp_pagenum, tmp);
        tmp->g.fp_pagenum = neighbor_pagenum;
        file_write_page(table_id, tmp_pagenum, tmp);
//        unPin(table_id, tmp_pagenum);
//        unPin(table_id, tmp_pagenum);

        for (i = 0; i < neighbor->g.num_keys; i++){
            tmp_pagenum = neighbor->g.internal_record[i].pagenum;
            buf_read_page(table_id, tmp_pagenum, tmp);
            tmp->g.fp_pagenum = neighbor_pagenum;
            file_write_page(table_id, tmp_pagenum, tmp);
//            unPin(table_id, tmp_pagenum);
//            unPin(table_id, tmp_pagenum);
        }
        free(tmp);
    }
    else{
        n_end = page->g.num_keys;
        for (i = neighbor_insertion_index, j = 0; j < n_end; i++, j++){
            memcpy(neighbor->g.record + i, page->g.record + j, sizeof(struct record));
            neighbor->g.num_keys++;
            page->g.num_keys--;
        }
        neighbor->g.li_pagenum = page->g.li_pagenum;
    }

    file_write_page(table_id, neighbor_pagenum, neighbor);

    // unPin(table_id, pagenum);
    // unPin(table_id, parent_pagenum);
//    unPin(table_id, neighbor_pagenum);

    delete_entry(table_id, k_prime, parent_pagenum);

    file_free_page(table_id, pagenum);

    free(parent);
    free(page);
    free(neighbor);

    return 0;
}

int redistribute_pages(int table_id, pagenum_t pagenum, page_t * page, pagenum_t parent_pagenum, page_t * parent, pagenum_t neighbor_pagenum, page_t * neighbor, int neighbor_index) {

    int i, j;
    int64_t k_prime;

    int delay_value = neighbor->g.num_keys / 2;

    if (page->g.isLeaf) {
        // printf("DEBUG::REDISTRIBUTE:: leaf page %lu don't redistribute.\n", pagenum);
        return -1;
    }

    // neighbor is on right
    if (neighbor_index == -2) {
        k_prime = parent->g.internal_record[0].key;

        page->g.internal_record[page->g.num_keys].key = k_prime;
        page->g.internal_record[page->g.num_keys].pagenum = neighbor->g.li_pagenum;

        // printf("DEBUG::REDISTRIBUTE:: one more page. k_prime %ld\n", k_prime);
        page_t * child = make_page();
        pagenum_t child_pagenum = neighbor->g.li_pagenum;
        buf_read_page(table_id, child_pagenum, child);
        child->g.fp_pagenum = pagenum;
        buf_write_page(table_id, child_pagenum, child);
//        unPin(table_id, child_pagenum);
//        unPin(table_id, child_pagenum);
        for (i = 0, j = page->g.num_keys + 1; j < delay_value; i++, j++) {
            page->g.internal_record[j] = neighbor->g.internal_record[i];
            child_pagenum = neighbor->g.internal_record[i].pagenum;
            buf_read_page(table_id, child_pagenum, child);
            child->g.fp_pagenum = pagenum;
            buf_write_page(table_id, child_pagenum, child);
//            unPin(table_id, child_pagenum);
//            unPin(table_id, child_pagenum);
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

        // printf("DEBUG::REDISTRIBUTE:: left neighbor page. k_prime %ld\n", k_prime);

        page_t * child = make_page();
        pagenum_t child_pagenum = neighbor->g.internal_record[delay_value].pagenum;
        buf_read_page(table_id, child_pagenum, child);
        child->g.fp_pagenum = pagenum;
        buf_write_page(table_id, child_pagenum, child);
//        unPin(table_id, child_pagenum);
//        unPin(table_id, child_pagenum);
        // neighbor->num_keys/2개만큼 한번에 넣는다.
        for (i = delay_value + 1, j = 0; i < neighbor->g.num_keys; i++, j++) {
            page->g.internal_record[j] = neighbor->g.internal_record[i];
            child_pagenum = neighbor->g.internal_record[i].pagenum;
            buf_read_page(table_id, child_pagenum, child);
            child->g.fp_pagenum = pagenum;
            buf_write_page(table_id, child_pagenum, child);
//            unPin(table_id, child_pagenum);
//            unPin(table_id, child_pagenum);
        }
        free(child);
        parent->g.internal_record[parent->g.num_keys - 1].key = neighbor->g.internal_record[delay_value].key;
    }

    page->g.num_keys += delay_value;
    neighbor->g.num_keys -= delay_value;

    buf_write_page(table_id, pagenum, page);
    buf_write_page(table_id, parent_pagenum, parent);
    buf_write_page(table_id, neighbor_pagenum, neighbor);

//    unPin(table_id, pagenum);
//    unPin(table_id, parent_pagenum);
//    unPin(table_id, neighbor_pagenum);

    free(page);
    free(parent);
    free(neighbor);

    return 0;
}

int delete_entry(int table_id, int64_t key, pagenum_t delete_pagenum) {
    int capacity, neighbor_index;
    page_t * page, *hp;
    pagenum_t root_pagenum;

    page = make_page();
    buf_read_page(table_id, delete_pagenum, page);
    delete_pagenum = remove_entry_from_page(table_id, key, delete_pagenum, page);

    hp = make_page();
    buf_read_page(table_id, 0, hp);
    root_pagenum = hp->h.root_pagenum;
    free(hp);

    if (delete_pagenum == root_pagenum) {
        free(page);
//        unPin(table_id, 0);
//        unPin(table_id, delete_pagenum);
        return adjust_root(table_id, delete_pagenum);
    }

    // delete를 수행하고 나서 키의 개수가 1개 이상이면 merge를 수행하지 않으므로 함수 종료
    if (page->g.num_keys >= 1) {
//        buf_write_page(table_id, delete_pagenum, page);
        free(page);
//        unPin(table_id, 0);
//        unPin(table_id, delete_pagenum);
        return 0;
    }

    printf("DEBUG::DELETE:: there are no key. delayed merge start at %lu\n", delete_pagenum);
    page_t * parent = make_page();
    pagenum_t parent_pagenum = page->g.fp_pagenum;
    buf_read_page(table_id, parent_pagenum, parent);

    neighbor_index = get_neighbor_index(table_id, parent, delete_pagenum);
    // printf("DEBUG::DELETE:: neighbor index %d\n", neighbor_index);
    page_t * neighbor = make_page();
    pagenum_t neighbor_pagenum = get_neighbor_pagenum(table_id, parent, delete_pagenum);
    // printf("DEBUG::DELETE:: neighbor pagenum %lu\n", neighbor_pagenum);
    buf_read_page(table_id, neighbor_pagenum, neighbor);

    capacity = page->g.isLeaf ? cut(LEAF_ORDER) : cut(INTERNAL_ORDER);
    // printf("DEBUG::DELETE:: capacity: %d\n", capacity);


    if (neighbor->g.num_keys >= capacity) {
        // printf("DEBUF::DELETE:: neighbor_num_keys %d\n", neighbor->g.num_keys);
        if (!page->g.isLeaf) {
            // printf("DEBUG::DELETE:: REDISTRIBUTE START at %lu\n", delete_pagenum);
            // redistribute
            return redistribute_pages(table_id, delete_pagenum, page, parent_pagenum, parent, neighbor_pagenum, neighbor, neighbor_index);
        }
    }
    // printf("DEBUG::DELETE:: COALESCE START at %lu\n", delete_pagenum);
    // Leaf 페이지거나, internal페이지에서 coalesce를 못하는 경우만 제외하고
    // 나머지는 모두 coalesce함
    return coalesce_pages(table_id, delete_pagenum, page, parent_pagenum, parent, neighbor_pagenum, neighbor, neighbor_index);
}

int idx_delete(int table_id, int64_t key) {
    int isExist;
    pagenum_t key_leafnum;//
//    char * value = (char *)malloc(120 * sizeof(char));
//
//    // printf("DEBUG::DELETE:: delete key %ld\n", key);
//    isExist = idx_find(table_id, key, value);
//    free(value);
//    if (isExist != 0) return -1;

    key_leafnum = find_leaf(table_id, key);
    if (key_leafnum != 0)
        return delete_entry(table_id, key, key_leafnum);
    else return -1;
}

bool index_check_table(int table_id){
    return buf_check_table(table_id);
}