#include "../include/buffer_layer.h"

static map<pagenum_t, int> hashTable[MAX_TABLE];       // save table information and buffer index
static table_list tableList[MAX_TABLE];
static map<string, int> file_name_map;
static frameM* q;

static int head = 0;
//static frameM *head, *current;
static int buf_capacity = 0;
static int num_frames = 0;

//PRINT
void printPoolContent () {
    int i;

    for (i = 0; i < buf_capacity; i++) {
        printf("{ table %d:: index %d[%lu %s] }", q[i].tid, i, q[i].pagenum, (q[i].isDirty == true ? "O" : "X"));
        printf("\n");
    }
}

void printTable() {

    int i;
    map<string, int>::iterator it;
    for(i = 1; i <= 10; i++){
        string file_name;
        for(it = file_name_map.begin(); it != file_name_map.end(); it++){
            if (it->second == i){
                file_name = it->first;
            }
        }
        cout << "file_name " << file_name;
        printf("\ttable_id %d, fd %d, isopen %d\n", tableList[i].tid, tableList[i].fd, tableList[i].isOpened);
    }

}

// layered architecture
int init_buf(int num_buf) {

    if (buf_capacity != 0){
        printf("ERROR : already have buffer\n");
        return -1;
    }
    if (num_buf == 0){
        printf("ERROR : num_buf is 0\n");
        return -1;
    }

    q = (frameM *)calloc(num_buf, sizeof(frameM));
    pthread_mutex_init(&buffer_latch, NULL);
//    buffer_latch = PTHREAD_MUTEX_INITIALIZER;
    if (q == NULL) return -1;

    buf_capacity = num_buf;

    head = -1;

    int i;
    // initialize prev, next
    for (i = 0; i < buf_capacity; i++) {
        q[i].tid = -1;
        q[i].pagenum = -1;
        q[i].isDirty = false;
        pthread_mutex_init(&q[i].page_latch, NULL);
//        q[i].page_latch = PTHREAD_MUTEX_INITIALIZER;
//        q[i].pinCount = 0;
        q[i].buf_index = i;
        q[i].prev = -1;
        q[i].next = -1;
    }

    printf("::START DBMS::\n");
    return 0;

}

bool buf_check_table(int table_id) {

    if (tableList[table_id].isOpened) return true;
    else return false;

}

int get_table_id(char * pathname) {

    int table_id;
    string s_path(pathname);
//    printf("Get_table_id\n");
    if (!file_name_map.count(s_path)){
        table_id = (int)file_name_map.size() + 1;
        if (table_id >= MAX_TABLE) {
            printf("table id full\n");
            return -1;
        }

        file_name_map[s_path] = table_id;
    }
    else{
        table_id = file_name_map[s_path];
    }

//    printf("table_id %d\n", table_id);
    return table_id;

}

int open_buf_table(char* pathname) {

    int i, j, table_id;
    string s_path(pathname);
    // case 1: file is already exist
    int fd = open(pathname, O_RDWR, S_IRWXU);
    if (fd > 0) {
        table_id = get_table_id(pathname);
        if (table_id == -1) return -1;
        tableList[table_id].fd = fd;
        tableList[table_id].isOpened = true;
        tableList[table_id].tid = table_id;
        // read hp from disk and save to buffer

        return table_id;
    }
        // case 2: file is not exist. create new file
    else {
        printf("create new file start\n");
        fd = open(pathname, O_RDWR|O_CREAT|O_EXCL, 0664);
        if (fd == -1) return -1;

        table_id = get_table_id(pathname);
        if (table_id == -1) return -1;

        tableList[table_id].fd = fd;
        tableList[table_id].isOpened = true;
        tableList[table_id].tid = table_id;

        // init header

        page_t * header = make_page();

        header->h.free_pagenum = 0;
        header->h.root_pagenum = 0;
        header->h.num_pages = 1;
        buf_write_page(table_id, 0, header);
        free(header);
        printf("buffer write page header\n");

        printf("get_hp_index done\n");
        return table_id;
    }

}

int find_fd(int table_id) {

    int i, fd;
    for (i = 0; i < MAX_TABLE; i++) {
        if (tableList[i].tid == table_id) {
            fd = tableList[i].fd;
            break;
        }
    }
    return fd;

}

int get_tableList_tid(int table_id) {

    int tid = tableList[table_id].tid;
    return tid;

}

void set_tableList_tid(int table_id) {

    tableList[table_id].tid = table_id;

}

void init_tableList(int table_id) {

    tableList[table_id].fd = -1;
    tableList[table_id].tid = 0;
    tableList[table_id].isOpened = false;

}

// Write the pages relating to this table to disk and close the table.
int close_buf_table(int table_id) {

    if (buf_capacity == 0) {
        printf("There is no buf. Only close file\n");
        if (tableList[table_id].isOpened)
            close(tableList[table_id].fd);

        tableList[table_id].isOpened = false;
        return -1;
    }

//    printf("close buf table %d\n", table_id);
    int i;
    for (i = 0; i < buf_capacity; i++) {
        if (q[i].tid == table_id) {
            if (q[i].isDirty) {
                file_write_page(q[i].tid, q[i].pagenum, &q[i].frame);
            }

            // initialize buffer pool
            q[i].tid = -1;
            q[i].pagenum = -1;
            q[i].isDirty = false;
//            q[i].pinCount = 0;
        }
    }
    hashTable[table_id].clear();


//    printf("::Close table %d::\n", table_id);
    return 0;

}

int shutdown_buf(void) {

    if (buf_capacity == 0) {
        printf("shutdown_buf : buf is not set\n");
        return -1;
    }

    int i, table_id;
    for (i = 1; i < MAX_TABLE; i++) {
        close_buf_table(i);
        // initialize table list for next dbms
        init_tableList(i);
    }

    file_name_map.clear();
    free(q);
    buf_capacity = 0;
    printf("::SHUTDOWN DBMS::\n");
    return 0;

}

// buffer API
bool isFull() {
    bool ret = buf_capacity == num_frames ? true: false;
    return ret;
}
// this case is the frame is not in buffer pool
// so, read from disk and link it
// 1) if there has empty space, just insert it
// 2) else, do page eviction and return frame index
int get_LRU_index(int table_id, pagenum_t pagenum) {

//    printf("lru index\n");
    int i;
    // case 1: buffer pool has empty space
    if (!isFull()) {
//        printf("having empty space /");
        int p_index;
        p_index = num_frames;
        pthread_mutex_lock(&q[p_index].page_latch);
        pthread_mutex_unlock(&buffer_latch);

        q[p_index].tid = table_id;
        q[p_index].pagenum = pagenum;
        q[p_index].isDirty = false;
//        q[p_index].pinCount = 0;
        q[p_index].buf_index = num_frames;
        q[p_index].next = -1;
        q[p_index].prev = p_index - 1;
        num_frames++;
        pthread_mutex_unlock(&q[p_index].page_latch);

        return p_index;
    }
        // case 2: buffer is FULL - page eviction by LRU Policy
        // current is an index pointer
    else {
//        printf("is full / ");
        i = head;
        pthread_mutex_lock(&q[i].page_latch);
        pthread_mutex_unlock(&buffer_latch);
        int current = q[i].prev;
        pthread_mutex_unlock(&q[i].page_latch);

        while (true) {
            pthread_mutex_lock(&buffer_latch);

            // Check for duplicate lock
            // if there is a page latch from other thread,
            // trylock is failed so buffer latch have to relock.
            // (for re-line on waiting list)
            while (pthread_mutex_trylock(&q[current].page_latch) != 0) {
                pthread_mutex_unlock(&buffer_latch);
                pthread_mutex_lock(&buffer_latch);
            }
            pthread_mutex_unlock(&buffer_latch);
            // if buffer pool has empty space, return that index for insert
            if (q[current].tid == -1){
                pthread_mutex_unlock(&q[current].page_latch);
                return current;
            }

            // if the frame's lock is distinct(this lock only) and not a headerPage, select it for victim
            if(q[current].pagenum != 0){
                // if the frame has dirty page, flush
                if(q[current].isDirty == true){
                    file_write_page(table_id, pagenum, &q[current].frame);
                    q[current].isDirty = false;
                }

                // set hashtable's index to -1
                // -1 means Ith index's page is not in buffer pool.
                hashTable[table_id].erase(q[current].pagenum);
                // tableList[table_id].hashTable[q[i].pagenum] = -1;
                pthread_mutex_unlock(&q[current].page_latch);
                return current;
            }
            // move current point to prev
//            current = q[i].prev;
//            current = q[i].prev->buf_index;
            current = q[current].prev;
        }
    }
}


void link_list(int table_id, pagenum_t pagenum, page_t* page, int p_index) {

    int temp_prev, temp_next;
    if (head == -1) {
        head = p_index;
    }
    else {
        if (head == p_index) return;
        if (q[p_index].next == -1 || q[p_index].prev == -1) {
            temp_prev = q[head].prev;
            q[head].prev = p_index;
            q[temp_prev].next = p_index;
            q[p_index].next = head;
            q[p_index].prev = temp_prev;
            head = p_index;
        }
        else {
            temp_prev = q[p_index].prev;
            temp_next = q[p_index].next;
            q[temp_prev].next = temp_next;
            q[temp_next].prev = temp_prev;
            temp_prev = q[head].prev;
            q[head].prev = p_index;
            q[temp_prev].next = p_index;
            q[p_index].next = head;
            q[p_index].prev = temp_prev;
            head = p_index;
        }
    }
}

// return T/F for LRU Policy
bool isHeaderPage(int f_index) {

    bool isHeader = q[f_index].pagenum == 0 ? true: false;
    return isHeader;

}

// 1) get buffer index by insert to buffer
// 2) update hash table
int get_index_from_buffer(int table_id, pagenum_t pagenum) {

    int p_index;
    p_index = get_LRU_index(table_id, pagenum);
    hashTable[table_id][pagenum] = p_index;
    return p_index;

}

// read disk page to buffer
void buf_read_page(int table_id, pagenum_t pagenum, page_t* page) {

    if (buf_capacity == 0) {
        file_read_page(table_id, pagenum, page);
        return;
    }
//    printf("buf read page %d %lld\n", table_id, pagenum);

    int p_index;
    pthread_mutex_lock(&buffer_latch);

    if (hashTable[table_id].count(pagenum)) {
        p_index = hashTable[table_id][pagenum];
        pthread_mutex_lock(&q[p_index].page_latch);
//        q[p_index].pinCount++;
        memcpy(page, &q[p_index], sizeof(page_t));

        link_list(table_id, pagenum, page, p_index);
        pthread_mutex_unlock(&buffer_latch);
        pthread_mutex_unlock(&q[p_index].page_latch);
        return;
    }
    else {
        p_index = get_index_from_buffer(table_id, pagenum);
        pthread_mutex_lock(&q[p_index].page_latch);
        file_read_page(table_id, pagenum, page);
        // buffer setting
        memcpy(&q[p_index].frame, page, sizeof(page_t));
        q[p_index].tid = table_id;
        q[p_index].pagenum = pagenum;
        q[p_index].buf_index = p_index;
//        q[p_index].pinCount++;

        link_list(table_id, pagenum, page, p_index);
        pthread_mutex_unlock(&buffer_latch);
        pthread_mutex_unlock(&q[p_index].page_latch);
        return;
    }
}

void buf_write_page(int table_id, pagenum_t pagenum, page_t* page) {

    if (buf_capacity == 0) {
        file_write_page(table_id, pagenum, page);
        return;
    }
//    printf("buf write page %d %lld\n", table_id, pagenum);

    int p_index;
    pthread_mutex_lock(&buffer_latch);

    if (hashTable[table_id].count(pagenum)) {
        p_index = hashTable[table_id][pagenum];
//        printf("buf page p_index %d\n", p_index);

        pthread_mutex_lock(&q[p_index].page_latch);
        q[p_index].isDirty = true;
//        q[p_index].pinCount++;
        memcpy(&q[p_index].frame, page, sizeof(page_t));

        link_list(table_id, pagenum, page, p_index);
        pthread_mutex_unlock(&buffer_latch);
        pthread_mutex_unlock(&q[p_index].page_latch);
        return;
    }
    else {
        p_index = get_index_from_buffer(table_id, pagenum);
//        printf("lru buf page p_index %d\n", p_index);

        pthread_mutex_lock(&q[p_index].page_latch);
        // buffer setting
        memcpy(&q[p_index].frame, page, sizeof(page_t));
        q[p_index].tid = table_id;
        q[p_index].pagenum = pagenum;
        q[p_index].buf_index = p_index;
        q[p_index].isDirty = true;
//        q[p_index].pinCount++;

        link_list(table_id, pagenum, page, p_index);
        pthread_mutex_unlock(&buffer_latch);
        pthread_mutex_unlock(&q[p_index].page_latch);
        return;
    }
}

// for unpin
// find page index through hash table
//void unPin(int table_id, pagenum_t pagenum) {
//    if (buf_capacity == 0){
//        return;
//    }
//
//    int p_index = hashTable[table_id][pagenum];
//    q[p_index].pinCount--;
//}

void buf_print_hash() {

    printf("\n");
    for(int i = 1; i < MAX_TABLE;i++){
        map<pagenum_t, int> hash = hashTable[i];
        printf("id %d : ",i);
        for(auto h : hash){
            printf("(%lld, %d), ", h.first, h.second);
        }
        printf("\n");
    }
    printf("\n");

}

void buf_print_tree (int table_id) {
    queue <pagenum_t> qu;
    int i;
    page_t * page = make_page();
    page_t * hp = make_page();
    buf_read_page(table_id, 0, hp);

    pagenum_t root_pagenum = hp->h.root_pagenum;
    if (root_pagenum == 0) {
        printf("Tree is empty\n");
        free(page);
        free(hp);
        return;
    }
    qu.push(root_pagenum);
    printf("\n");

    while (!qu.empty()) {
        int temp_size = qu.size();

        while (temp_size) {
            pagenum_t pagenum = qu.front();
            qu.pop();

            buf_read_page(table_id, pagenum, page);
            if (page->g.isLeaf) {
                for (i = 0; i < page->g.num_keys; i++) {
                    printf("(%ld, %s) ", page->g.record[i].key, page->g.record[i].value);
                }
                printf(" | ");
            }

            else {
                if (page->g.num_keys > 0){
                    printf("[%lu] ", page->g.li_pagenum);
                    qu.push(page->g.li_pagenum);
                }

                for (i = 0; i < page->g.num_keys; i++) {
                    printf("%ld [%lu] ", page->g.internal_record[i].key, page->g.internal_record[i].pagenum);
                    qu.push(page->g.internal_record[i].pagenum);
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

void buf_print_page(int table_id, pagenum_t pagenum){
    int i, p_index;

    p_index = get_index_from_buffer(table_id, pagenum);
    printf ("parent number : %lu\n", q[p_index].frame.g.fp_pagenum);
    printf ("number of keys : %d\n", q[p_index].frame.g.num_keys);
    if (q[p_index].frame.g.isLeaf) {
        printf ("next leaf page number : %lu\n", q[p_index].frame.g.li_pagenum);
        for (i = 0; i < q[p_index].frame.g.num_keys; i++) {
            printf("%d | key : %ld , value : %s\n", i, q[p_index].frame.g.record[i].key, q[p_index].frame.g.record[i].value);
        }
    } else {
        printf ("most left child page number : %lu\n", q[p_index].frame.g.li_pagenum);
        for (i = 0; i < q[p_index].frame.g.num_keys; i++) {
            printf ("%d | key : %ld , child : %ld\n", i, q[p_index].frame.g.internal_record[i].key, q[p_index].frame.g.internal_record[i].pagenum);
        }
    }
}