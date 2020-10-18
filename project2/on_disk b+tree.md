# Project2_On-disk b+ tree

DBMS의 Layered architecture와 디스크 파일구조에 대해 이해하기 위한 프로젝트로, 기존의 in-Memory b+ tree를 on-Disk 방식으로 작동하도록 구현하였다. 지난 문서에서 b+ tree 코드 분석을 자세히 하였으므로, 본 문서에서는 b+ tree가 어떻게 동작하는지에 대한 설명보다 in-Memory구조와의 차이점을 중심으로 기술하였다.



프로젝트 실행 환경은 아래와 같다.

* Ubuntu 18.04.5 LTS
* gcc/g++ 7.5.0







## Design Overview

#### A. Layered architecture

![image-20201018064444373](/Users/SR/Desktop/image-20201018064444373.png)

1. main.c

   사용자가 명령어를 입력하면 그에 맞는 API Service를 호출

2. db.c

   함수 내부에서 index management layer에 구현된 b+ tree functions를 호출

```c
int open_table(char * pathname);
int db_insert(int64_t key, char * value);
int db_find(int64_t key, char * ret_val);
int db_delete(int64_t key);
```

3. bpt.c

   b+ tree 구조에 맞게 동작하며, 파일의 I/O가 필요할 때 file manager API를 호출

4. file.c

   disk의 I/O를 관리하는 file manager API

```c
page_t * make_page(void);
page_t * make_general_page(void);
page_t * make_header_page(void);
// Allocate an on-disk page from the free page list 
pagenum_t file_alloc_page();
// Free an on-disk page to the free page list
void file_free_page(pagenum_t pagenum);
// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(pagenum_t pagenum, page_t* dest);
// Write an in-memory page(src) to the on-disk page
void file_write_page(pagenum_t pagenum, const page_t* src);
```

* make_page

  * make_page: 메모리상에 할당만 해서 페이지를 읽어올 때 사용할 함수
  * make_general_page: leaf/internal/free 페이지에서 아예 새로운 페이지를 할당받아 디스크에 써야 할 경우
  * make_header_page: 맨 처음 header 페이지를 만들고 초기화할 경우

* file_alloc_page

  할당해 줄 프리 페이지가 남아 있는 경우와 없는 경우로 나누어 동작한다.

  1) 할당해 줄 프리 페이지가 있는 경우

  	- header page가 가리키는 free pagenum을 반환하여 해당 pagenum이 새 페이지로 쓰일 수 있게 한다.

  2) 더 이상 프리 페이지가 없는 경우

  - 파일을 새로 만들었을 때 / 기존 파일에서 모든 프리 페이지를 소진한 상태
  - 새로 쓸 pagenum을 계산하기 위해 현재 파일의 끝 byte(EOF)를 찾아 페이지 크기 단위로 나눈다.
  - 1MB의 free page를 만들기 위해 페이지를 새로 할당하고, 반복문을 돌며 각 페이지의 fp_pagenum(free or parent pagenum)을 자신의 오른쪽 프리페이지의 pagenum으로 설정하여 리스트를 연결한다. 가장 마지막 프리페이지의 fp_pagenum은 0으로 설정하여 프리 페이지의 끝을 알린다.
  - header page가 가리키는 free pagenum을 방금 만든 프리 페이지 리스트의 두 번째 pagenum으로 설정하고, 맨 앞의 pagenum(eof)을 반환하여 새 페이지로 쓰일 수 있게 한다.

* file_free_page

  많은 delete가 발생하여 디스크에 있던 페이지가 프리 페이지가 되었을 때 호출하는 함수이다.

  leaf / internal page에 맞게 각 레코드를 초기화시키고, 해당 페이지의 next free pagenum을 header page가 가리키는 free pagenum으로 설정해주어 프리 페이지 리스트에 연결한다.

* file_read/write_page

  입출력을 디스크립터로 할지, 포인터로 할지 고민하였다. 기존에는 파일 포인터를 이용하여 입출력을 처리하였으나, 처리 속도가 느리다는 단점이 있었다. 저수준 입출력 함수는 원래의 데이터를 세밀하게 조작할 수 있어 융통성이 뛰어나고 처리 속도가 빠르다는 장점이 있다. 따라서 저수준 입출력 함수인 read(), write() 와 lseek() 를 함께 사용하였다. write() 시에는 출력 결과가 바로 디스크에 반영될 수 있도록 sync() 를 함께 사용하였다.

  





#### B. Page structure

![image-20201018064659158](/Users/SR/Desktop/image-20201018064659158.png)

내가 작성한 코드가 남의 테스트 환경에서도 돌아가야 하므로, 명시된 disk page size와 record size에 맞춰 구현하였다.

크게 page_t 구조체 안에 union형 변수 headerPage, generalPage가 존재하며, 이 페이지도 각각 구조체 형태를 띈다.

```c
typedef struct page_t {
	union {
		headerPage h;
		generalPage g;
	};
}page_t;
```



headerPage는 특수한 페이지이므로 나머지 페이지들과 다른 구조로 설계하였다.

```c
typedef struct headerPage {
	pagenum_t free_pagenum;
	pagenum_t root_pagenum;
	int64_t num_pages;
	char reserved[4072]; //4096-24
}headerPage;
```



internal, leaf, free 페이지는 같은 byte 수만큼 나뉘어 있는 비슷한 구조이므로, union을 통해 레코드를 구분하여 internal page일 때와 leaf page일 때 모두 사용할 수 있도록 정의하였다.

* fp_pagenum:

  free page일 때 = next free pagenum

  internal/leaf page일 때 = parent pagenum

* li_pagenum:

  leaf page일 때 = right sibling pagenum

  internal page일 때 = one more pagenum

```c
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
```



페이지가 각각 leaf/internal page일 때 상황에 맞게 접근할 수 있도록 레코드 구조체를 정의하였다.

```c
typedef struct record {
	int64_t key;
	char value[120];
}record;

typedef struct internal_record {
	int64_t key;
	int64_t pagenum;
}internal_record;
```







#### C. Index

기존 in-memory b+ tree는 node 구조체를 선언하여 child node를 가리키는 데 pointer를 이용한 반면, 이번 on-disk b+ tree는 page 구조체 안의 pagenum 멤버를 통해 child page에 접근하는 차이가 있다.

특히, 각 bpt에서 leftmost page를 가리키는 데 가장 큰 차이가 있다.

pointer에서는 0번째가 가리키던 node였지만, 이번 b+tree에서는 internal page 맨 앞에 위치한 128byte짜리 page header 안의 li_pagenum(one more pagenum)이 leftmost page의 pagenum이다.

![image-20201018064842775](/Users/SR/Desktop/image-20201018064842775.png)





#### D. Delayed merge

tree structure modification을 줄이기 위해 페이지의 Key가 하나도 없을 경우에만 merge를 수행하도록 구현하였다. 그러나 leaf page에서 key가 하나도 없어 delayed merge(coalesce)가 발생할 경우 neighbor page가 가득 차 있는 상태였다면, 이후 이 이웃 페이지에 insert operation이 한 번이라도 들어오면 split이 또 발생하여 결과적으로는 2번의 tree structure modification이 발생하게 된다. 이런 경우 structure modification을 줄이기 위하여 redistribution이 필요하다고 판단하였다.

따라서 internal page에서 key가 존재하지 않아 delayed merge를 하는 경우에 속하나, capacity를 만족하지 못하여 merge를 수행하지 못하는 상황에서만 redistribution되도록 구현하였다. 기존의 redistribution은 neighbor page의 key를 한 개만 가져오지만, 이후 이 page에 delete operation이 들어와 다시 structure modification이 일어나는 경우를 방지하기 위해 (neighbor page의 Key 개수 / 2)개씩 가져오도록 하여 성능을 향상시켰다.

![image-20201018162803525](/Users/SR/Desktop/image-20201018162803525.png)







## b+ tree Functions

#### A. Insert

```c
int cut(int length);
int get_right_page_index(page_t * page, pagenum_t right_pagenum);
int insert_into_leaf_after_splitting(pagenum_t pagenum, int64_t key, char * value);
int insert_into_internal_after_splitting(pagenum_t pagenum, int right_index, int64_t key, pagenum_t right_pagenum);
int insert_into_parent(pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum);
int insert_into_new_root(pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum);
void make_root_page(int64_t key, char * value);
page_t * insert_into_leaf(page_t * page, int64_t key, char * value);
page_t * insert_into_internal(page_t * page, int right_index, int64_t key, pagenum_t right_pagenum);
```



#### B. Find

```c
pagenum_t find_leaf(int64_t key);
```

db_find에서 호출하여 internal page의 key값을 따라 leaf page까지 이동하는 함수이다. key값이 존재하는 페이지가 있으면 해당 pagenum을 반환한다.



#### C. Delete

```c
pagenum_t get_neighbor_pagenum(page_t * parent, pagenum_t pagenum);
page_t * remove_entry_from_page(int64_t key, pagenum_t pagenum);
int get_neighbor_index(page_t * parent, pagenum_t pagenum);
void adjust_root(pagenum_t pagenum);
void coalesce_pages(pagenum_t pagenum, pagenum_t parent_pagenum, pagenum_t neighbor_pagenum, int neighbor_index);
void redistribute_pages(pagenum_t pagenum, pagenum_t parent_pagenum, pagenum_t neighbor_pagenum, int neighbor_index);
void delete_entry(int64_t key, pagenum_t delete_pagenum);
```

get_neighbor_index 함수를 통해 coalesce와 redistribute 함수를 실행한다. 

페이지의 현재 위치에 따라 neighbor의 index와 neighbor pagenum이 달라짐에 유의한다. 페이지가 leftmost일 경우에는 오른쪽을 이웃으로, 나머지 경우에는 왼쪽을 이웃 페이지로 정의하였다.

![image-20201018064800373](/Users/SR/Desktop/image-20201018064800373.png)





## Results

open_table을 이용하여 sample.db 파일을 생성하고 테스트 케이스를 실행해보았다. 우선 반복문을 실행하며 999개의 정수가 insert됨을 확인할 수 있었다.

```c
            case 'i':
                for(i=1; i<1000; i++){
                    db_insert(i, "value");
                }
                printf("::AUTO INSERT 1 to 999::\n");
                print_tree();
                break;
```

1. insert 10000 value를 하면 성공적으로 값이 들어감을 확인할 수 있다.

2. insert 1 value를 하면 중복된 key가 존재하므로 error를 확인할 수 있다.

3. find 9999 value를 하면 해당 key를 찾아 return함을 확인할 수 있다.

   

또한, 1부터 999까지 홀수만을 삭제하는 함수를 실행하여 성공적으로 delete가 동작함을 확인할 수 있었다.

```c
            case 'b':
                for(i=1; i<1000; i = i+2){
                    db_delete(i);
                    print_tree();
                }
                printf("::AUTO DELETE ODD NUMBER\n");
                break;
```

![image-20201018180101426](/Users/SR/Desktop/4-1/image-20201018180101426.png)



leaf의 leftmost page가 삭제되면서 delete merge(coalesce)가 잘 동작하는지 확인하기 위해 첫 번째 leaf page의 모든 키를 삭제하고, 성공적으로 동작함을 확인할 수 있었다.

![image-20201018180447963](/Users/SR/Desktop/4-1/image-20201018180447963.png)



redistribution을 편하게 확인하기 위해 임의적으로 leaf/internal order를 3으로 변경하여 2, 4, 6, 8, 10을 삭제하였을 때, redistribution이 실행되는 것을 확인할 수 있었다.

![image-20201018181955497](/Users/SR/Desktop/4-1/image-20201018181955497.png)

