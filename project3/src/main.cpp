#include "../include/db.h"

// MAIN
int main() {
    int64_t input;
    int i, tid, size;
    char instruction;
    char buf[120];
    char pathname[100];
    pagenum_t inp_page;

    usage_1();
    printf("> ");
    while(scanf("%c", &instruction) != EOF){
        switch(instruction){
            case 'x':
                print_tree(tid);
                break;
            case 's':
                scanf(" %d", &size);
                init_db(size);
                break;
            case 'n':
                buf_print_hash();
//                printPoolContent();
                break;
            case 'o':
                scanf(" %[^\n]", pathname);
                tid = open_table(pathname);
                if (tid != -1) {
                    printf ("open_table success.\ntid: %d\n", tid);
                } else {
                    perror ("file open failed.\n");

                }
                printTable();
                break;
            case 't':
                printTable();
                break;
            case 'c':
                scanf("%d", &tid);
                close_table(tid);
                printf("::CLOSE TABLE %d::\n", tid);
                break;
            case 'i':
                scanf("%d %ld %s", &tid, &input, buf);
                if (db_insert(tid, input, buf)) {
                    printf("::INSERT FAILED::\n");
                }
                printf("::INSERT %ld SUCCESS::\n", input);
                print_tree(tid);
                break;
            case 'j':
                scanf("%d", &tid);
                for(i=1; i<50; i++){
                    db_insert(tid, i, "value");
                }
                printf("::AUTO INSERT 1 to 999::\n");
                print_tree(tid);
                break;
            case 'f': {
                scanf("%d %ld %s", &tid, &input, buf);
                char *val = (char *) malloc(sizeof(char) * 120);
                if (db_find(tid, input, buf)) {
                    printf("the key is not in database.\n");
                }
                printf("::FIND RESULT at TABLE %d::Key: %lld, value: %s\n", tid, input, val);
                free(val);
                // print_tree();
                break;
            }
            case 'd':
                scanf("%d %ld", &tid, &input);
                if (!db_delete(tid, input)) {
                    printf("::DELETION %ld SUCCESS\n", input);
                    print_tree(tid);
                }
                 print_tree(tid);
                break;
            case 'b':
                scanf("%d", &tid);
                for(i=1; i<50; i = i+2){
                    db_delete(tid, i);
                    print_tree(tid);
                }
                printf("::AUTO DELETE ODD NUMBER\n");
                print_tree(tid);
                // print_tree();
                break;
            case 'p':
//                scanf("%d %lu", &tid, &inp_page);
//                print_page(tid, inp_page);
                scanf("%d", &tid);
                print_tree(tid);
                break;
            case 'l':
                scanf("%d", &tid);
                buf_print_tree(tid);
                break;

            case 'q':
                shutdown_db();
                break;
            default:
                usage_1();
                break;
        }
        while (getchar() != (int)'\n');
        printf("> ");
    }
    printf("\n");

    return 0;
}
