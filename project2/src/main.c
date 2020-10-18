#include "db.h"
#include "db.c"
#include <time.h>

// MAIN
int main() {
    int64_t input;
    int i;
    int tid;
    char instruction;
    char buf[120];
    char pathname[100];
    char *result;
    pagenum_t inp_page;

    usage_1();
    printf("> ");
    while(scanf("%c", &instruction) != EOF){
        switch(instruction){
            case 'o':
                scanf(" %[^\n]", pathname);
                tid = open_table(pathname);
                if (tid != -1) {
                    printf ("open_table success.\ntid: %d\n", tid);
                } else {
                    perror ("file open failed.\n");
                    return -1;
                }
                break;
            case 'i':
                for(i=1; i<1000; i++){
                    db_insert(i, "value");
                }
                printf("::AUTO INSERT 1 to 999::\n");
                // print_tree();
                break;
            case 'j':
                scanf("%ld %s", &input, buf);
                if (db_insert(input, buf)) {
                    printf("::INSERT FAILED::\n");
                }
                // print_tree();
                break;
            case 'f':
                scanf("%ld %s", &input, buf);
                if (db_find (input, buf)) {
                    printf("the key is not in database.\n");
                }
                // print_tree();
                break;
            case 'd':
                scanf("%ld", &input);
                if (!db_delete(input)) {
                    printf("::DELETION %ld SUCCESS\n", input);
                }
                // print_tree();
                break;
            case 'b':
                for(i=1; i<1000; i = i+2){
                    db_delete(i);
                }
                printf("::AUTO DELETE ODD NUMBER\n");
                // print_tree();
                break;
            case 'p':
                scanf("%lu", &inp_page);
                // print_page(inp_page);
                break;
            case 'q':
                while (getchar() != (int)'\n');
                return EXIT_SUCCESS;
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