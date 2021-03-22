#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>

#define MAX_SIZE 100000000

typedef struct PartArgs{
    int start_id, end_id, left, right;
}Part_args;

typedef struct Arguments{

    /*
    left_bound:     left bound of the thread on the array
    right_bound:    right bound of the thread on the array
    start_idx:      index of first thread in this partition
    end_idx:        index of last thread in this partition
    pivot:          pivot in this partition
    thread_idx:     index of this thread
    left_ptr:       pointer for swapping
    pre_lsum:       previous less (<=pivot) sum (not used)
    cur_lsum:       current less (<=pivot) sum
    pre_gsum:       previous greater (>pivot) sum (not used)
    cur_gsum:       current greater (>pivot) sum
    fbegin:         index of filling first less number
    sbegin:         index of filling first greater number
    fnums:          number of less numbers in this thread
    snums:          number of greater numbers in this thread
    mypart:         numbers from left bound to right bound
    barrier:        barrier for this thread
    */
    int left_bound, right_bound, start_idx, end_idx, pivot;
    int thread_idx, left_ptr, cur_lsum, cur_gsum;
    int pre_lsum, pre_gsum;
    int fbegin, sbegin, fnums, snums;
    int *mypart;
    pthread_barrier_t *barrier;
}Args;

Args *args;
int *myarray, *myarray2;    // myarray: for parallel
pthread_t *workers;         // threads
pthread_mutex_t *locks;     // locks
pthread_cond_t *conds;      // condition variables

// compare function for qsort
int cmpfunc(const void * a, const void * b) {
   return ( *(int*)a - *(int*)b );
}

// local + global rearrangement function
void *rearrange(void *arguments){

    Args *arg = (Args*) arguments;
    int diff = 1, temp;

    // local rearrangement: when encounter a number <= pivot, swap with left_ptr++
    for(int i = arg->left_bound; i <= arg->right_bound; ++i){
        if (myarray[i] <= arg->pivot){
            if (arg->left_ptr < i){
                temp = myarray[arg->left_ptr];
                myarray[arg->left_ptr] = myarray[i];
                myarray[i] = temp;
            }
            arg->left_ptr++;
        }
    }

    // count fnums and snums
    arg->fnums = arg->cur_lsum = arg->left_ptr - arg->left_bound;
    arg->snums = arg->cur_gsum = arg->right_bound - arg->left_bound + 1 - arg->fnums;

    // copy rearranged partial array to mypart: [left_bound,right_bound]
    // if (arg->right_bound - arg->left_bound + 1 < MAX_SIZE){
    //     printf("error\n");
    // }
    for(int i = arg->left_bound; i <= arg->right_bound; ++i) arg->mypart[i - arg->left_bound] = myarray[i];

    // wait other threads
    pthread_barrier_wait(&(*(arg->barrier)));

    // // calculate number of threads
    // int times = arg->end_idx - arg->start_idx + 1;

    // // prefix sum for less and greater
    // while(times > diff){

    //     // update previous sums
    //     arg->pre_lsum = arg->cur_lsum;
    //     arg->pre_gsum = arg->cur_gsum;
    //     pthread_barrier_wait(&(*(arg->barrier)));

    //     // update current sums
    //     if (arg->thread_idx >= diff + arg->start_idx){
    //         arg->cur_lsum = args[arg->thread_idx - diff].pre_lsum + arg->pre_lsum;
    //         arg->cur_gsum = args[arg->thread_idx - diff].pre_gsum + arg->pre_gsum;
    //     }
    //     pthread_barrier_wait(&(*(arg->barrier)));
    //     diff *= 2;
    // }
    // pthread_barrier_wait(&(*(arg->barrier)));

    for (int j = arg->start_idx; j < arg->thread_idx; ++j){
        arg->cur_lsum += args[j].fnums;
        arg->cur_gsum += args[j].snums;
    }
    // condition wait
    pthread_barrier_wait(&(*(arg->barrier)));

    // caiculate two begins
    if (arg->thread_idx > arg->start_idx){
        arg->fbegin += args[arg->thread_idx - 1].cur_lsum;
        arg->sbegin += args[arg->end_idx].cur_lsum + args[arg->thread_idx - 1].cur_gsum;
    }
    else arg->sbegin += args[arg->end_idx].cur_lsum;

    // global rearrangement
    // if (arg->fbegin + arg->fnums > MAX_SIZE){
    //     printf("First:  %d, %d\n", arg->fbegin, arg->fnums);
    // }
    for(int j = arg->fbegin; j < arg->fbegin + arg->fnums; ++j) myarray[j] = arg->mypart[j - arg->fbegin];

    // if (arg->sbegin + arg->snums > MAX_SIZE){
    //     printf("Second: %d, %d\n", arg->sbegin, arg->snums);
    // }
    for(int j = arg->sbegin; j < arg->sbegin + arg->snums; ++j) myarray[j] = arg->mypart[j - arg->sbegin + arg->fnums];

    return NULL;
}

int partition(int start_id, int end_id, int left, int right){

    int numbers = right - left + 1;
    int threads = end_id - start_id + 1;
    int range = numbers / threads;
    int remain = numbers % threads;
    int com_pivot = myarray[rand()%numbers+left];
    int last_right = left;
    pthread_barrier_t local_barr, *ptr_barr;
    pthread_barrier_init(&local_barr, NULL, threads);
    ptr_barr = &local_barr;

    // range = 12 * 4 when 100/8
    for (int i = start_id; i <= end_id - remain; i++) {
        args[i].thread_idx = i;
        args[i].left_bound = last_right;
        args[i].right_bound = last_right + range - 1;
        args[i].left_ptr = args[i].left_bound;
        args[i].pivot = com_pivot;
        args[i].pre_lsum = 0;   // not used
        args[i].cur_lsum = 0;
        args[i].pre_gsum = 0;   // not used
        args[i].cur_gsum = 0;
        args[i].fbegin = left;
        args[i].sbegin = left;
        args[i].start_idx = start_id;
        args[i].end_idx = end_id;
        args[i].barrier = ptr_barr;
        last_right = args[i].right_bound + 1;
        pthread_create(&workers[i], NULL, rearrange, &args[i]);
    }
    // range = 13 * 4 when 100/8
    for (int i = end_id - remain + 1; i <= end_id; i++) {
        args[i].thread_idx = i;
        args[i].left_bound = last_right;
        args[i].right_bound = last_right + range;
        args[i].left_ptr = args[i].left_bound;
        args[i].pivot = com_pivot;
        args[i].pre_lsum = 0;   // not used
        args[i].cur_lsum = 0;
        args[i].pre_gsum = 0;   // not used
        args[i].cur_gsum = 0;
        args[i].fbegin = left;
        args[i].sbegin = left;
        args[i].start_idx = start_id;
        args[i].end_idx = end_id;
        args[i].barrier = ptr_barr;
        last_right = args[i].right_bound + 1;
        pthread_create(&workers[i], NULL, rearrange, &args[i]);
    }

    for (int i = start_id; i <= end_id; i++) {
        pthread_join(workers[i], NULL);
    }
    return args[start_id].sbegin;
}

// need fix 1
void *quicksort(void *p_args){

    Part_args *args = (Part_args*) p_args;
    int threads =  args->end_id - args->start_id + 1;

    if (threads > 1){

        int middle = partition(args->start_id, args->end_id, args->left, args->right);

        // if (middle > args->right) quicksort(args);

        int left_threads = (middle - args->left) * threads / (args->right - args->left + 1);
        int right_threads = args->end_id - args->start_id + 1 - left_threads;

        if (left_threads == 0){
            left_threads++;
            right_threads--;
        }
        else if (right_threads == 0){
            left_threads--;
            right_threads++;
        }

        Part_args left_args, right_args;
        pthread_t manager;

        right_args.start_id = args->start_id + left_threads;
        right_args.end_id = args->end_id;
        right_args.left = middle;
        right_args.right = args->right;
        pthread_create(&manager, NULL, quicksort, &right_args);

        left_args.start_id = args->start_id;
        left_args.end_id = args->start_id + left_threads - 1;
        left_args.left = args->left;
        left_args.right = middle - 1;
        quicksort(&left_args);
        pthread_join(manager, NULL);
    }
    else {
        qsort(myarray + args->left, args->right - args->left + 1, sizeof(int), cmpfunc);
    }
}

void init_arrays(int numbers, int threads){

    myarray = (int*)malloc(numbers * sizeof(int));
    myarray2 = (int*)malloc((numbers + 1) * sizeof(int));
    for (int i = 0; i < numbers; i++){
        myarray[i] = rand()%100000000+1;
        myarray2[i+1] = myarray[i];
    }
    // int room = numbers / threads * 3;
    workers = (pthread_t*)malloc(threads * sizeof(pthread_t));
    locks = (pthread_mutex_t*)malloc(threads * sizeof(pthread_mutex_t));
    conds = (pthread_cond_t*)malloc(threads * sizeof(pthread_cond_t));
    args = (Args*)malloc(threads * sizeof(Args));

    for (int i = 0; i < threads; i++){
        pthread_mutex_init(&locks[i], NULL);
        pthread_cond_init(&conds[i], NULL);
        args[i].mypart = (int*)malloc(numbers * sizeof(int));
    }
}

void free_all(int threads){

    free(myarray);
    free(myarray2);
    free(workers);
    free(locks);
    free(conds);
    free(args);
}

int main(int argc, char *argv[]){

    struct timeval start, end, start2, end2;
    int numbers, threads;
    int para_time, lin_time;

    srand(time(NULL));
    printf("numbers: ");
    int value1 = scanf("%d", &numbers);
    printf("threads: ");
    int value2 = scanf("%d", &threads);

    if (numbers <= 0 || threads <= 0) return 0;
    init_arrays(numbers, threads);

    // for(int i = 0; i < numbers; ++i){
    //     printf("%d ", myarray[i]);
    // }
    // printf("\n");

    gettimeofday(&start, NULL);

    if (numbers <= threads) qsort(myarray, numbers, sizeof(int), cmpfunc);
    else {
        Part_args p_args;
        p_args.start_id = 0;
        p_args.end_id = threads - 1;
        p_args.left = 0;
        p_args.right = numbers - 1;
        quicksort(&p_args);
    }

    gettimeofday(&end, NULL);

    // for(int i = 0; i < numbers; ++i){
    //     printf("%d ", myarray[i]);
    // }
    // printf("\n");

    para_time = (end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec);
    // printf("%d\n", para_time);

    gettimeofday(&start2, NULL);

    qsort(myarray2 + 1, numbers, sizeof(int), cmpfunc);

    gettimeofday(&end2, NULL);

    lin_time = (end2.tv_sec * 1000000 + end2.tv_usec) - (start2.tv_sec * 1000000 + start2.tv_usec);

    printf("parallel = %d, linear = %d, improve = %.2f\n", para_time, lin_time, (double)lin_time/para_time);

    free_all(threads);

    return 0;
}