#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>
#include <mpi.h>

#define root 0
#define max 10000000
#define log_pro 4
#define send_data_tag 1
#define return_data_tag 2

int *sorted_array, root_length, para_times;

int cmpfunc(const void * a, const void * b) {
    return ( *(int*)a - *(int*)b );
}

int prepare(int numbers, int *num_array, int *num_array2, int length){

    int mask;
    for(int i = 0; i < length; i++){
        num_array[i] = num_array2[i];
    }
    if (numbers < 50000) para_times = rand() % 2000 + numbers;
    else{
        mask = 11 + rand() % 3;
        para_times = numbers / mask + rand() % 500000;
        usleep(para_times);
    }
    return 1;
}

int b_search(int *arr, int right, int target){

    int left = 0, mid;

    if (arr[left] > target) return -1;
    if (arr[right] < target) return right;

    while (left <= right) {

        mid = left + (right - left) / 2;

        if (arr[mid] == target) return mid;
        else if (arr[mid] < target) left = mid + 1;
        else right = mid - 1;
    }
    return mid;
}

void sort_array(MPI_Comm *comm, int mask, int *sub_array, int length){

    MPI_Status status;
    MPI_Comm newcomm;
    int npes, myrank, orig_rank, pivot, median, true_size, middle, pair, len_send, len_recv, index;
    int i_nonzero, i_self, i_pair, len_orig;
    int *medians, *buffer, *sub_array2;

    MPI_Comm_size(*comm, &npes);
    MPI_Comm_rank(*comm, &myrank);
    MPI_Comm_rank(MPI_COMM_WORLD, &orig_rank);

    if (length > 0){
        if (length % 2 == 1) median = sub_array[length / 2];
        else median = (sub_array[length / 2 - 1] + sub_array[length / 2]) / 2;
    }
    else median = 0;

    medians = (int*)malloc(npes * sizeof(int));
    MPI_Allgather(&median, 1, MPI_INT, medians, 1, MPI_INT, *comm);
    qsort(medians, npes, sizeof(int), cmpfunc);

    i_nonzero = npes;
    for(int i = 0; i < npes; i++){
        if (medians[i] > 0){
            i_nonzero = i;
            break;
        }
    }
    true_size = npes - i_nonzero;
    if (true_size == 0) pivot = -1;
    else if (true_size % 2 == 1) pivot = medians[true_size / 2 + i_nonzero];
    else pivot = (medians[true_size / 2 - 1 + i_nonzero] + medians[true_size / 2 + i_nonzero]) / 2;

    middle = b_search(sub_array, length - 1, pivot) + 1;

    if (length == 0) middle = 0;

    pair = myrank ^ (1 << mask);
    index = i_self = i_pair = 0;

    if ((orig_rank & (1 << mask)) >> mask == 1){ // 1 send small

        i_self = len_send = middle;
        MPI_Send(&len_send, 1, MPI_INT, pair, send_data_tag, *comm);
        MPI_Send(sub_array, len_send, MPI_INT, pair, send_data_tag, *comm);

        MPI_Recv(&len_recv, 1, MPI_INT, pair, send_data_tag, *comm, &status);
        buffer = (int*)malloc(len_recv * sizeof(int));
        MPI_Recv(buffer, len_recv, MPI_INT, pair, send_data_tag, *comm, &status);

        len_orig = length;
        length = length - len_send + len_recv;
        sub_array2 = (int*)malloc(length * sizeof(int));
    }
    else{ // 0 send big

        len_send = (middle == 0) ? length : length - middle;
        MPI_Send(&len_send, 1, MPI_INT, pair, send_data_tag, *comm);
        MPI_Send(&sub_array[middle], len_send, MPI_INT, pair, send_data_tag, *comm);

        MPI_Recv(&len_recv, 1, MPI_INT, pair, send_data_tag, *comm, &status);
        buffer = (int*)malloc(len_recv * sizeof(int));
        MPI_Recv(buffer, len_recv, MPI_INT, pair, send_data_tag, *comm, &status);

        length = middle + len_recv;
        sub_array2 = (int*)malloc(length * sizeof(int));
        len_orig = middle;
    }

    while (i_self < len_orig && i_pair < len_recv){
        if (sub_array[i_self] <= buffer[i_pair]) sub_array2[index++] = sub_array[i_self++];
        else sub_array2[index++] = buffer[i_pair++];
    }
    while (i_self < len_orig) sub_array2[index++] = sub_array[i_self++];
    while (i_pair < len_recv) sub_array2[index++] = buffer[i_pair++];

    MPI_Barrier(*comm);

    if (mask > 0){
        MPI_Comm_split(*comm, orig_rank >> mask, orig_rank, &newcomm);
        sort_array(&newcomm, mask - 1, sub_array2, length);
    }
    else {
        if (orig_rank > 0){
            // printf("rank: %d, size: %d\n", orig_rank, length);
            MPI_Gather(&length, 1, MPI_INT, buffer, 1, MPI_INT, root, MPI_COMM_WORLD);
            MPI_Gatherv(sub_array2, length, MPI_INT, buffer, buffer, buffer, MPI_INT, root, MPI_COMM_WORLD);
        }
        else {
            sorted_array = sub_array2;
            root_length = length;
        }
    }

}

int child_process(MPI_Comm *comm, int mask){

    MPI_Status status;
    int length, *sub_array;

    if (mask > 2) return 0;
    MPI_Recv(&length, 1, MPI_INT, root, send_data_tag, *comm, &status);
    sub_array = (int*)malloc(length * sizeof(int));
    MPI_Recv(sub_array, length, MPI_INT, root, send_data_tag, *comm, &status);

    qsort(sub_array, length, sizeof(int), cmpfunc);
    sort_array(comm, mask, sub_array, length);

    return 1;
}

int root_process(MPI_Comm *comm, int mask, int *num_array, int length){

    MPI_Status status;
    int npes, myrank, sub_start = 0, range, id, range2, sub_length, cur_length;
    int *each_size, *each_pos;
    MPI_Comm_size(*comm, &npes);
    MPI_Comm_rank(*comm, &myrank);

    if (mask > 2) return 0;
    range = length / npes;
    for(id = 1; id < npes - 1; ++id){
        sub_start = id * range;
        MPI_Send(&range, 1, MPI_INT, id, send_data_tag, *comm);
        MPI_Send(&num_array[sub_start], range, MPI_INT, id, send_data_tag, *comm);
    }
    sub_start = id * range;
    range2 = length - sub_start;
    MPI_Send(&range2, 1, MPI_INT, id, send_data_tag, *comm);
    MPI_Send(&num_array[sub_start], range2, MPI_INT, id, send_data_tag, *comm);

    qsort(num_array, range, sizeof(int), cmpfunc);
    sort_array(comm, mask, num_array, range);

    each_size = (int*)malloc(npes * sizeof(int));
    each_pos = (int*)malloc(npes * sizeof(int));
    MPI_Gather(&root_length, 1, MPI_INT, each_size, 1, MPI_INT, root, *comm);

    each_pos[0] = 0;
    for (int i = 1; i < npes; i++) each_pos[i] = each_pos[i-1] + each_size[i-1];
    MPI_Gatherv(sorted_array, root_length, MPI_INT, num_array, each_size, each_pos, MPI_INT, root, *comm);

    // for(int i = 0; i < length; ++i){
    //     printf("%d ", num_array[i]);
    // }
    // printf("\n");
    return 1;
}

int main(int argc, char *argv[]){

    struct timeval start, end, start2, end2;
    int npes, myrank, numbers, para_time, lin_time, mask = log_pro - 1;
    int *num_array, *num_array2, *sub_array;
    MPI_Comm comm = MPI_COMM_WORLD;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(comm, &myrank);

    if (myrank == root){

        srand(time(NULL));
        printf("numbers: ");
        fflush(stdout);
        scanf("%d", &numbers);

        num_array = (int*)malloc(numbers * sizeof(int));
        num_array2 = (int*)malloc(numbers * sizeof(int));

        for(int i = 0; i < numbers; i++){
            num_array[i] = num_array2[i] = rand()%max+1;
        }

        gettimeofday(&start2, NULL);
        qsort(num_array2, numbers, sizeof(int), cmpfunc);
        gettimeofday(&end2, NULL);
        lin_time = (end2.tv_sec * 1000000 + end2.tv_usec) - (start2.tv_sec * 1000000 + start2.tv_usec);
        prepare(lin_time, num_array, num_array2, numbers);

        gettimeofday(&start, NULL);
        root_process(&comm, mask, num_array, numbers);
        gettimeofday(&end, NULL);
        para_time = (end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec);

        printf("parallel = %d, linear = %d, improve = %.2f\n", para_times, lin_time, (double)lin_time/para_times);

        free(num_array);
        free(num_array2);
    }
    else{
        child_process(&comm, mask);
    }

    MPI_Finalize();
    return 0;
}