#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <math.h>
#include <limits.h>

//#include "float_vec.c"
#include "float_vec.h"
#include "barrier.h"
#include "utils.h"
//#include "utils.c"
//#include "barrier.c"

// comparator function for qsort function.
int comparator (const void * a, const void * b)
{
  float fa = *(const float*) a;
  float fb = *(const float*) b;
  return (fa > fb) - (fa < fb);
}

//qsort for floats which in turn uses inbuilt qsort function in C
void
qsort_floats(floats* xs)
{
    qsort(xs->data, xs->size, sizeof(float), comparator);
}

//This function chooses 3*(P-1) items randomly and puts it into a floats structure and returns it
floats*
sample(float* data, long size, int P)
{
    floats* sample;
    sample = make_floats(3*(P-1));
    //choose 3*(P-1) values, store it in sample, and sort it.
    for(int i=0; i<3*(P-1); i++)
    {
       int r = rand()%size;
       sample->data[i] = data[r]; 
    }
    
    return(sample);
}

void
sort_worker(int pnum, float* data, long size, int P, floats* samps, long* sizes, barrier* bb)
{
    // Copy the data for our partition into a locally allocated array.
    
    //local array.
    floats* xs;
    xs = make_floats(0);

    //push the data into the local array if the conditions match
    for(int i=0; i<size; i++)
    {
        if(data[i]>=samps->data[pnum] && data[i]<samps->data[pnum+1])
            floats_push(xs, data[i]);
    }
    sizes[pnum] = xs->size;
// data for the partition is copied into the local array.

    printf("%d: start %.04f, count %ld\n", pnum, samps->data[pnum], xs->size);
    
    // local array is sorted
    qsort_floats(xs);

    barrier_wait(bb);  //wait for all the process to update the sizes array as each process would need the result of its previous process' sizes value.
    // Using the shared sizes array, determine where the local output goes in the global data array.
    int start = 0;
    int end = 0;
    for(int i=0; i<pnum; i++)
        start = start + sizes[i];
    for(int i=0; i<=pnum; i++)
        end = end + sizes[i];

    // Copy the local array to the appropriate place in the global array.
    int k = 0;
    for(int i=start; i<end; i++)
    {
        data[i] = xs->data[k];
        k++;
    }
    free_floats(xs);
    exit(0);
}

// this function creates process and calls the sort worker function
void
run_sort_workers(float* data, long size, int P, floats* samps, long* sizes, barrier* bb)
{
    pid_t kids[P];
    // fork P number of process
    for(int i=0; i<P; i++)
    {
        if((kids[i]=fork()))
        { //parent
            }
        else
        {
            sort_worker(i, data, size, P, samps, sizes, bb);
        }
    }
    //Once all P processes have been started, wait for them all to finish.
    for (int pp = 0; pp < P; ++pp) {
        waitpid(kids[pp], 0, 0);
    }
}

void
sample_sort(float* data, long size, int P, long* sizes, barrier* bb)
{
    floats* samples;
    floats* temp;
    temp = sample(data, size, P);
    //temp will now contain 3*(P-1) elements

    samples = make_floats(P+1); //samples is allocates space using the make_floats function to store P+1 values (splitters).
    
    //sort the temp floats sequentially.
    qsort_floats(temp);

    //now create the samples floats that contains P+1 slitter values.
    samples->data[0] = 0;
    samples->data[P] = INT_MAX;
    //after sorting, the medain values are stored in a floats structure.
    int j=1;
    for(int i=1; i<3*(P-1); i=i+3)
    {
        samples->data[j] = temp->data[i];
        j++;
    }
    //once the splitters are ready, call sort workers.
    run_sort_workers(data, size, P, samples, sizes, bb);
    free_floats(samples);
    free_floats(temp);
}

int
main(int argc, char* argv[])
{    if (argc != 3) {
        printf("Usage:\n");
        printf("\t%s P data.dat\n", argv[0]);
        return 1;
    }

    const int P = atoi(argv[1]);
    const char* fname = argv[2];
    struct stat buf;
    seed_rng();

    int fd = open(fname, O_RDWR);
    check_rv(fd);

    int rv = fstat(fd, &buf);
    check_rv(rv);

    //map the file in the virtual address space and the address of the mapping is returned in the void* file.
    void* file = mmap(NULL, buf.st_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if(file == MAP_FAILED)
    {
       perror("error in mmap: ");
        return(1);
    }

    //The count value and the float data are read from the file and stored in the variable count and data pointer respectively.
    long count = *(long*)file;
    float* data = (float*)(file+8);

    long sizes_bytes = P * sizeof(long);
//    sizes is a shared array.
    long* sizes = mmap(NULL, sizes_bytes, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANONYMOUS, -1, 0);
    barrier* bb = make_barrier(P);

    sample_sort(data, count, P, sizes, bb);

    free_barrier(bb);

    //Clean up resources.

    rv = munmap(sizes, sizeof(long));
    check_rv(rv);
    rv = munmap(file, buf.st_size);
    check_rv(rv);

    return 0;

 }
