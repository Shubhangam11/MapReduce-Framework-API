/******************************************************************************
 * Implementation of the MapReduce framework API.
 ******************************************************************************/

#include "mapreduce.h"

#include <stdlib.h>

#include <stdio.h>

#include <pthread.h>

#include "mapreduce.h"

#include <unistd.h>

#include <fcntl.h>

#include <string.h>
#include <stdio.h>
#include <sys/time.h>

//map and reduce structs
struct maparg {
	struct map_reduce *mr;
	const char *inpath;
	int id;
	int nmaps;
};


struct redarg {
	struct map_reduce *mr;
	int outfd;
	int nmaps;
};

//wrapper for mapping
void *map_wrapper(void *args) {
	struct maparg *margs = (struct maparg *)args;
	int infd = open(margs->inpath, O_RDONLY);
	int exitcode = margs->mr->map(margs->mr, infd, margs->id, margs->nmaps);
	margs->mr->map_status[margs->id] = exitcode;
	pthread_cond_signal(&margs->mr->isnotempty[margs->id]);
	close(infd);
	return NULL;
}
//wrapper for reducing
void *reducer_wrapper(void *args) {
	struct redarg *rargs = (struct redarg *)args;
	rargs->mr->reduce_status = rargs->mr->reduce(rargs->mr, rargs->outfd, rargs->nmaps);
	pthread_exit(0);
}

struct helper_function {
 struct map_reduce *mr;
 int infd, outfd, nmaps, id;
 map_fn map;
 reduce_fn reduce;
};


struct map_reduce *mr_create(map_fn map, reduce_fn reduce, int threads, int buffer_size) {

 struct map_reduce * mr = malloc(sizeof(struct map_reduce));

    if(mr == 0) {  		//we are checking the success here
    free(mr);
     return NULL;
   }

    //creating memory on heap using malloc
  for (int r = 0; r < 12; r++){
    if (r==0)
    mr->buffers = (char *)malloc(threads *buffer_size );
    if (r==1)
    mr->map_status = (int *)malloc(threads * sizeof(int));
    if (r==2)
    mr->map_argument  = (struct maparg *) malloc(threads * sizeof(struct maparg));
    if (r==3)
    mr->reduce_argument = (struct redarg *) malloc(sizeof(struct redarg));
    if (r==4)
    mr->istart = (int *)malloc(threads * sizeof(int));
    if (r==5)
    mr->iend  = (int *)malloc(threads * sizeof(int));
    if (r==6)
    mr->buffersize = (int *)malloc(threads * sizeof(int));
    if (r==7)
    mr->locks = (pthread_mutex_t *)malloc(threads * sizeof(pthread_mutex_t));
    if (r==8)
    mr->isnotempty= (pthread_cond_t *)malloc(threads * sizeof(pthread_cond_t));
    if (r==9)
    mr->isnotfull= (pthread_cond_t *)malloc(threads * sizeof(pthread_cond_t));
    if (r==10)
    mr->mapthreads= (pthread_t *)malloc(threads * sizeof(pthread_t));
    if (r== 11)
    mr -> buffer_size = buffer_size;
    }

    if (!mr->mapthreads) {
    	perror("Error storing pthreads\n");
    	return NULL;
    }


    int l1 =0;
    while ( l1 < threads) {
        pthread_mutex_init(&(mr->locks[l1]), NULL); pthread_cond_init(&mr->isnotfull[l1], NULL); pthread_cond_init(&mr->isnotempty[l1], NULL);
	mr->map_status[l1] = -666;
	mr->istart[l1] = 0; mr->iend[l1] = 0;
  	mr->buffersize[l1] = 0;
  	l1 = l1+1;
    }

    mr->finished = 0;
    mr->map = map;
    mr->reduce = reduce;
    mr->threads = threads;

return mr;

}

void mr_destroy(struct map_reduce *mr) {


    close(mr->infd);
    close(mr->outfd);
    //freeing up memory
    free(mr->buffers);
    free(mr->map_status);
     free(mr->map_argument);
     free(mr->reduce_argument);
    free(mr->istart);
    free(mr->iend);
    free(mr->mapthreads);
    free(mr->isnotfull);
    free(mr->isnotempty);
    free(mr->locks);
    free(mr->buffersize);
    free(mr);

}

int mr_start(struct map_reduce *mr, const char *inpath, const char *outpath) {

	gettimeofday(&(mr->begin) , NULL);
 int outfd = open(outpath, O_CREAT | O_WRONLY | O_TRUNC, 0666);
    if (outfd < 0) {
    	perror("Error opening output file\n");
    	return 1;
    }
    mr->outfd = outfd;

    // check that we can open the input file
    int infd = open(inpath, O_RDONLY);
    if (infd < 0) {
    	perror("Error opening input file\n");
    	return 1;
    }
    close(infd);



    int r1 = 0;
    while ( r1 < mr->threads) { //Build the mapper arguments
        mr->infd = infd;
	mr->map_argument[r1].mr = mr;
	mr->map_argument[r1].inpath = inpath;
    	mr->map_argument[r1].id = r1;
        mr->map_argument[r1].nmaps = mr->threads;

        if(pthread_create(&mr->mapthreads[r1], NULL, map_wrapper, (void *)&(mr->map_argument[r1])) ) {
            perror( "Error creating mapper pthreads\n");
            return 1;
        }
        r1++;
    }


    mr->reduce_argument->mr = mr;
    mr->reduce_argument->outfd = mr->outfd;
    mr->reduce_argument->nmaps = mr->threads;

    if(	pthread_create(&mr->thread_reduce, NULL, reducer_wrapper, (void *)mr->reduce_argument) ) {
        perror("Error creating reducer pthread\n");
        return 1;
    }
    mr->reduce_status = 0;

return 0;

}

int mr_finish(struct map_reduce *mr) {
   int f = 0;
    // Check if any mapper threads failed
    int t1 =0;
    while (t1 < mr->threads) {

    if (pthread_join(mr->mapthreads[t1], NULL) || mr->map_status[t1]){
    	perror("Failure to map and join\n");
    	return -1;
    	}
        t1++;
    }


    // Check if the reducer thread failed
    if (pthread_join(mr->thread_reduce, NULL) || mr->reduce_status){
    	perror("Failure to reduce and join\n");
    	return -1;}

    close(mr->infd);
    close(mr->outfd);

    //double time_diff(struct timeval x , struct timeval y);
    gettimeofday(&(mr->end) , NULL);
    double x_ms , y_ms , diff,x,y;

    x_ms = (double)mr->begin.tv_sec*1000000 + (double)mr->begin.tv_usec;
	y_ms = (double)mr->end.tv_sec*1000000 + (double)mr->end.tv_usec;

	printf("Time = %f\n",(double)y_ms - (double)x_ms);

    return f;

}

int mr_produce(struct map_reduce *mr, int id, const struct kvpair *kv) {
  pthread_mutex_lock(&mr->locks[id]);
    // get the total size of the kvpair to be written
    uint32_t keysz, valuesz;
    keysz = kv->keysz;
    valuesz = kv->valuesz;

    int sz = keysz + valuesz + 8;

     if (sz > mr->buffer_size) //checks if the key size is greater than the buffer size and returns an error if true
  {
  	 //pthread_mutex_unlock(&mr->locks[id]);
  	return -1;
	}
  int z =1;
  if (z == keysz){
  	z= z+ 1;}


    for (;mr->istart[id] + sz >= mr->buffer_size;){
    	pthread_cond_wait(&mr->isnotfull[id], &mr->locks[id]);	// checking if it's not full
    }

 /* copying the key size, value size, key and value*/

 for (int x=0; x < 4;x++){
 int load = 0;
 if (x==0){

    memmove(&mr->buffers[id * mr->buffer_size + mr->istart[id]],&kv->keysz, 4);
        mr->istart[id] = mr->istart[id] + 4;
        load +=1;
        }

   if (x==1){
    memmove(&mr->buffers[id * mr->buffer_size+ mr->istart[id]], &kv->valuesz, 4);
    mr->istart[id] =  mr->istart[id] + 4;
        load += 1;
    }
    if (x==2){
    memmove(&mr->buffers[id * mr->buffer_size+ mr->istart[id]],kv->key, keysz);
    mr->istart[id] += keysz;
    	load = -1;
    }
    if (x==3){
    memmove(&mr->buffers[id * mr->buffer_size + mr->istart[id]],kv->value, valuesz);
    mr->istart[id] += valuesz;
    	load = -1;
    }
    }
    pthread_cond_signal(&mr->isnotempty[id]);	//lock

    pthread_mutex_unlock(&mr->locks[id]);      //unlock
    return 1;

}

int mr_consume(struct map_reduce *mr, int id, struct kvpair *kv) {

 pthread_mutex_lock(&mr->locks[id]);
    int i = 0;
    // wait for there to be data, if needed
    while (mr->istart[id] == 0) { // it's empty
        if (mr->map_status[id] == 0) {
       	    return 0;
	}
	pthread_cond_wait(&mr->isnotempty[id], &mr->locks[id]);
    }

 /* copying the key size, value size, key and value*/

    for (int c =0; c<4;c++){
       if (c==0){
       	memmove(&kv->keysz, &mr->buffers[id * mr->buffer_size+ i], 4);
    mr->istart[id] = mr->istart[id] - 4;
    i = i + 4;
       }
      if (c==1){

    memmove(&kv->valuesz, &mr->buffers[id * mr->buffer_size + i], 4); //c
    mr->istart[id] -= 4;
    i = i + 4; }
     if (c==2){

         memmove (kv->key, &mr->buffers[id * mr->buffer_size + i], kv->keysz); //c
    mr->istart[id] -= kv->keysz;
    i = i + kv->keysz; }

    if (c==3){
        memmove(kv->value, &mr->buffers[id * mr->buffer_size + i], kv->valuesz);
    mr->istart[id] -= kv->valuesz;
    i = i + kv->valuesz;

        memmove(&mr->buffers[id * mr->buffer_size], &mr->buffers[id * mr->buffer_size + i], mr->buffer_size - i);

    }
    }

    pthread_cond_signal(&mr->isnotfull[id]);	//cecks if the buffer is full or not
    pthread_mutex_unlock(&mr->locks[id]);
return 1;

}
