#ifndef BUILD_UFX_HASH_H
#define BUILD_UFX_HASH_H

#include <stdio.h>
#include <sys/stat.h>
#include <assert.h>

#include "meraculous.h"
#include "packingDNAseq.h"
#include "kmer_hash.h"
#include "wtimer.h"

#ifdef PAPYRUSKV
int cswap_used_flag(const char* key, size_t keylen, char** val, size_t* vallen, void* userin, size_t userinlen, void* userout, size_t useroutlen) {
    list_t* list = *((list_t**) val);
    if (useroutlen == sizeof(int))
        *((int*) userout) = list->used_flag;
    else if (useroutlen == sizeof(int64_t))
        *((int64_t*) userout) = list->used_flag;
    if (list->used_flag == UNUSED) {
        list->used_flag = USED;
        memcpy((void*) &(list->my_contig), userin, userinlen);
        return 1;
    }
    return 0;
}
#endif /* PAPYRUSKV */

#ifdef PAPYRUSKV
int buildUFXhash(int64_t size, FILE *fd, memory_heap_t *memory_heap_res, int64_t myShare, int64_t dsize, int64_t dmin, int CHUNK_SIZE, int load_factor)
#else
hash_table_t* buildUFXhash(int64_t size, FILE *fd, memory_heap_t *memory_heap_res, int64_t myShare, int64_t dsize, int64_t dmin, int CHUNK_SIZE, int load_factor)
#endif /* PAPYRUSKV */
{
#ifdef PAPYRUSKV
   int dist_hashtable;
   int64_t ptr, retval, i;
   int64_t my_ufx_lines, idx;
   int64_t chars_to_be_read;
#else
   hash_table_t *dist_hashtable;
   memory_heap_t memory_heap;
   shared list_t *lookup_res;
   int64_t chars_read, cur_chars_read , ptr, retval, i;
   int64_t buffer_size, offset;
   int64_t num_chars, chars_to_be_read;
   shared[1] int64_t *heap_sizes;
   int64_t *my_heap_sizes, my_heap_size, my_ufx_lines, idx;
   int *ufx_remote_thread, remote_thread;
   int64_t  hashval;
#endif /* PAPYRUSKV */

   UPC_TICK_T start_read, end_read, start_storing, end_storing, start_setup, end_setup, start_calculation, end_calculation;

#ifdef PAPYRUSKV
   papyruskv_option_t opt;
   opt.keylen = KMER_PACKED_LENGTH;
   opt.vallen = sizeof(list_t);
   opt.hash = NULL;

   papyruskv_open("meraculous", PAPYRUSKV_CREATE | PAPYRUSKV_RELAXED | PAPYRUSKV_WRONLY, &opt, &dist_hashtable);
   papyruskv_register_update(dist_hashtable, 0xdeadcafe, &cswap_used_flag);
#else
   heap_sizes = (shared[1] int64_t*) upc_all_alloc(THREADS, sizeof(int64_t));
   heap_sizes[MYTHREAD] = 0;
#endif /* PAPYRUSKV */
   
   my_ufx_lines = (size / THREADS + size % THREADS);
   if (MYTHREAD == THREADS-1) {
      my_ufx_lines = (size / THREADS + size % THREADS);
   } else {
      my_ufx_lines = size / THREADS;
   }
   
#ifndef PAPYRUSKV
   ufx_remote_thread = (int*) malloc(my_ufx_lines * sizeof(int));
#endif /* PAPYRUSKV */
   
   if (VERBOSE > 1) LOG("Thread %d: Preparing to read %ld UFX lines (%ld chars)\n", MYTHREAD, my_ufx_lines, chars_to_be_read);
   
#ifdef DEBUG
   int64_t kmer_count = 0;
#endif
   
   /* Initialize lookup-table --> necessary for packing routines */
   init_LookupTable();
   char **kmersarr;
   int *counts;
   char *lefts;
   char *rights;
   
   start_read = UPC_TICKS_NOW();
   int64_t kmers_read = UFXRead(fd, dsize, &kmersarr, &counts, &lefts, &rights, my_ufx_lines, dmin, 0, MYTHREAD);
   
   //FILE *aux = fopen("conerted.txt", "w");
   //char curkmer[KMER_LENGTH+1];
   //curkmer[KMER_LENGTH] = '\0';
   //for (i=0; i< kmers_read; i++) {
   //   fprintf( aux ,"%s\t%c%c\n", kmersarr[i], lefts[i], rights[i]);

   //}
   
   upc_barrier;
   if (MYTHREAD == 0)
      printf("Threads done with I/O\n");
   
   end_read = UPC_TICKS_NOW();
#if defined(DETAILED_BUILD_PROFILE) || defined(IO_TIME_PROFILE)
   fileIOTime = UPC_TICKS_TO_SECS(end_read-start_read);
#endif

#ifndef PAPYRUSKV
   my_heap_sizes = (int64_t*) calloc(THREADS, sizeof(int64_t));
#endif /* PAPYRUSKV */
   
   start_calculation= UPC_TICKS_NOW();
   /* calculate the exact number of UFX entries for each thread */
   /* previously estimated as EXPANSION_FACTOR * (size + size % THREADS) / THREADS */
   idx = 0;
   char rc_kmer[KMER_LENGTH+1];
   rc_kmer[KMER_LENGTH] = '\0';
   int is_least;
   
   for (ptr= 0; ptr < kmers_read; ptr++) {
      reverseComplementKmer(kmersarr[ptr], rc_kmer);
      is_least = 0;
      is_least = (strcmp(kmersarr[ptr], rc_kmer) > 0) ? 0  : 1 ;

#ifdef MERACULOUS
      if (is_least && (lefts[ptr] != 'F') && (lefts[ptr] != 'X') && (rights[ptr] != 'F') && (rights[ptr] != 'X'))
#endif
         
#ifdef MERDEPTH
      if (is_least)
#endif
         
#ifdef CEA
      if (is_least)
#endif
#ifdef PAPYRUSKV
      {
         _w0(13);
         list_t new_entry;
         char exts[2];
         unsigned char packed_key[KMER_PACKED_LENGTH];
         packSequence((unsigned char*) (kmersarr[ptr]), packed_key, KMER_LENGTH);
         exts[0] = lefts[ptr];
         exts[1] = rights[ptr];
         new_entry.packed_extensions = convertExtensionsToPackedCode((unsigned char*) exts);
         new_entry.used_flag = UNUSED;
         new_entry.my_contig = NULL;

         papyruskv_put(dist_hashtable, (const char*) packed_key, KMER_PACKED_LENGTH, (const char*) &new_entry, sizeof(new_entry));
         _w1(13);
      }
#else
      {
         hashval = hashkmer(load_factor * size, (char*) kmersarr[ptr]);
         remote_thread = hashval % (THREADS*BS);
         my_heap_sizes[remote_thread]++;
         ufx_remote_thread[idx] = remote_thread;
      } else {
         ufx_remote_thread[idx] = -1;
      }
#endif /* PAPYRUSKV */
      idx++;
   }
   
   assert(idx == my_ufx_lines);
   
#ifdef PAPYRUSKV
   _w0(14);
   papyruskv_consistency(dist_hashtable, PAPYRUSKV_SEQUENTIAL);
   _w1(14);
   _w0(15);
   papyruskv_protect(dist_hashtable, PAPYRUSKV_UDONLY);
   _w1(15);
   _w0(16);
   papyruskv_hash(dist_hashtable, NULL);
   _w1(16);
   
   end_calculation= UPC_TICKS_NOW();
#ifdef DETAILED_BUILD_PROFILE
   cardCalcTime = UPC_TICKS_TO_SECS(end_calculation-start_calculation);
#endif
   
   start_setup = UPC_TICKS_NOW();

   /* Create and initialize hashtable */

   end_setup = UPC_TICKS_NOW();
#ifdef DETAILED_BUILD_PROFILE
   setupTime = UPC_TICKS_TO_SECS(end_setup - start_setup);
#endif

   if (MYTHREAD == 0)
   printf("Threads done with setting-up\n");
   
   /* Allocate local arrays used for book-keeping when using aggregated upc_memputs */
   start_storing = UPC_TICKS_NOW();
#else
   /* now atomically add all my_sizes to global size heap */
   /* TODO replace with all reduce? */
   for(i = MYTHREAD; i < THREADS+MYTHREAD; i++) {
      if (VERBOSE > 1) LOG("Thread %d: sending %ld to thread %d\n", MYTHREAD, my_heap_sizes[i%THREADS], (int) (i%THREADS));
      assert( upc_threadof( heap_sizes + (i%THREADS) ) == (i%THREADS) );
      UPC_ATOMIC_FADD_I64(heap_sizes + (i%THREADS), my_heap_sizes[i%THREADS]);
   }
   free(my_heap_sizes);
   upc_fence;
   upc_barrier;
   assert(upc_threadof( heap_sizes + MYTHREAD) == MYTHREAD);
   my_heap_size = heap_sizes[MYTHREAD];
   upc_all_free(heap_sizes); // should clean up after last thread gets here
   if (VERBOSE > 1) LOG("Thread %d: allocating memory for %ld kmers\n",MYTHREAD, my_heap_size);
   
   end_calculation= UPC_TICKS_NOW();
#ifdef DETAILED_BUILD_PROFILE
   cardCalcTime = UPC_TICKS_TO_SECS(end_calculation-start_calculation);
#endif
   
   start_setup = UPC_TICKS_NOW();

   /* Create and initialize hashtable */
   dist_hashtable = create_hash_table(size * load_factor, &memory_heap, my_heap_size);
   upc_barrier;
   
   end_setup = UPC_TICKS_NOW();
#ifdef DETAILED_BUILD_PROFILE
   setupTime = UPC_TICKS_TO_SECS(end_setup - start_setup);
#endif

   if (MYTHREAD == 0)
   printf("Threads done with setting-up\n");
   
   /* Allocate local arrays used for book-keeping when using aggregated upc_memputs */
   list_t *local_buffs = (list_t*) malloc(THREADS * CHUNK_SIZE * sizeof(list_t));

   UPC_INT64_T *local_index = (UPC_INT64_T*) malloc(THREADS * sizeof(UPC_INT64_T));
   memset(local_index, 0, THREADS * sizeof(UPC_INT64_T));
   
   start_storing = UPC_TICKS_NOW();
      
   ptr = 0;
   idx = 0;
   list_t new_entry;

   UPC_INT64_T store_pos;
   char exts[2];
   
   while (ptr < kmers_read) {
      /* Set end of string chars to the appropriate positions:       */
      /* Pointer: working_buffer+ptr                ---> current kmer        */
      /* Pointer: working_buffer+ptr+KMER_LENGTH+1  ---> current extensions  */
      if (ufx_remote_thread[idx] >= 0) {
         // get cached remote thread
         remote_thread = ufx_remote_thread[idx];
         
#ifdef DEBUG
         /* if DEBUG, verify the calculated remote thread is actually the correct one */
         hashval = hashkmer(dist_hashtable->size, (char*) kmersarr[ptr]);
         assert( remote_thread == upc_threadof(&(dist_hashtable->table[hashval])) );
#endif
         packSequence((unsigned char*) (kmersarr[ptr]), new_entry.packed_key, KMER_LENGTH);
         
#ifdef MERACULOUS
         exts[0] = lefts[ptr];
         exts[1] = rights[ptr];
         new_entry.packed_extensions = convertExtensionsToPackedCode((unsigned char*) exts);
         new_entry.used_flag = UNUSED;
         new_entry.my_contig = NULL;
#endif
         
#ifdef MERDEPTH
         new_entry.count = counts[ptr];
#endif
         
#ifdef CEA
         new_entry.left_ext = lefts[ptr];
         new_entry.right_ext = rights[ptr];
#endif
         new_entry.next = NULL;
         
         /* Has more space at local buffer, so add to local buffer */
         if (local_index[remote_thread] <= CHUNK_SIZE - 1 ) {
            memcpy( &(local_buffs[local_index[remote_thread] + remote_thread * CHUNK_SIZE]), &new_entry, sizeof(list_t));
            local_index[remote_thread]++;
            if (VERBOSE > 2) LOG("Thread %d: sending %s to %d\n", MYTHREAD, kmersarr[ptr], remote_thread);
         }
         
         /* If buffer for that thread is full, do a remote upc_memput() */
         if (local_index[remote_thread] == CHUNK_SIZE) {
            store_pos = UPC_ATOMIC_FADD_I64((memory_heap.cached_heap_indices[remote_thread]), CHUNK_SIZE);
            upc_memput( (shared[] list_t*)  ((memory_heap.cached_heap_ptrs[remote_thread]) + store_pos)  , &(local_buffs[remote_thread * CHUNK_SIZE]), (CHUNK_SIZE) * sizeof(list_t));
            local_index[remote_thread] = 0;
         }
#ifdef DEBUG
         kmer_count++;
#endif
      } else {
         if (VERBOSE > 1) LOG("Thread %d: Skipping %.*s\n", MYTHREAD, KMER_LENGTH+3,kmersarr[ptr]);
      }

      ptr++;
      idx++;
   }
   
   assert(idx == my_ufx_lines);

   free(ufx_remote_thread);

   upc_barrier;
   upc_fence;
   upc_barrier;
   
   /* Now check if there are few more kmers to be stored in the local buffs */
   for (i = 0; i < THREADS; i++) {
      /* Have to store local_index[i] items */
      if (local_index[i] != 0) {
         store_pos = UPC_ATOMIC_FADD_I64((memory_heap.cached_heap_indices[i]), local_index[i]);
         upc_memput( (shared[] list_t*)  ((memory_heap.cached_heap_ptrs[i]) + store_pos)  , &(local_buffs[i * CHUNK_SIZE]), (local_index[i]) * sizeof(list_t));
      }
   }
   
   upc_barrier;
   upc_fence;
   upc_barrier;

   assert( memory_heap.heap_indices[MYTHREAD] == my_heap_size );
   /* Now each thread will iterate over its local heap and will add nodes to the local part of the hashtable - NO NEED FOR LOCKS!!! */
   int64_t heap_entry;
   
   list_t *local_filled_heap = (list_t*) memory_heap.cached_heap_ptrs[MYTHREAD];
   int64_t hashTableSize = dist_hashtable->size;
   shared[] list_t* shared_local_filled_heap = (memory_heap.heap_ptr[MYTHREAD]);
   //shared[] list_t* local_filled_heap = (memory_heap.heap_ptr[MYTHREAD]);
   unsigned char unpacked_kmer[KMER_LENGTH+1];
   unpacked_kmer[KMER_LENGTH] = '\0';

   for (heap_entry = 0; heap_entry < memory_heap.heap_indices[MYTHREAD]; heap_entry++) {
   
      unpackSequence((unsigned char*) &(local_filled_heap[heap_entry].packed_key), (unsigned char*) unpacked_kmer, KMER_LENGTH);
      hashval = hashkmer(dist_hashtable->size, (char*) (unpacked_kmer));
      local_filled_heap[heap_entry].next = dist_hashtable->table[hashval].head;
      dist_hashtable->table[hashval].head =  (shared[] list_t*) &(shared_local_filled_heap[heap_entry]);

   }
   
   upc_barrier;
   upc_fence;
   upc_barrier;

#endif /* PAPYRUSKV */

   end_storing = UPC_TICKS_NOW();
#ifdef DETAILED_BUILD_PROFILE
   storeTime = UPC_TICKS_TO_SECS(end_storing-start_storing);
#endif
   
#ifdef PROFILE 
   upc_fence;
   upc_barrier;
   upc_fence;
   upc_barrier;
   
   if (MYTHREAD == 0) {
      printf("\n************* SET - UP TIME *****************");
      printf("\nTime spent on setting up the distributed hash table is %f seconds\n", UPC_TICKS_TO_SECS(end_setup -start_setup));

#ifdef DETAILED_IO_PROFILING
      printf("\n\n************* DETAILED TIMINGS FOR I/O AND STORING KMERS *****************\n");
#endif
   }
   
   upc_barrier;
   
#ifdef DETAILED_IO_PROFILING

   printf("Thread %d spent %f seconds on read I/O and %f seconds on storing %ld kmers\n", MYTHREAD, UPC_TICKS_TO_SECS(end_read-start_read), UPC_TICKS_TO_SECS(end_storing -start_storing));

#endif

   upc_barrier;
   
   DeAllocateAll(&kmersarr, &counts, &lefts, &rights, kmers_read);
   
#ifdef BUCKET_BALANCE_PROFILING
   if (MYTHREAD == 0) {
      printf("\n--------------------------------\n");
      printf("--------- Bucket balance -------\n");
      printf("--------------------------------\n");
   }
   if (MYTHREAD == 0) {
      for (i=0; i<THREADS; i++)
         printf("Thread %ld has %ld elements in its buckets\n", i, memory_heap.heap_indices[i]);    
   }
#endif

#endif

   upc_barrier;

#ifndef PAPYRUSKV
   (*memory_heap_res) = memory_heap;
#endif /* PAPYRUSKV */
   return dist_hashtable;
}

#endif // BUILD_UFX_HASH_H

