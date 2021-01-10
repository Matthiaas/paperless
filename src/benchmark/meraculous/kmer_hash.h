#ifndef KMER_HASH_H
#define KMER_HASH_H

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <math.h> 
#include <upc.h>
#include <string.h>
#include <assert.h>

#include "meraculous.h"
#include "kmer_handling.h"
#include "packingDNAseq.h"
#include "wtimer.h"

#ifdef PAPYRUSKV
#include <papyrus/kv.h>
#endif /* PAPYRUSKV */

extern int64_t  local_allocs;

#ifndef COLL_ALLOC
/* Creates and initializes a distributed hastable - collective function */
hash_table_t* create_hash_table(int64_t  size, memory_heap_t *memory_heap, int64_t my_heap_size)
{
   hash_table_t *result;
   int64_t  i;
   int64_t  n_buckets = size;
   
   result = (hash_table_t*) malloc(sizeof(hash_table_t));
   result->size = n_buckets;
   result->table = (shared[BS] bucket_t*) upc_all_alloc(n_buckets, sizeof(bucket_t));
   if (result->table == NULL) {
      fprintf(stderr, "Thread %d: ERROR: Could not allocate memory for the distributed hash table! %ld buckets of %ld bytes\n", MYTHREAD, n_buckets, sizeof(bucket_t));
      upc_global_exit(1);
   }
   
   for (i=MYTHREAD; i < n_buckets; i+=THREADS) {
      result->table[i].head = NULL;
   }
   
   memory_heap->heap_ptr = (shared[BS] shared_heap_ptr*) upc_all_alloc(THREADS, sizeof(shared_heap_ptr));
   memory_heap->heap_indices = (shared[BS] UPC_INT64_T*) upc_all_alloc(THREADS, sizeof(UPC_INT64_T));
   if (memory_heap->heap_indices == NULL || memory_heap->heap_ptr == NULL) {
      fprintf(stderr, "Thread %d: ERROR: Could not allocate memory for the distributed hash table or index arrays!\n", MYTHREAD);
      upc_global_exit(1);
   }

   memory_heap->heap_size = my_heap_size; // local variable
   memory_heap->heap_indices[MYTHREAD] = 0;
   memory_heap->heap_ptr[MYTHREAD] = (shared[] list_t*) upc_alloc( my_heap_size * sizeof(list_t));
   if (memory_heap->heap_ptr[MYTHREAD] == NULL) {
      fprintf(stderr, "Thread %d: ERROR: Could not allocate memory for the distributed hash table! %ld elements of %ld bytes\n", MYTHREAD, my_heap_size, sizeof(list_t));
      upc_global_exit(1);
   }
   
   
   upc_fence;
   upc_barrier;
   
   memory_heap->cached_heap_ptrs = (shared_heap_ptr *) malloc(THREADS * sizeof(shared_heap_ptr));
   memory_heap->cached_heap_indices = (shared_int64_ptr *) malloc(THREADS * sizeof(shared_int64_ptr));

   for (i=0; i < THREADS; i++) {
      memory_heap->cached_heap_ptrs[i] = memory_heap->heap_ptr[i];
      memory_heap->cached_heap_indices[i] = &(memory_heap->heap_indices[i]);
   }
   
   return result;
}
#endif

#ifdef COLL_ALLOC
/* Creates and initializes a distributed hastable - collective function */
hash_table_t* create_hash_table(int64_t  size, memory_heap_t *memory_heap)
{
   hash_table_t *result;
   int64_t  i;
   int64_t  n_buckets = size;
   
#ifdef ALLOC_TIMING
   UPC_TICK_T start_timer, end_timer;
   upc_barrier;
   if (MYTHREAD == 0)
      start_timer = UPC_TICKS_NOW();
#endif

   result = (hash_table_t*) malloc(sizeof(hash_table_t));
   result->size = n_buckets;
   result->table = (shared[BS] bucket_t*) upc_all_alloc(n_buckets, sizeof(bucket_t));
   
#ifdef ALLOC_TIMING
   upc_barrier;
   if (MYTHREAD == 0) {
      end_timer = UPC_TICKS_NOW();
      printf("\nupc_all_alloc()time for buckets is %f seconds\n", UPC_TICKS_TO_SECS(end_timer-start_timer);
      
   }
#endif

#ifdef ALLOC_TIMING
   if (MYTHREAD == 0)
      start_timer = UPC_TICKS_NOW();
#endif

   for (i=MYTHREAD; i < n_buckets; i+=THREADS) {
         result->table[i].head = NULL;
   }
   
#ifdef ALLOC_TIMING
   upc_barrier;
   if (MYTHREAD == 0) {
      end_timer = UPC_TICKS_NOW();
      printf("\nTime for initializing heads is %f seconds\n", UPC_TICKS_TO_SECS(end_timer-start_timer));
      
   }
#endif

#ifdef ALLOC_TIMING
   if (MYTHREAD == 0)
      start_timer = UPC_TICKS_NOW();
#endif
   shared[BS] list_t *init_ptr = (shared[BS] list_t*) upc_all_alloc(EXPANSION_FACTOR * n_buckets), sizeof(list_t));
   
#ifdef ALLOC_TIMING
   upc_barrier;
   if (MYTHREAD == 0) {
      end_timer = UPC_TICKS_NOW();
      printf("\nupc_all_alloc()time for entries is %f seconds\n", UPC_TICKS_TO_SECS(end_timer-start_timer));

   }
#endif
   
#ifdef ALLOC_TIMING
   if (MYTHREAD == 0)
      start_timer = UPC_TICKS_NOW();
#endif
   memory_heap->heap_ptr = (shared[BS] shared_heap_ptr*) upc_all_alloc(THREADS, sizeof(shared_heap_ptr));
#ifdef ALLOC_TIMING
   upc_barrier;
   if (MYTHREAD == 0) {
      end_timer = UPC_TICKS_NOW();
      printf("\nupc_all_alloc()time for heap_ptrs is %f seconds\n", UPC_TICKS_TO_SECS(end_timer-start_timer));
      
   }
#endif
   
#ifdef ALLOC_TIMING
   if (MYTHREAD == 0)
      start_timer = UPC_TICKS_NOW();
#endif

   memory_heap->heap_indices = (shared[BS] UPC_INT64_T*) upc_all_alloc(THREADS, sizeof(UPC_INT64_T));

#ifdef ALLOC_TIMING
   upc_barrier;
   if (MYTHREAD == 0) {
      end_timer = UPC_TICKS_NOW();
      printf("\nupc_all_alloc()time for heap indices is %f seconds\n", UPC_TICKS_TO_SECS(end_timer-start_timer));
      
   }
#endif

#ifdef ALLOC_TIMING
   if (MYTHREAD == 0)
      start_timer = UPC_TICKS_NOW();
#endif
   memory_heap->heap_ptr[MYTHREAD] = (shared[] list_t*)  (init_ptr + MYTHREAD);
#ifdef ALLOC_TIMING
   upc_barrier;
   if (MYTHREAD == 0) {
      end_timer = UPC_TICKS_NOW();
      printf("\nTime for initializing heap ptrs is %f seconds\n", UPC_TICKS_TO_SECS(end_timer-start_timer));
      
   }
#endif

   /* Make sure that memory is allocated before copying local object */
#ifdef ALLOC_TIMING
   if (MYTHREAD == 0)
      start_timer = UPC_TICKS_NOW();
#endif   
   memory_heap->heap_indices[MYTHREAD] = 0;
   upc_barrier;
#ifdef ALLOC_TIMING
   upc_barrier;
   if (MYTHREAD == 0) {
      end_timer = UPC_TICKS_NOW();
      printf("\nTime for initializing heap indices is %f seconds\n", UPC_TICKS_TO_SECS(end_timer-start_timer));
      
   }
#endif
   
   return result;
}
#endif

/* Computes the hash value of null terminated sequence */
int64_t  hash(int64_t  hashtable_size, char *kmer)
{
    unsigned long hashval;
    hashval = 5381;
    for(; *kmer != '\0'; kmer++) hashval = (*kmer) +  (hashval << 5) + hashval;
      
    return hashval % hashtable_size;
}

int64_t  hashseq(int64_t  hashtable_size, char *seq, int size)
{
   unsigned long hashval;
   hashval = 5381;
   for(int i = 0; i < size; i++) {
      hashval = seq[i] +  (hashval << 5) + hashval;
   }
   
   return hashval % hashtable_size;
}

int64_t hashkmer(int64_t  hashtable_size, char *seq)
{
   return hashseq(hashtable_size, seq, KMER_LENGTH);
}
  
/* Use this lookup function when no writes take place in the distributed hashtable */
#ifdef PAPYRUSKV
list_t* lookup_kmer_and_copy(int hashtable, const unsigned char *kmer, unsigned char* packed_key, list_t *cached_copy, papyruskv_pos_t* pos)
#else
shared[] list_t* lookup_kmer_and_copy(hash_table_t *hashtable, const unsigned char *kmer, list_t *cached_copy)
#endif /* PAPYRUSKV */
{  
   // TODO: Fix hash functions to compute on packed kmers and avoid conversions
#ifdef PAPYRUSKV
   packSequence(kmer, packed_key, KMER_LENGTH);

   list_t *result = NULL;
   _w0(11);
   int iret = papyruskv_get_pos(hashtable, (const char*) packed_key, KMER_PACKED_LENGTH, (char**) &result, NULL, pos);
   _w1(11);
   if (iret != PAPYRUSKV_OK) return NULL;
   *cached_copy = *result; //todo:
   return result;
#else
   int64_t  hashval = hashkmer(hashtable->size, (char*) kmer);
   unsigned char packed_key[KMER_PACKED_LENGTH];
   unsigned char remote_packed_key[KMER_PACKED_LENGTH];

   packSequence(kmer, packed_key, KMER_LENGTH);

   shared[] list_t *result;
   bucket_t local_buc;
   
   _w0(11);
   local_buc = hashtable->table[hashval];
   result = local_buc.head;

   for (; result != NULL;) {
      //upc_memget(remote_packed_key, result->packed_key, KMER_PACKED_LENGTH * sizeof(char));
      //upc_memget(local_res, result, sizeof(list_t));
      assert( upc_threadof( result ) == hashval % THREADS );
#ifdef MERACULOUS
      loop_until( *cached_copy = *result , IS_VALID_UPC_PTR(cached_copy->my_contig) );
#endif
#ifdef MERDEPTH
      *cached_copy = *result;
#endif
#ifdef CEA
      *cached_copy = *result;
#endif
      //if (local_res.structid != 1988) {
      // printf("FATAL error in lookup, struct id is %d!!!\n", local_res.structid);
      //}
      if (comparePackedSeq(packed_key, cached_copy->packed_key, KMER_PACKED_LENGTH) == 0){
          _w1(11);
         return result;
      }
      result = cached_copy->next;
   }
   if (VERBOSE > 2) {
      unsigned char tmp[KMER_LENGTH+1];
      tmp[KMER_LENGTH] = '\0';
      unpackSequence(packed_key, tmp, KMER_LENGTH);
      LOG("Thread %d: lookup_kmer(): did not find %s (%s)\n", MYTHREAD, tmp, kmer);
   }
   _w1(11);
   return NULL;
#endif /* PAPYRUSKV */
}

#ifdef PAPYRUSKV
list_t* lookup_kmer(int hashtable, const unsigned char *kmer, unsigned char* packed_key) {
#else
shared[] list_t* lookup_kmer(hash_table_t *hashtable, const unsigned char *kmer) {
#endif /* PAPYRUSKV */
   list_t cached_copy;
#ifdef PAPYRUSKV
   return lookup_kmer_and_copy(hashtable, kmer, packed_key, &cached_copy, NULL);
#else
   return lookup_kmer_and_copy(hashtable, kmer, &cached_copy);
#endif /* PAPYRUSKV */
}

/* find the entry for this kmer or reverse complement */
#ifdef PAPYRUSKV
list_t *lookup_least_kmer_and_copy(int dist_hashtable, const char *next_kmer, unsigned char* packed_key, list_t *cached_copy, int *is_least, papyruskv_pos_t* pos) {
#else
shared[] list_t *lookup_least_kmer_and_copy(hash_table_t *dist_hashtable, const char *next_kmer, list_t *cached_copy, int *is_least) {
#endif /* PAPYRUSKV */
   char auxiliary_kmer[KMER_LENGTH+1];
   char *kmer_to_search;
   auxiliary_kmer[KMER_LENGTH] = '\0';
   /* Search for the canonical kmer */
   kmer_to_search = getLeastKmer(next_kmer, auxiliary_kmer);
   *is_least = (kmer_to_search == next_kmer) ? 1 : 0;
#ifdef PAPYRUSKV
   list_t *res;
   res = lookup_kmer_and_copy(dist_hashtable, (unsigned char*) kmer_to_search, packed_key, cached_copy, pos);
#else
   shared[] list_t *res;
   res = lookup_kmer_and_copy(dist_hashtable, (unsigned char*) kmer_to_search, cached_copy);
#endif /* PAPYRUSKV */
   if (VERBOSE > 2) LOG("Thread %d: lookup_least_kmer2(%s) is_least: %d res: %s\n", MYTHREAD, next_kmer, *is_least, res == NULL ? " not found" : "found");
   return res;
}

#ifdef PAPYRUSKV
list_t *lookup_least_kmer(int dist_hashtable, const char *next_kmer, int *was_least, unsigned char* packed_key, papyruskv_pos_t* pos) {
#else
shared[] list_t *lookup_least_kmer(hash_table_t *dist_hashtable, const char *next_kmer, int *was_least) {
#endif /* PAPYRUSKV */
   list_t cached_copy;
#ifdef PAPYRUSKV
   return lookup_least_kmer_and_copy(dist_hashtable, next_kmer, packed_key, &cached_copy, was_least, pos);
#else
   return lookup_least_kmer_and_copy(dist_hashtable, next_kmer, &cached_copy, was_least);
#endif /* PAPYRUSKV */
}

void set_ext_of_kmer(list_t *lookup_res, int is_least, char *new_seed_le, char *new_seed_re) {
   if (is_least) {
#ifdef MERACULOUS
      convertPackedCodeToExtension(lookup_res->packed_extensions,new_seed_le,new_seed_re);
#endif
   } else {
#ifdef MERACULOUS
      convertPackedCodeToExtension(lookup_res->packed_extensions,new_seed_re,new_seed_le);
#endif
      *new_seed_re = reverseComplementBaseExt(*new_seed_re);
      *new_seed_le = reverseComplementBaseExt(*new_seed_le);
   }
   if (VERBOSE > 2) LOG("Thread %d: set_ext_of_kmer: is_least: %d l:%c r:%c\n", MYTHREAD, is_least, *new_seed_le, *new_seed_re);
   
}

/* find the entry for this kmer or reverse complement, set the left and right extensions */
#ifdef PAPYRUSKV
list_t *lookup_and_get_ext_of_kmer(int dist_hashtable, const char *next_kmer, char *new_seed_le, char *new_seed_re, unsigned char* packed_key, papyruskv_pos_t* pos)
#else
shared[] list_t *lookup_and_get_ext_of_kmer(hash_table_t *dist_hashtable, const char *next_kmer, char *new_seed_le, char *new_seed_re)
#endif /* PAPYRUSKV */
{
   int is_least;
#ifdef PAPYRUSKV
   list_t *lookup_res;
#else
   shared[] list_t *lookup_res;
#endif /* PAPYRUSKV */
   list_t copy;
   *new_seed_le = '\0';
   *new_seed_re = '\0';
   
   /* Search for the canonical kmer */
#ifdef PAPYRUSKV
   lookup_res = lookup_least_kmer_and_copy(dist_hashtable, next_kmer, packed_key, &copy, &is_least, pos);
#else
   lookup_res = lookup_least_kmer_and_copy(dist_hashtable, next_kmer, &copy, &is_least);
#endif /* PAPYRUSKV */
   
   if (lookup_res == NULL) {
      return lookup_res;
   }
   
   /* Find extensions of the new kmer found in the hashtable */
   set_ext_of_kmer(&copy, is_least, new_seed_le, new_seed_re);
   
   return lookup_res;
}

/* get the contig_ptr "box" associated with a kmer -- must be USED already to work */
#ifdef PAPYRUSKV
shared[] contig_ptr_box_list_t *get_contig_box_of_kmer(int hashtable, unsigned char* packed_key, list_t *lookup_res, int follow_list) {
#else
shared[] contig_ptr_box_list_t *get_contig_box_of_kmer(shared[] list_t *lookup_res, int follow_list) {
#endif /* PAPYRUSKV */
   shared[] contig_ptr_box_list_t *box_ptr = NULL;
   kmer_and_ext_t kmer_and_ext;
   
   if (lookup_res == NULL)
      printf("FATAL ERROR: K-mer should not be NULL here (right walk)\n");
   assert(lookup_res != NULL);
   assert(lookup_res->used_flag == USED);
#ifdef MERACULOUS
#ifdef PAPYRUSKV
   do {
       box_ptr = lookup_res->my_contig;
       if (box_ptr != NULL && IS_VALID_UPC_PTR(box_ptr)) break;
       _w0(12);
       papyruskv_get(hashtable, (const char*) packed_key, KMER_PACKED_LENGTH, (char**) &lookup_res, NULL);
       _w1(12);
   } while (1);
#else
   loop_until ( box_ptr = lookup_res->my_contig, box_ptr != NULL && IS_VALID_UPC_PTR(box_ptr)  );
#endif /* PAPYRUSKV */
#endif
   // follow box to tail
   while(follow_list && box_ptr->next != NULL) {
      box_ptr = box_ptr->next;
   }

   return box_ptr;
}

/* get the contig associated with a kmer -- must be USED already to work */
#ifdef PAPYRUSKV
shared[] contig_t *get_contig_of_kmer(int hashtable, unsigned char* packed_key, list_t *lookup_res, int follow_list) {
#else
shared[] contig_t *get_contig_of_kmer(shared[] list_t *lookup_res, int follow_list) {
#endif /* PAPYRUSKV */
   shared[] contig_ptr_box_list_t *box_ptr;
   kmer_and_ext_t kmer_and_ext;
   shared[] contig_t *contig = NULL;
   
   assert(lookup_res != NULL);
   assert(lookup_res->used_flag == USED);

   loop_until( 
#ifdef PAPYRUSKV
      box_ptr = get_contig_box_of_kmer(hashtable, packed_key, lookup_res, follow_list); contig = box_ptr->contig,
#else
      box_ptr = get_contig_box_of_kmer(lookup_res, follow_list); contig = box_ptr->contig,
#endif /* PAPYRUSKV */
      contig != NULL && IS_VALID_UPC_PTR(contig)
      );

   // can not make this assertion in parallel as the list can grow
   // assert( follow_list == 0 || box_ptr->next == NULL );
   
   return contig;
}

/* Use this lookup function when writes take place in the distributed hashtable - i.e. during creation */
/*shared[] list_t* sync_lookup_kmer(hash_table_t *hashtable, unsigned char *kmer)
{  
   // TODO: Fix hash functions to compute on packed kmers and avoid conversions
    int64_t  hashval = hash(hashtable->size, (char*) kmer);
   unsigned char packed_key[KMER_PACKED_LENGTH];
   packSequence(kmer, packed_key, KMER_LENGTH);
   shared[] list_t *result;
   list_t remote_element;
   
   upc_lock(hashtable->table[hashval].bucket_lock);
   for (result = hashtable->table[hashval].head; result != NULL; result = result->next) {
      upc_memget(&remote_element, result, sizeof(list_t));
      if (comparePackedSeq(packed_key, remote_element.packed_key, KMER_PACKED_LENGTH) == 0){ 
         upc_unlock(hashtable->table[hashval].bucket_lock);
         return result;
      }
   }
   upc_unlock(hashtable->table[hashval].bucket_lock);
   return NULL;
}*/

#ifndef PAPYRUSKV
int add_kmer(hash_table_t *hashtable, unsigned char *kmer, unsigned char *extensions, memory_heap_t *memory_heap, int64_t  *ptrs_to_stack, int64_t  *ptrs_to_stack_limits, int CHUNK_SIZE)
{
   list_t new_entry;
   shared[] list_t *insertion_point;

   int64_t  hashval = hashkmer(hashtable->size, (char*) kmer);
   int64_t  pos;
   int64_t  new_init_pos, new_limit;
   int remote_thread = upc_threadof(&(hashtable->table[hashval]));
   
   if (ptrs_to_stack[remote_thread] < ptrs_to_stack_limits[remote_thread]) {
      pos = ptrs_to_stack[remote_thread];
      ptrs_to_stack[remote_thread]++;
   } else {
   
      new_init_pos = UPC_ATOMIC_FADD_I64(&(memory_heap->heap_indices[remote_thread]), CHUNK_SIZE);
      
      new_limit = new_init_pos + CHUNK_SIZE;
      if (new_limit > EXPANSION_FACTOR * (hashtable->size / THREADS))
         new_limit = EXPANSION_FACTOR * (hashtable->size / THREADS);
      if (new_limit <= new_init_pos)
         pos = EXPANSION_FACTOR *(hashtable->size / THREADS);
      else
         pos = new_init_pos;
      new_init_pos++;
      ptrs_to_stack[remote_thread] = new_init_pos;
      ptrs_to_stack_limits[remote_thread] = new_limit;
   }
   
   if (pos < EXPANSION_FACTOR * (hashtable->size / THREADS)) {
      // Case 1: There is more space in the remote heap
      insertion_point = (shared[] list_t*) ((memory_heap->heap_ptr[remote_thread]) + pos);
   } else {
      // Case 2; There is NOT more space in the remote heap, allocate locally space
      insertion_point = (shared[] list_t*) upc_alloc(sizeof(list_t));
#ifdef MERACULOUS
      insertion_point->my_contig  = (shared[] contig_ptr_box_list_t*) upc_alloc(sizeof(contig_ptr_box_list_t));
      insertion_point->my_contig->contig = NULL;
#endif
      //local_allocs++;
   }
   
   /* Pack and insert kmer info into list - use locks to avoid race conditions */
   packSequence(kmer, new_entry.packed_key, KMER_LENGTH);
#ifdef MERACULOUS
   new_entry.packed_extensions = convertExtensionsToPackedCode(extensions);
   new_entry.used_flag = UNUSED;
#endif
   new_entry.next = NULL;

#ifdef SYNC_PROTOCOL
#ifdef MERACULOUS
   new_entry.my_contig = NULL;
#endif
#endif
   upc_memput(insertion_point, &new_entry, sizeof(list_t));
   //printf("Thread %d returned from upc_memput() and hashval is %ld \n", MYTHREAD, hashval);

   /* Use locks to update the head pointer in the remote bucket */
   insertion_point->next = hashtable->table[hashval].head;
   hashtable->table[hashval].head = insertion_point;
   //printf("Thread %d completed add_kmer for hashval %ld \n", MYTHREAD, hashval);

   
   return 0;
}
#endif /* PAPYRUSKV */

#endif // KMER_HASH_H

