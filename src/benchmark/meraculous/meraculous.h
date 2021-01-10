#ifndef MERACULOUS_H
#define MERACULOUS_H

#include <upc.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <math.h>
#include <string.h>
#include <assert.h>
#include "Debug.h"

#ifndef VERBOSE
#define VERBOSE 0
#endif

/* TODO: Define different block sizes according to data structure */
#define BS 1

FILE *mylog;
#define LOG(...) fprintf(mylog == NULL ? stderr : mylog, __VA_ARGS__)

#if defined USE_BUPC
#   define UPC_POLL bupc_poll()
#   define UPC_INT64_T int64_t
#   define UPC_ATOMIC_FADD_I64 bupc_atomicI64_fetchadd_strict
#   define UPC_ATOMIC_CSWAP_I64 bupc_atomicI64_cswap_strict
#   define USED_FLAG_TYPE int32_t
#   define UPC_ATOMIC_CSWAP_USED_FLAG bupc_atomicI_cswap_strict
// deprecated
// #   define UPC_TICK_T bupc_tick_t
// #   define UPC_TICKS_NOW bupc_ticks_now
// #   define UPC_TICKS_TO_SECS( t ) (bupc_ticks_to_us( t ) / 1000000.0)
#include <upc_tick.h>
#   define UPC_TICK_T upc_tick_t
#   define UPC_TICKS_NOW upc_ticks_now
#   define UPC_TICKS_TO_SECS( t ) (upc_ticks_to_ns( t ) / 1000000000.0)
#elif defined USE_CRAY_UPC
#   define UPC_POLL /* noop */
#   define UPC_INT64_T int64_t
#   define UPC_ATOMIC_FADD_I64 _amo_afadd_upc
#   define UPC_ATOMIC_CSWAP_I64 _amo_acswap_upc
#   define USED_FLAG_TYPE UPC_INT64_T
#   define UPC_ATOMIC_CSWAP_USED_FLAG UPC_ATOMIC_CSWAP_I64
#include <upc_tick.h>
#   define UPC_TICK_T upc_tick_t
#   define UPC_TICKS_NOW upc_ticks_now
#   define UPC_TICKS_TO_SECS( t ) (upc_ticks_to_ns( t ) / 1000000000.0)
#endif

#ifndef REMOTE_ASSERT
#define REMOTE_ASSERT 0
#endif

// This check is necessary to ensure that shared pointers of 16 bytes are unlikely to be corrupted.
// 8 byte pointers will never be subject to a race condition
#define LOOP_CHECK_INTERVAL 100000
#ifdef USE_CRAY_UPC
// going for the whole address space in upc_addrfield check
#define IS_VALID_UPC_PTR( ptr ) ( sizeof(shared void *) == 8 ? 1 : ( ptr == NULL || ( upc_threadof(ptr) >= 0 && upc_threadof(ptr) < THREADS && upc_phaseof(ptr) >= 0 && upc_phaseof(ptr) < BS && upc_addrfield(ptr) < (1L<<48) ) ) )
#else
#define IS_VALID_UPC_PTR( ptr ) ( sizeof(shared void *) == 8 ? 1 : ( ptr == NULL || ( upc_threadof(ptr) >= 0 && upc_threadof(ptr) < THREADS && upc_phaseof(ptr) >= 0 && upc_phaseof(ptr) < BS && upc_addrfield(ptr) < 17179869184 ) ) )
#endif

#define loop_until( stmt, cond ) \
{ \
   long attempts = 0; do \
   { \
      stmt; \
      if ( cond ) break; \
      UPC_POLL; \
      if (++attempts % LOOP_CHECK_INTERVAL == 0) LOG("possible infinite loop in loop_until(" #stmt ", " #cond "): %s:%u\n",  __FILE__, __LINE__); \
   } while( 1 ); \
   assert( cond ); \
}


#if REMOTE_ASSERT != 0
#define remote_assert( cond ) loop_until( ((void)0), cond ); assert( cond ); 
#else
#define remote_assert( cond ) /* noop(cond) */
#endif

#ifndef EXPANSION_FACTOR
#define EXPANSION_FACTOR 2
#endif

/* minimum allocation for contig sequences */
#ifndef MINIMUM_CONTIG_SIZE
#define MINIMUM_CONTIG_SIZE 192
#endif

#define INCR_FACTOR 2

/* Define segment length for FASTA sequences */
#ifndef SEGMENT_LENGTH
#define SEGMENT_LENGTH 51
#endif

/* Define constants for states etc */
#define USED 1
#define UNUSED (-1)
#define FINISHED 1
#define UNFINISHED 0
#define TRUE 1
#define FALSE 0
#define ACTIVE 0
#define ABORTED 1
#define CLAIMED 2
#define COMPLETE 3

#define oracle_t unsigned char

/* Contig data structure */
typedef struct contig_t contig_t;
typedef shared[] contig_t * contig_ptr_t;
typedef struct contig_ptr_box_list_t contig_ptr_box_list_t;

/* contig "box" holding contig_t pointers in a linked list and list of merged contigs */
struct contig_ptr_box_list_t {
   shared[] contig_ptr_box_list_t *next;
   contig_ptr_t contig;
   contig_ptr_t original;
};

struct contig_t{
#ifdef SYNC_PROTOCOL
   upc_lock_t *state_lock;                // Use this lock to synchronize threads when concatenating contigs
   int64_t state;                   // State of the contig
#endif
   shared[] char *sequence;               // Actual content of the contig
   int64_t size;                    // Current size of the contig
   int64_t start;                   // position of the first base in the sequence
   int64_t end;                     // position of the last base in the sequence
   int64_t is_circular;                // 1 if the contig is circular or otherwise wraps onto itself
#ifndef SYNC_PROTOCOL
   int64_t  timestamp;                 // Timestamp used to pick "randomly" contigs if no synchronization protocol is used
#endif
   int64_t contig_id;                  // The unique id of the contig
   shared[] contig_ptr_box_list_t *myself;            // a linked list pointer of contigs
};

/* Hash-table entries data structure (i.e. kmers and extensions) */
typedef struct list_t list_t;

#ifdef MARK_POS_IN_CONTIG
#define LIST_POS_IN_CONTIG int posInContig; /* position of kmer in the UU contig */
#else
#define LIST_POS_IN_CONTIG /* no field */
#endif

#ifdef SYNC_PROTOCOL

#ifdef MERACULOUS

#define LIST_STRUCT_CONTENTS \
shared[] list_t *next;                         /* Pointer to next entry in the same bucket */ \
shared[] contig_ptr_box_list_t *my_contig;     /* Pointer to the "box" of the contig ptr this k-mer has been added to */ \
USED_FLAG_TYPE used_flag;                      /* State flag */ \
LIST_POS_IN_CONTIG              \
unsigned char packed_key[KMER_PACKED_LENGTH];  /* The packed key of the k-mer */       \
unsigned char packed_extensions;               /* The packed extensions of the k-mer */ 

#endif

#ifdef MERDEPTH

#define LIST_STRUCT_CONTENTS \
shared[] list_t *next;                         /* Pointer to next entry in the same bucket */ \
int count;                      /* State flag */ \
unsigned char packed_key[KMER_PACKED_LENGTH];  /* The packed key of the k-mer */

#endif

#ifdef CEA

#define LIST_STRUCT_CONTENTS \
shared[] list_t *next;                         /* Pointer to next entry in the same bucket */ \
char left_ext;  \
char right_ext; \
unsigned char packed_key[KMER_PACKED_LENGTH];  /* The packed key of the k-mer */

#endif

#else // !defined SYNC_PROTOCOL

#define LIST_STRUCT_CONTENTS \
shared[] list_t *next;                         /* Pointer to next entry in the same bucket */ \
UPC_INT64_T used_flag;                         /* State flag */ \
unsigned char packed_key[KMER_PACKED_LENGTH];  /* The packed key of the k-mer */       \
unsigned char packed_extensions;               /* The packed extensions of the k-mer */

#endif

typedef struct unpadded_list_t unpadded_list_t;
struct unpadded_list_t{
   LIST_STRUCT_CONTENTS
};

//#define NO_PAD
#ifndef UPC_PAD_TO 
#define UPC_PAD_TO 64
#endif
#define UPC_PADDING (sizeof(unpadded_list_t) % UPC_PAD_TO == 0 ? 0 : UPC_PAD_TO - sizeof(unpadded_list_t) % UPC_PAD_TO)

struct list_t{
#ifdef PAPYRUSKV
shared[] contig_ptr_box_list_t *my_contig;     /* Pointer to the "box" of the contig ptr this k-mer has been added to */ \
USED_FLAG_TYPE used_flag;                      /* State flag */ \
unsigned char packed_extensions;               /* The packed extensions of the k-mer */ 
#else
   LIST_STRUCT_CONTENTS
#ifndef NO_PAD
   unsigned char _padding_for_UPC_bug[ UPC_PADDING ];
#endif
#endif /* PAPYRUSKV */
};

typedef shared[] list_t* shared_heap_ptr;


/* Bucket data structure */
typedef struct bucket_t bucket_t;
struct bucket_t{
   shared[] list_t *head;                    // Pointer to the first entry of the hashtable
};

/* Hash-table data structure */
typedef struct hash_table_t hash_table_t;
struct hash_table_t {
   int64_t size;                             // Size of the hashtable
   shared[BS] bucket_t *table;                     // Entries of the hashtable are pointers to buckets
};

typedef shared[BS] UPC_INT64_T* shared_int64_ptr;

/* Memory heap data structure */
typedef struct memory_heap_t memory_heap_t;
struct memory_heap_t {
   shared[BS] shared_heap_ptr *heap_ptr;              // Pointers to shared memory heaps
   shared[BS] UPC_INT64_T *heap_indices;                 // Indices of remote heaps
   shared_heap_ptr *cached_heap_ptrs;
   shared_int64_ptr *cached_heap_indices;
   int64_t heap_size;
};

/* contigScaffoldMap data structure */
typedef struct contigScaffoldMap_t contigScaffoldMap_t;
struct contigScaffoldMap_t {
   int cStrand;
   int scaffID;
   int sStart;
   int sEnd;
};

#endif // MERACULOUS_H

