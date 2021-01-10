#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <math.h> 
#include <assert.h>
#include "optlist.h"
#include "optlist.c"

#include <upc.h>

#include "meraculous.h"

#ifdef LHS_PERF
int64_t lookups = 0;
int64_t success = 0;
#endif

#ifdef USE_CRAY_UPC
#include <intrinsics.h>
#include <upc_cray.h>
#endif

#include <string.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>

#include "kmer_hash.h"
#include "kmer_handling.h"

#if defined(DETAILED_BUILD_PROFILE) || defined(IO_TIME_PROFILE)
double fileIOTime = 0.0;
#endif
#ifdef DETAILED_BUILD_PROFILE
double cardCalcTime = 0.0;
double setupTime = 0.0;
double storeTime = 0.0;
#endif

#include "readufx.h"
#include "buildUFXhashBinary.h"

int myContigs = 0;

#include "UU_traversal_final.h"

#define MAX_READ_FILENAME_SIZE 2000

#ifdef PAPYRUSKV
#include <papyrus/bupc.h>
#endif
char nick_[256];

#ifdef USE_BUPC
shared int64_t contig_id;
#endif

#ifdef USE_CRAY_UPC
shared long contig_id;
#endif

shared[BS] contig_ptr_t *contig_list;

shared int64_t timestamp;
int64_t local_allocs = 0;
 
#ifdef PROFILE
int64_t bases_added = 0;
#endif

#if defined(UU_TRAV_PROFILE) || defined(DETAILED_UU_TRAV_PROFILE)
double walking_time = 0.0;
double UU_time = 0.0;
#endif

#ifdef CONTIG_LOCK_ALLOC_PROFILE
double lockAllocTime = 0.0;
#endif

int main(int argc, char **argv) {
#ifdef PAPYRUSKV
   bupc_init(&argc, &argv);
   papyruskv_init(&argc, &argv, "./pkv");
#endif /* PAPYRUSKV */
   sprintf(nick_, "[%d]", MYTHREAD);
   int fileNo = 0;
   int i;
   int64_t size;
#ifdef PAPYRUSKV
   int dist_hashtable;
#else
   hash_table_t *dist_hashtable;
#endif /* PAPYRUSKV */
   memory_heap_t memory_heap;
   FILE *fd, *my_out_file, *blastresFD;
   FILE *reads_fd, *fdReads;
   char readFileName[MAX_READ_FILENAME_SIZE];
   char outputBlastmap[MAX_READ_FILENAME_SIZE];
#if defined(DETAILED_BUILD_PROFILE) || defined(IO_TIME_PROFILE)
   shared[1] double *ioTimes;
   double max_IO;
#endif
#ifdef DETAILED_BUILD_PROFILE
   shared[1] double *setupTimes;
   shared[1] double *calcTimes;
   shared[1] double *storeTimes;
   double min_IO, min_calc, min_store, min_setup;
   double max_store, max_calc, max_setup;
   double sum_ios=0.0;
   double sum_calcs=0.0;
   double sum_stores=0.0;
   double sum_setups=0.0;
#endif 
#ifdef DETAILED_UU_TRAV_PROFILE
   shared[1] double *UU_times_all;
   shared[1] double *walking_times_all;
   double min_UU_time, min_walking_time;
   double max_UU_time, max_walking_time;
   double sum_UU_time=0.0;
   double sum_walking_time=0.0;
#endif
 
   /* Use getopt() to read arguments */
   extern char *optarg;
   extern int optind;
   int c;
   char *input_UFX_name, *output_name, *read_files_name;
   int minimum_contig_length=MINIMUM_CONTIG_SIZE, dmin=10;
   int chunk_size = 1;
   
   option_t *optList, *thisOpt;
   optList = NULL;
   optList = GetOptList(argc, argv, "i:o:m:d:c:s:l:f:");
   
   char *string_size;
   int load_factor = 1;
   char *oracle_file;
   
   while (optList != NULL) {
      thisOpt = optList;
      optList = optList->next;
      switch (thisOpt->option) {
         case 'i':
            input_UFX_name = thisOpt->argument;
            break;
         case 'o':
            output_name = thisOpt->argument;
            break;
         case 'm':
            minimum_contig_length = atoi(thisOpt->argument);
            break;
         case 'd':
            dmin = atoi(thisOpt->argument);
            break;
         case 'c':
            chunk_size = atoi(thisOpt->argument);
            break;
         case 's':
            string_size = thisOpt->argument;
            break;
         case 'l':
            load_factor = atoi(thisOpt->argument);
            break;
         case 'f':
            oracle_file = thisOpt->argument;
            break;
         default:
            break;
      }
      
      free(thisOpt);
   }
   
   UPC_TICK_T start, end;

   if (MYTHREAD == 0) {
#ifndef NO_PAD
      printf("Struct size is %ld, with %ld padding, %ld shared[] ptr\n", sizeof(list_t), UPC_PADDING, sizeof(shared void*));
#else
      printf("Struct size is %ld, no padding, %ld shared[] ptr\n", sizeof(list_t), sizeof(shared void*));
#endif
   }
   if (strlen(output_name) > 200) {
      if (MYTHREAD == 0) printf("Please choose a shorter output name max 200 chars\n");
      upc_global_exit(1);
   }
   double con_time, trav_time, blastmap_time;
   char outputfile_name[255];
   sprintf(outputfile_name,"output_%d_%s", MYTHREAD, output_name);
   
#if VERBOSE > 0
   char logfile_name[255];
   sprintf(logfile_name,"log_%d_%s", MYTHREAD, output_name);

   mylog = fopen(logfile_name, "w");
#else
   mylog = NULL;
#endif

#ifdef SYNC_PROTOCOL
   if (MYTHREAD == 0)
      printf("\n\n*************** RUNNING VERSION WITH SYNCHRONIZATION PROTOCOL ENABLED ***************\n\n");
#endif
   
#ifndef SYNC_PROTOCOL
   if (MYTHREAD == 0)
      printf("\n\n*************** RUNNING VERSION WITH SYNCHRONIZATION PROTOCOL DISABLED ***************\n\n");
#endif

#ifdef DEBUG
   if (MYTHREAD == 0)
      printf("\n\n*************** RUNNING VERSION WITH DEBUG assertions and extra checks ***************\n\n");
#endif

#if VERBOSE > 0
   if (MYTHREAD == 0)
      printf("\n\n*************** RUNNING LOGGED VERBOSE (%d) OUTPUT ***********************************\n\n", VERBOSE);
#endif


#ifdef PROFILE
   upc_barrier;
   /* Time the construction of the hashtable */
   if (MYTHREAD == 0) {
      start = UPC_TICKS_NOW();
   }
#endif

   /* Build UFX hashtable */
   FILE *fdInput;
   int64_t myshare;
   int dsize;
   fdInput = UFXInitOpen(input_UFX_name, &dsize, &myshare, THREADS, MYTHREAD, &size);
   if (MYTHREAD == 0) {
      int64_t minMemory = 12*(sizeof(list_t) + sizeof(int64_t)) * size / 10 / 1024/ 1024/ THREADS;
      printf("Minimum required shared memory: %ld MB. (%ld ufx kmers) If memory runs out re-run with more -shared-heap\n", minMemory, size);
      fflush(stdout);
   }
   _trace("myshare[%ld] dsize[%d] size[%ld] dmin[%d] chunk_size[%d] load_factor[%d]", myshare, dsize, size, dmin, chunk_size, load_factor);

   dist_hashtable = buildUFXhash(size, fdInput, &memory_heap, myshare, dsize, dmin, chunk_size, load_factor);

#ifdef PROFILE
   /* Time the construction of the hashtable */
   if (MYTHREAD == 0) {
      end = UPC_TICKS_NOW();
      con_time = UPC_TICKS_TO_SECS(end-start);
      printf("\n\n*********** OVERALL TIME BREAKDOWN ***************\n\n");
      printf("\nTime for constructing UFX hash table is : %f seconds\n", con_time);
      fflush(stdout);
   }
#endif
   
   /* UU-mer graph traversal */

   /* initialze global shared variables */
   if (MYTHREAD == 0) {
      contig_id = 0;
      contig_list = NULL;
      upc_fence;
   }
   
#ifdef DISABLE_SAVE_RESULTS
   my_out_file = NULL;
#else
   my_out_file = fopen(outputfile_name,"w+");
#endif
   
#ifdef PROFILE
   upc_barrier;
   
   /* Time the UU-graph traversal */
   if (MYTHREAD == 0) {
      start = UPC_TICKS_NOW();
   }
#endif

   UU_graph_traversal(dist_hashtable, my_out_file, minimum_contig_length);
   
#ifdef PROFILE
   upc_barrier;
   /* Time the UU-graph traversal */
   if (MYTHREAD == 0) {
      end = UPC_TICKS_NOW();
      trav_time = UPC_TICKS_TO_SECS(end-start);
      printf("\nTime for UU-graph traversal is : %f seconds\n", trav_time);
      printf("\nTotal time is :  %f seconds\n", trav_time + con_time);
      printf("\nTotal contigs stored: %ld\n", contig_id);
   }
#endif

#ifdef IO_TIME_PROFILE
   upc_barrier;
   ioTimes = (shared[1] double*) upc_all_alloc(THREADS, sizeof(double));
   upc_barrier;
   ioTimes[MYTHREAD] = fileIOTime;
   upc_barrier;
   upc_fence;
   if (MYTHREAD == 0) {
      max_IO = ioTimes[0];
      for (i=0; i<THREADS; i++)
         if ( ioTimes[i] > max_IO)
            max_IO = ioTimes[i];
      printf("\nMaximum IO time : %f seconds\n", max_IO);
   }
   upc_all_free(ioTimes);
   upc_barrier;
#endif

#ifdef CONTIG_LOCK_ALLOC_PROFILE
   upc_barrier;
   printf("\nTime in contig lock allocation is : %f seconds\n", lockAllocTime);
   upc_barrier;
#endif
   
#ifdef DETAILED_BUILD_PROFILE
   /* Measure load imbalance in blastmap */
   upc_barrier;
   ioTimes = (shared[1] double*) upc_all_alloc(THREADS, sizeof(double));
   storeTimes = (shared[1] double*) upc_all_alloc(THREADS, sizeof(double));
   calcTimes = (shared[1] double*) upc_all_alloc(THREADS, sizeof(double));
   setupTimes = (shared[1] double*) upc_all_alloc(THREADS, sizeof(double));
   upc_barrier;
   ioTimes[MYTHREAD] = fileIOTime;
   setupTimes[MYTHREAD] = setupTime;
   storeTimes[MYTHREAD] = storeTime;
   calcTimes[MYTHREAD] = cardCalcTime;
   upc_barrier;
   upc_fence;
   
   if (MYTHREAD == 0) {
      min_IO = ioTimes[0];
      max_IO = ioTimes[0];
      min_setup = setupTimes[0];
      max_setup = setupTimes[0];
      min_store = storeTimes[0];
      max_store = storeTimes[0];
      min_calc = calcTimes[0];
      max_calc = calcTimes[0];
      
      
      for (i=0; i<THREADS; i++) {
         sum_ios += ioTimes[i];
         sum_stores += storeTimes[i];
         sum_setups += setupTimes[i];
         sum_calcs += calcTimes[i];
         
         if ( ioTimes[i] < min_IO) {
            min_IO = ioTimes[i];
         }
         
         if ( ioTimes[i] > max_IO) {
            max_IO = ioTimes[i];
         }
         
         if ( storeTimes[i] < min_store) {
            min_store = storeTimes[i];
         }
         
         if ( storeTimes[i] > max_store) {
            max_store = storeTimes[i];
         }
         
         if ( calcTimes[i] < min_calc) {
            min_calc = calcTimes[i];
         }
         
         if ( calcTimes[i] > max_calc) {
            max_calc = calcTimes[i];
         }
         
         if ( setupTimes[i] < min_setup) {
            min_setup = setupTimes[i];
         }
         
         if ( setupTimes[i] > max_setup) {
            max_setup = setupTimes[i];
         }
         
      }
      printf("\n******** CONSTRUCTION DETAILED STATISTICS ********\n");
      printf("IOs:                       Avg %f\tMin %f\tMax %f\n",sum_ios/THREADS, min_IO, max_IO);
      printf("Calculating cardinalities: Avg %f\tMin %f\tMax %f\n",sum_calcs/THREADS, min_calc, max_calc);
      printf("Setting up:                Avg %f\tMin %f\tMax %f\n",sum_setups/THREADS, min_setup, max_setup);
      printf("Storing k-mers:            Avg %f\tMin %f\tMax %f\n",sum_stores/THREADS, min_store, max_store);
#ifdef LHS_PERF
      printf("Percentage of node-local lookups: %f %%\n", (100.0*success)/(1.0 * lookups));
#endif
      
   }
   
   upc_barrier;

#endif

   upc_barrier;
   if (my_out_file != NULL)
      fclose(my_out_file);
   
#ifdef PROFILE
#ifdef WORKLOAD_PROFILING
   if (MYTHREAD == 0) {
      printf("\n------------------------- STATS for UU graph traversal ----------------------------\n");
   }
   upc_barrier;
   fclose(my_out_file);
   printf("Thread %d added %ld bases in total\n", MYTHREAD, bases_added);
#endif
#endif

#ifdef UU_TRAV_PROFILE
   printf("Thread %d spent %f seconds in UU-graph traversal, %f seconds in walking (%f %%)\n", MYTHREAD, UU_time, walking_time, walking_time/UU_time*100.0);
#endif

   upc_barrier;

#ifdef DETAILED_UU_TRAV_PROFILE
    upc_barrier;
    UU_times_all = (shared[1] double*) upc_all_alloc(THREADS, sizeof(double));
    walking_times_all = (shared[1] double*) upc_all_alloc(THREADS, sizeof(double));
    upc_barrier;
    UU_times_all[MYTHREAD] = UU_time;
    walking_times_all[MYTHREAD] = walking_time;
    upc_barrier;
    upc_fence;
    
    if (MYTHREAD == 0) {
        min_UU_time = UU_times_all[0];
        min_walking_time = walking_times_all[0];
        max_UU_time = UU_times_all[0];
        max_walking_time = walking_times_all[0];

        for (i=0; i<THREADS; i++) {
            sum_UU_time += UU_times_all[i];
            sum_walking_time += walking_times_all[i];
            
            if ( walking_times_all[i] < min_walking_time ) {
               min_walking_time = walking_times_all[i];
            }
            
            if ( UU_times_all[i] < min_UU_time ) {
               min_UU_time = UU_times_all[i];
            }

            if ( walking_times_all[i] > max_walking_time ) {
               max_walking_time = walking_times_all[i];
            }
            
            if ( UU_times_all[i] > max_UU_time ) {
               max_UU_time = UU_times_all[i];
            }
        }
        printf("\n******** TRAVERSAL DETAILED STATISTICS ********\n");
        printf("Total traversal: Avg %f\tMin %f\tMax %f\n", sum_UU_time / THREADS, min_UU_time, max_UU_time);
        printf("Walking time:    Avg %f\tMin %f\tMax %f\n", sum_walking_time / THREADS, min_walking_time, max_walking_time);
    }

    upc_barrier;

#endif /* DETAILED_UU_TRAV_PROFILE */

   if (mylog != NULL)
      fclose(mylog);
   
#ifndef DISABLE_SAVE_RESULTS

   /* Print some metafiles used in downstream steps */
   if ( MYTHREAD == 0 ) {
      char countFileName[255];
      sprintf(countFileName,"nUUtigs_%s.txt", output_name);
      FILE *countFD = fopen(countFileName, "w+" );
      fprintf(countFD, "%ld\n", contig_id);
      fclose(countFD);
   }
   
   char mycountFileName[255];
   sprintf(mycountFileName,"myUUtigs_%s_%d.txt", output_name, MYTHREAD);
   FILE *mycountFD = fopen(mycountFileName, "w+" );
   fprintf(mycountFD, "%d\n", myContigs);
   fclose(mycountFD);

#endif
   
   upc_barrier;

#ifdef PAPYRUSKV
   papyruskv_finalize();
   bupc_exit(0);
#endif /* PAPYRUSKV */

   return 0;
}
