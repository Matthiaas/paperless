#ifndef UU_TRAVERSAL_H
#define UU_TRAVERSAL_H

#include <assert.h>

#include "meraculous.h"
#include "kmer_hash.h"
#include "wtimer.h"

extern shared int64_t contig_id;
extern shared[BS] contig_ptr_t *contig_list;

#ifdef PROFILE
extern int64_t bases_added;
#endif

#if defined(UU_TRAV_PROFILE) || defined(DETAILED_UU_TRAV_PROFILE)
extern double walking_time;
extern double UU_time;
#endif

#ifdef CONTIG_LOCK_ALLOC_PROFILE
extern double lockAllocTime;
#endif

#ifdef MARK_POS_IN_CONTIG
/* Stores the exact position of each kmer in the eventual contig */
int markPosInContig(contig_ptr_t in_contig, hash_table_t *hashtable)
{
	contig_t *cur_contig = (contig_t *) in_contig;
	int i;
	int contig_length = cur_contig->end - cur_contig->start + 1;
	char *contig = (char*) (cur_contig->sequence + cur_contig->start);
	char cur_kmer[KMER_LENGTH+1], auxiliary_kmer[KMER_LENGTH+1];
	char *kmer_to_search;
	auxiliary_kmer[KMER_LENGTH] = '\0';
	cur_kmer[KMER_LENGTH] = '\0';
	shared[] list_t *lookup_res;
	
	for (i=0; i <= contig_length - KMER_LENGTH; i++) {
		memcpy(cur_kmer, contig+i, KMER_LENGTH * sizeof(char));
		kmer_to_search = getLeastKmer(curr_kmer, auxiliary_kmer);
		lookup_res = lookup_kmer(hashtable, (unsigned char*) kmer_to_search);
		
		/* Mark the k-mer with the position in the contig */
		lookup_res->posInContig = i;
		lookup_res->my_contig = cur_contig->myself;
	}
	
	upc_fence;
	return 0;
	
}
#endif

void reallocateContig(contig_ptr_t cur_contig, int64_t new_size, int64_t new_start) {
	int64_t new_end, cur_length = cur_contig->end - cur_contig->start + 1;
	shared[] char *prev_buffer;
	// ensure proper room for '\0' at end...
	if (new_size < new_start + cur_length + 1)
		new_size = new_start + cur_length + 1;

	new_end = new_start + cur_length - 1;
	assert(new_size > new_end + 1);
	
	prev_buffer = cur_contig->sequence;
	cur_contig->sequence = (shared[] char*) upc_alloc(new_size * sizeof(char));
	if (cur_contig->sequence == NULL)
		printf("upc_alloc failed!!!\n");
	
	if (VERBOSE > 2) LOG( "Thread %d: reallocating %ld from %ld (length %ld) to %ld (start %ld)\n", MYTHREAD, cur_contig->contig_id, cur_contig->size, cur_length, new_size, new_start);
	
	memcpy((char*) &(cur_contig->sequence[new_start]), (char*) &(prev_buffer[cur_contig->start]), cur_length * sizeof(char));
	cur_contig->start = new_start;
	cur_contig->end = new_end;
	cur_contig->size = new_size;
	upc_free(prev_buffer);
	upc_fence;
	
}

void reallocateContigIfNeededLR(contig_ptr_t cur_contig, int leftBases, int rightBases, float growthFactor)
{
	int64_t cur_length, new_size;
	int64_t new_start, new_end;
	
	
	if ( cur_contig->start - leftBases <= 1 || cur_contig->end + rightBases >= (cur_contig->size-2) ) {
		/* Should reallocate space for contig buffer */
		new_size = ceil(growthFactor * cur_contig->size) + leftBases + rightBases + 2;
		cur_length = cur_contig->end - cur_contig->start + 1;

		new_start = new_size/2 - cur_length/2;
		if (new_start <= leftBases) {
			new_start = leftBases + 1;
		}
		new_end = new_start + cur_length - 1;
		if (new_size - new_end -1 <= rightBases) {
			new_start -= (rightBases + 1 - (new_size - new_end - 1));
			new_end = new_start + cur_length - 1;
		}
		
		reallocateContig(cur_contig, new_size, new_start);
	}
	assert(cur_contig->start > leftBases && cur_contig->size - 1 - cur_contig->end > rightBases);
}

void reallocateContigIfNeeded(contig_ptr_t cur_contig ) {
	reallocateContigIfNeededLR( cur_contig, 1, 2, INCR_FACTOR );
}

#ifdef PAPYRUSKV
list_t *getNextHashTableEntry(int hashtable, papyruskv_iter_t* it, int* init_it, list_t** kmer_seed_ptr)
{
    if (*init_it == 0) {
        papyruskv_iter_local(hashtable, it);
        *init_it = 1;
    } else papyruskv_iter_next(hashtable, it);

    if (*it != NULL) {
        papyruskv_get(hashtable, (const char*) (*it)->key, KMER_PACKED_LENGTH, (char**) kmer_seed_ptr, NULL);
    }
	return *kmer_seed_ptr;
}
#else
shared[] list_t *getNextHashTableEntry(hash_table_t *hashtable, int64_t *seed_index, shared[] list_t *old_kmer_seed_ptr)
{
	/***********************************************************************************************/
	/* Pick a kmer as seed from local kmers' buckets. We assume cyclic distribution of the buckets */
	/***********************************************************************************************/
	assert( upc_threadof( &(hashtable->table[*seed_index]) ) == MYTHREAD);
	assert( *seed_index < hashtable->size );
	
	if (old_kmer_seed_ptr != NULL && VERBOSE > 2) {
		kmer_and_ext_t kmer_and_ext;
		unpackKmerAndExtensions(old_kmer_seed_ptr, &kmer_and_ext);
		LOG("Thread %d: getNextHashTableEntry(%ld): starting with %s %d\n", MYTHREAD, *seed_index, getLeft(&kmer_and_ext), old_kmer_seed_ptr->used_flag);
	}
	
	shared[] list_t *new_kmer_seed_ptr = old_kmer_seed_ptr;
	
	if (new_kmer_seed_ptr == NULL) {
		new_kmer_seed_ptr = hashtable->table[*seed_index].head;
	} else {
		new_kmer_seed_ptr = new_kmer_seed_ptr->next;
	}
	
	while (new_kmer_seed_ptr == NULL && (*seed_index + THREADS) < hashtable->size) {
		*seed_index += THREADS;
		new_kmer_seed_ptr = hashtable->table[*seed_index].head;
	}
	
	if (VERBOSE > 2) {
		if (new_kmer_seed_ptr != NULL) {
			kmer_and_ext_t kmer_and_ext;
			unpackKmerAndExtensions(new_kmer_seed_ptr, &kmer_and_ext);
			LOG("Thread %d: getNextHashTableEntry(%ld): ending with %s %d\n", MYTHREAD, *seed_index, getLeft(&kmer_and_ext), new_kmer_seed_ptr->used_flag);
		} else {
			LOG("Thread %d: getNextHashTableEntry(%ld): reached the end\n", MYTHREAD, *seed_index);
		}
	}
	assert( new_kmer_seed_ptr == NULL || upc_threadof( new_kmer_seed_ptr ) == MYTHREAD );
	
	return new_kmer_seed_ptr;
}
#endif /* PAPYRUSKV */


#ifdef PAPYRUSKV
int isLeftKmerUU(int hashtable, kmer_and_ext_t *kmer_and_ext) {
#else
int isLeftKmerUU(hash_table_t *hashtable, kmer_and_ext_t *kmer_and_ext) {
#endif /* PAPYRUSKV */
	char seed_le, seed_re;
#ifdef PAPYRUSKV
    unsigned char packed_key[KMER_PACKED_LENGTH];
	list_t *lookup_res;
#else
	shared[] list_t *lookup_res;
#endif /* PAPYRUSKV */
	if (!isACGT(kmer_and_ext->left_ext)) {
		if (VERBOSE > 1) LOG("Thread %d: isLeftKmerUU(%s): not unique left ext\n", MYTHREAD, getLeft(kmer_and_ext));
		return 0;
	}
#ifdef PAPYRUSKV
	lookup_res = lookup_and_get_ext_of_kmer(hashtable, getLeft(kmer_and_ext), &seed_le, &seed_re, (unsigned char*) packed_key, NULL);
#else
	lookup_res = lookup_and_get_ext_of_kmer(hashtable, getLeft(kmer_and_ext), &seed_le, &seed_re);
#endif /* PAPYRUSKV */
#ifdef PAPYRUSKV
    _w0(10);
    if (lookup_res != NULL) papyruskv_free(lookup_res);
    _w1(10);
#endif /* PAPYRUSKV */

	if (lookup_res == NULL) {
		if (VERBOSE > 1) LOG("Thread %d: isLeftKmerUU(%s): no left kmer. okay\n", MYTHREAD, getLeft(kmer_and_ext));
		return 1;
	} else if (seed_re != kmer_and_ext->kmer[KMER_LENGTH-1] || !isACGT(seed_re)) {
		if (VERBOSE > 1) LOG("Thread %d: isLeftKmerUU(%s): invalid seed_re (%c,%c) exp: %c\n", MYTHREAD, getLeft(kmer_and_ext), seed_le, seed_re, kmer_and_ext->kmer[KMER_LENGTH-1]);
		return 0;
	}
	return 1;
}

/* expects KMER_LENGTH+2 string kmer_and_ext: le-kmer-re */
#ifdef PAPYRUSKV
int isRightKmerUU(int hashtable, kmer_and_ext_t *kmer_and_ext) {
#else
int isRightKmerUU(hash_table_t *hashtable, kmer_and_ext_t *kmer_and_ext) {
#endif /* PAPYRUSKV */
	char seed_le, seed_re;
#ifdef PAPYRUSKV
    unsigned char packed_key[KMER_PACKED_LENGTH];
	list_t *lookup_res;
#else
	shared[] list_t *lookup_res;
#endif /* PAPYRUSKV */
	if (!isACGT (kmer_and_ext->right_ext)) {
		if (VERBOSE > 1) LOG("Thread %d: isRightKmerUU(%s): not unique right ext\n", MYTHREAD, getLeft(kmer_and_ext));
		return 0;
	}
#ifdef PAPYRUSKV
	lookup_res = lookup_and_get_ext_of_kmer(hashtable, getRight(kmer_and_ext), &seed_le, &seed_re, (unsigned char*) packed_key, NULL);
#else
	lookup_res = lookup_and_get_ext_of_kmer(hashtable, getRight(kmer_and_ext), &seed_le, &seed_re);
#endif /* PAPYRUSKV */
#ifdef PAPYRUSKV
    _w0(10);
    if (lookup_res != NULL) papyruskv_free(lookup_res);
    _w1(10);
#endif /* PAPYRUSKV */

	if (lookup_res == NULL) {
		if (VERBOSE > 1) LOG("Thread %d: isRightKmerUU(%s): no right kmer. okay\n", MYTHREAD, getLeft(kmer_and_ext));
		return 1;
	} else if (seed_le != kmer_and_ext->kmer[0] || !isACGT(seed_le)) {
		if (VERBOSE > 1) LOG("Thread %d: isRightKmerUU(%s): invalid seed_le (%c,%c) exp: %c\n", MYTHREAD, getLeft(kmer_and_ext), seed_le, seed_re, kmer_and_ext->kmer[0]);
		return 0;
	}
	return 1;
}

#ifdef PAPYRUSKV
int isKmerUULocalCopy(int hashtable, unsigned char* packed_key, list_t *copy, char *new_seed_le, char *new_seed_re ) {
#else
int isKmerUULocalCopy(hash_table_t *hashtable, list_t *copy, char *new_seed_le, char *new_seed_re ) {
#endif /* PAPYRUSKV */
	kmer_and_ext_t kmer_and_ext;
	
#ifdef PAPYRUSKV
	unpackKmerAndExtensionsLocalCopy(packed_key, copy, &kmer_and_ext);
#else
	unpackKmerAndExtensionsLocalCopy(copy, &kmer_and_ext);
#endif /* PAPYRUSKV */
	*new_seed_le = kmer_and_ext.left_ext;
	*new_seed_re = kmer_and_ext.right_ext;
	return isLeftKmerUU(hashtable, &kmer_and_ext) && isRightKmerUU(hashtable, &kmer_and_ext);
}

#ifdef PAPYRUSKV
int isKmerUU(int hashtable, unsigned char* packed_key, list_t *new_kmer_seed_ptr, char *new_seed_le, char *new_seed_re )
#else
int isKmerUU(hash_table_t *hashtable, shared[] list_t *new_kmer_seed_ptr, char *new_seed_le, char *new_seed_re )
#endif /* PAPYRUSKV */
{
	list_t copy = *new_kmer_seed_ptr;
#ifdef PAPYRUSKV
	return isKmerUULocalCopy(hashtable, packed_key, &copy, new_seed_le, new_seed_re);
#else
	return isKmerUULocalCopy(hashtable, &copy, new_seed_le, new_seed_re);
#endif /* PAPYRUSKV */
}

#ifdef PAPYRUSKV
list_t *getNextUnusedUUKmer(int hashtable, papyruskv_iter_t* it, int *init_it, int64_t *seed_index, list_t *new_kmer_seed_ptr, char *new_seed_le, char *new_seed_re)
#else
shared[] list_t *getNextUnusedUUKmer(hash_table_t *hashtable, int64_t *seed_index, shared[] list_t *new_kmer_seed_ptr, char *new_seed_le, char *new_seed_re)
#endif /* PAPYRUSKV */
{
	kmer_and_ext_t kmer_and_ext;
	list_t copy;
	int seed_used_flag = USED;
	
	while (seed_used_flag == USED) {
#ifdef PAPYRUSKV
        unsigned char* packed_key;
        if (*init_it == 0) {
            _w0(0);
            papyruskv_iter_local(hashtable, it);
            _w1(0);
            *init_it = 1;
        } else {
            _w0(1);
            papyruskv_iter_next(hashtable, it);
            _w1(1);
        }

        _w0(2);
        if (*it == NULL) new_kmer_seed_ptr = NULL;
        else {
            packed_key = (unsigned char*) (*it)->key;
            papyruskv_get(hashtable, (const char*) packed_key, KMER_PACKED_LENGTH, (char**) &new_kmer_seed_ptr, NULL);
        }
        _w1(2);
#else
		new_kmer_seed_ptr = getNextHashTableEntry(hashtable, seed_index, new_kmer_seed_ptr);
#endif /* PAPYRUSKV */
		
		if (new_kmer_seed_ptr == NULL) {
			break;
		}
		
		copy = *new_kmer_seed_ptr;
		seed_used_flag = copy.used_flag;
		
		if (seed_used_flag == USED) {
			if (VERBOSE > 1) {
#ifdef PAPYRUSKV
				unpackKmerAndExtensionsLocalCopy(packed_key, &copy, &kmer_and_ext);
#else
				unpackKmerAndExtensionsLocalCopy(&copy, &kmer_and_ext);
#endif /* PAPYRUSKV */
				LOG( "Thread %d: getNextUnusedUUKmer already used kmer_and_ext: %s\n", MYTHREAD, getLeft(&kmer_and_ext));
			}
			continue;
		}
		
#ifdef PAPYRUSKV
		unpackKmerAndExtensionsLocalCopy(packed_key, &copy, &kmer_and_ext);
#else
		unpackKmerAndExtensionsLocalCopy(&copy, &kmer_and_ext);
#endif /* PAPYRUSKV */
		*new_seed_le = kmer_and_ext.left_ext;
		*new_seed_re = kmer_and_ext.right_ext;
		
		/* Do not use as seed unless both left and rigth kmers exist and agree */
		if ( ! (isLeftKmerUU(hashtable, &kmer_and_ext) && isRightKmerUU(hashtable, &kmer_and_ext) ) ) {
			if (VERBOSE > 1) {
				LOG("Thread %d: kmer is not UU to the left or right: %s\n", MYTHREAD, getLeft(&kmer_and_ext));
			}
			seed_used_flag = USED;
		}
		
		/*********************************************************************************************/
		/* If seed_used_flag == UNUSED thus far then the seed is valid and we should try to visit it */
		/*********************************************************************************************/
		
		if (seed_used_flag == UNUSED){
            _w0(3);
#ifdef PAPYRUSKV
            new_kmer_seed_ptr->used_flag = USED;
            papyruskv_update(hashtable, (const char*) packed_key, KMER_PACKED_LENGTH, NULL, 0xdeadcafe, NULL, 0UL, (void*) &seed_used_flag, sizeof(int));
#else
			seed_used_flag = UPC_ATOMIC_CSWAP_USED_FLAG(&(new_kmer_seed_ptr->used_flag), UNUSED, USED);
#endif /* PAPYRUSKV */
            _w1(3);
		}
		if (VERBOSE > 1) {
			if (seed_used_flag == UNUSED) {
				LOG( "Thread %d: getNextUnusedUUKmer claimed kmer_and_ext: %s\n", MYTHREAD, getLeft(&kmer_and_ext));
			} else {
				LOG( "Thread %d: getNextUnusedUUKmer was just used! kmer_and_ext: %s\n", MYTHREAD, getLeft(&kmer_and_ext));
			}
		}
#ifndef PAPYRUSKV_FENCE
		upc_fence;
#endif /* PAPYRUSKV */
	}
	return new_kmer_seed_ptr;
	
}



/* Performs the right walk on the UU-graph traversal */
#ifdef PAPYRUSKV
int walk_right(int dist_hashtable, contig_ptr_t cur_contig, char seed_re, char *last_right_extension_found, contig_ptr_t *right_contig)
#else
int walk_right(hash_table_t *dist_hashtable, contig_ptr_t cur_contig, char seed_re, char *last_right_extension_found, contig_ptr_t *right_contig)
#endif /* PAPYRUSKV */
{
	kmer_and_ext_t kmer_and_ext;
#ifdef PAPYRUSKV
    unsigned char packed_key[KMER_PACKED_LENGTH];
    unsigned char r_packed_key[KMER_PACKED_LENGTH];
	list_t *lookup_res = NULL, *right_kmer_ptr = NULL;
    papyruskv_pos_t pos, r_pos;
#else
	shared[] list_t *lookup_res = NULL, *right_kmer_ptr = NULL;
#endif /* PAPYRUSKV */
	list_t copy, right_copy;
	int is_least, is_right_least;
	int64_t kmer_used_flag;
	char new_seed_re, new_seed_le, right_le, right_re;
	int result;
	char cur_base, *tmpKmer;
	shared[] contig_ptr_box_list_t *contig_ptr_box = cur_contig->myself;

	kmer_used_flag = UNUSED;
	result = UNFINISHED;
	cur_base = '\0';
	
	assert( upc_threadof( cur_contig ) == MYTHREAD );
	assert(cur_contig->end - cur_contig->start + 1 >= KMER_LENGTH);
	assert( upc_threadof( cur_contig->sequence ) == MYTHREAD);

	// get the last kmer
	tmpKmer = (char*) &(cur_contig->sequence[cur_contig->end - KMER_LENGTH+1]);
#ifdef PAPYRUSKV
	lookup_res = lookup_least_kmer_and_copy(dist_hashtable, tmpKmer, (unsigned char*) packed_key, &copy, &is_least, NULL);
#else
	lookup_res = lookup_least_kmer_and_copy(dist_hashtable, tmpKmer, &copy, &is_least);
#endif /* PAPYRUSKV */
#ifdef LHS_PERF
   lookups++;
   if ( (MYTHREAD/CORES_PER_NODE) == (upc_threadof(lookup_res)/CORES_PER_NODE) ) success++;
#endif
	assert(lookup_res != NULL);
	assert(copy.used_flag == USED);
	assert(copy.my_contig != NULL);
	remote_assert( lookup_res->my_contig->contig == cur_contig );

#ifdef PAPYRUSKV
	unpackKmerAndExtensionsLocalCopyOrient((unsigned char*) packed_key, &copy, &kmer_and_ext, is_least);
#else
	unpackKmerAndExtensionsLocalCopyOrient(&copy, &kmer_and_ext, is_least);
#endif /* PAPYRUSKV */
	if (VERBOSE>1) {
		LOG("Thread %d: walk_right: Starting %.*s ext %s with %c\n", MYTHREAD, KMER_LENGTH, (char*) &(cur_contig->sequence[cur_contig->start]), getLeft(&kmer_and_ext), seed_re);
	}
	assert( kmer_and_ext.kmer[KMER_LENGTH-1] == cur_contig->sequence[cur_contig->end] );
	assert( kmer_and_ext.right_ext == seed_re );

	// get the next kmer to the right
#ifdef PAPYRUSKV
	lookup_res = lookup_least_kmer_and_copy(dist_hashtable, getRight(&kmer_and_ext), (unsigned char*) packed_key, &copy, &is_least, &pos);
#else
	lookup_res = lookup_least_kmer_and_copy(dist_hashtable, getRight(&kmer_and_ext), &copy, &is_least);
#endif /* PAPYRUSKV */
#ifdef LHS_PERF
   lookups++;
   if ( (MYTHREAD/CORES_PER_NODE) == (upc_threadof(lookup_res)/CORES_PER_NODE) ) success++;
#endif
	if (lookup_res == NULL) {
		if (VERBOSE > 1) {
			LOG("Thread %d: no right kmer for %s\n", MYTHREAD, getLeft(&kmer_and_ext));
		}
		return FINISHED;
	}

	while (kmer_used_flag == UNUSED && result == UNFINISHED) {
		assert(isACGT(seed_re));
		assert( lookup_res != NULL );
		right_kmer_ptr = NULL;
		tmpKmer = (char*) &(cur_contig->sequence[cur_contig->end - KMER_LENGTH + 1]);
		
		UPC_POLL;

		/* build the sequence */
#ifdef PAPYRUSKV
		unpackKmerAndExtensionsLocalCopyOrient((unsigned char*) packed_key, &copy, &kmer_and_ext, is_least);
#else
		unpackKmerAndExtensionsLocalCopyOrient(&copy, &kmer_and_ext, is_least);
#endif /* PAPYRUSKV */
		assert( kmer_and_ext.kmer[KMER_LENGTH-2] == cur_contig->sequence[cur_contig->end] );
		assert( kmer_and_ext.kmer[KMER_LENGTH-1] == seed_re );
		new_seed_le = kmer_and_ext.left_ext;
		new_seed_re = kmer_and_ext.right_ext;
		
		/* 
		 If the new kmer's left extension is not unique
		 or left extension is not compatible with the current contig,
		 do not visit */
		if ( !isACGT(new_seed_le)
			|| new_seed_le != cur_contig->sequence[cur_contig->end - KMER_LENGTH +1]) {
			if (VERBOSE > 0) {
				LOG( "Thread %d: walk_right finished. %s right has invalid extensions le (%c) l:%c r:%c\n", MYTHREAD, getLeft(&kmer_and_ext), cur_contig->sequence[cur_contig->end - KMER_LENGTH +1], new_seed_le, new_seed_re);
			}
			// Done with extension
			return FINISHED;
		}
		assert ( memcmp(getLeft(&kmer_and_ext), tmpKmer, KMER_LENGTH) == 0 );
		
		// good so far...
		// build and find the next kmer, check for UU
#ifdef PAPYRUSKV
		right_kmer_ptr = lookup_least_kmer_and_copy(dist_hashtable, getRight(&kmer_and_ext), (unsigned char*) r_packed_key, &right_copy, &is_right_least, &r_pos);
#else
		right_kmer_ptr = lookup_least_kmer_and_copy(dist_hashtable, getRight(&kmer_and_ext), &right_copy, &is_right_least);
#endif /* PAPYRUSKV */
#ifdef LHS_PERF
      lookups++;
      if ( (MYTHREAD/CORES_PER_NODE) == (upc_threadof(right_kmer_ptr)/CORES_PER_NODE) ) success++;
#endif
		if (right_kmer_ptr == NULL) {
			if (VERBOSE > 1) {
				LOG("Thread %d: walk_right finished. no right kmer %s, adding last base %c\n", MYTHREAD, getRight(&kmer_and_ext), new_seed_re);
			}
			// terminate walk, but add the last base, after locking kmer
			result = FINISHED;
			
		} else {
			/* get and validate the next kmer's extensions */
			if (is_right_least) {
				convertPackedCodeToExtension(right_copy.packed_extensions, &right_le, &right_re);
			} else {
				convertPackedCodeToExtension(right_copy.packed_extensions, &right_re, &right_le);
				reverseComplementBase(&right_le);
				reverseComplementBase(&right_re);
			}
			
			if ( !isACGT(right_le) || right_le != kmer_and_ext.kmer[0]) {
				if (VERBOSE > 1) {
					LOG( "Thread %d: walk_right finished. right ext kmer (%s) is not UU. exp_le:%c l:%c r:%c\n", MYTHREAD, getLeft(&kmer_and_ext), kmer_and_ext.kmer[0], new_seed_le, new_seed_re);
				}
				// terminate walk, do not add the last base
				return FINISHED;
			}
			
			if ( !isACGT(right_re) ) {
				if (VERBOSE > 1) {
					LOG("Thread %d: walk_right finished. right kmer %s, has invalid right ext.  adding last base %c\n", MYTHREAD, getRight(&kmer_and_ext), new_seed_re);
				}
				// terminate walk, but add the last base, after locking kmer
				result = FINISHED;
			}
		}
		
		kmer_used_flag = copy.used_flag;
		if (kmer_used_flag == USED) {
			assert( IS_VALID_UPC_PTR (copy.my_contig ) );
			if (VERBOSE > 1) LOG( "Thread %d: walk_right kmer %s is used by %ld!\n", MYTHREAD, getLeft(&kmer_and_ext), copy.my_contig == NULL ? -1 : (copy.my_contig->contig == NULL ? -2 : IS_VALID_UPC_PTR(copy.my_contig->contig) ? copy.my_contig->contig->contig_id : -3));
			break;
		}
		
		/* Now attempt to lock this kmer as used by this contig and add the base */
		
        _w0(5);
#ifdef PAPYRUSKV
        lookup_res->used_flag = USED;
        papyruskv_update(dist_hashtable, (const char*) packed_key, KMER_PACKED_LENGTH, &pos, 0xdeadcafe, (void*) &contig_ptr_box, sizeof(void*), (void*) &kmer_used_flag, sizeof(int64_t));
#else
		kmer_used_flag = UPC_ATOMIC_CSWAP_USED_FLAG(&(lookup_res->used_flag), UNUSED, USED);
#endif /* PAPYRUSKV */
        _w1(5);
		if (kmer_used_flag != UNUSED) {
			if (VERBOSE > 1) {
				LOG( "Thread %d: walk_right kmer %s is now used! ... expecting a lock in my future\n", MYTHREAD, getLeft(&kmer_and_ext));
			}
			break;
		}
		
		/* Fix the kmer to point to current contig box */
		assert( upc_threadof( contig_ptr_box ) == MYTHREAD );
		assert( contig_ptr_box->contig != NULL );
		assert( upc_threadof( contig_ptr_box->contig ) == MYTHREAD );
		assert( contig_ptr_box->contig->state == ACTIVE );
		assert( contig_ptr_box->next == NULL );
		assert( copy.my_contig == NULL );
		remote_assert( lookup_res->my_contig == NULL );
		lookup_res->my_contig = contig_ptr_box;
		
		/* Add the next base */
		reallocateContigIfNeeded(cur_contig);
		cur_contig->end++;
		
		/* Cur_base is the base to be added in the contig */
		cur_base = seed_re;
		/* Seed_re should be used to extend the contig with a right walk */
		seed_re = new_seed_re;
		cur_contig->sequence[cur_contig->end] = cur_base;
		cur_contig->sequence[cur_contig->end+1] = '\0';
		
		if (VERBOSE > 1) {
			LOG( "Thread %d: walk_right %s added %c to %ld\n", MYTHREAD, getLeft(&kmer_and_ext), cur_base, cur_contig->contig_id);
		}
		
#ifndef PAPYRUSKV_FENCE
		upc_fence;
#endif /* PAPYRUSKV */

		if (result == FINISHED)
			break;
		
		remote_assert( isRightKmerUU(dist_hashtable, &kmer_and_ext) );
		
		// shift for the next kmer
		lookup_res = right_kmer_ptr;
		copy = right_copy;
#ifdef PAPYRUSKV
        memcpy(packed_key, r_packed_key, KMER_PACKED_LENGTH);
        pos = r_pos;
#endif
		is_least = is_right_least;

#ifdef PROFILE
		bases_added++;
#endif
	}

	/* Return pointer to the right contig as well (needed for the synchronization protocol) */
	if (kmer_used_flag == USED) {
		if (VERBOSE > 0) LOG("Thread %d: walk_right Kmer is used %s\n", MYTHREAD, getLeft(&kmer_and_ext));
#ifdef PAPYRUSKV
		(*right_contig) = get_contig_of_kmer(dist_hashtable, (unsigned char*) packed_key, lookup_res, 0);
#else
		(*right_contig) = get_contig_of_kmer(lookup_res, 0);
#endif /* PAPYRUSKV */
		if (result == FINISHED) {
			// the last base was not added because the kmer was not acquired
			result = UNFINISHED;
		}
	}
	
	// disallow USED & FINSHED
	assert( kmer_used_flag == UNUSED || result == UNFINISHED);

	(*last_right_extension_found) = seed_re;
#ifndef PAPYRUSKV_FENCE
	upc_fence;
#endif /* PAPYRUSKV */
	
	return result;
}

/* Performs the left walk on the UU-graph traversal */
#ifdef PAPYRUSKV
int walk_left(int dist_hashtable, contig_ptr_t cur_contig, char seed_le, char *last_left_extension_found, contig_ptr_t *left_contig)
#else
int walk_left(hash_table_t *dist_hashtable, contig_ptr_t cur_contig, char seed_le, char *last_left_extension_found, contig_ptr_t *left_contig)
#endif /* PAPYRUSKV */
{
	kmer_and_ext_t kmer_and_ext;
#ifdef PAPYRUSKV
    unsigned char packed_key[KMER_PACKED_LENGTH];
    unsigned char l_packed_key[KMER_PACKED_LENGTH];
	list_t *lookup_res = NULL, *left_kmer_ptr = NULL;
    papyruskv_pos_t pos, l_pos;
#else
	shared[] list_t *lookup_res = NULL, *left_kmer_ptr = NULL;
#endif /* PAPYRUSKV */
	list_t copy, left_copy;
	int is_least, is_left_least;
	int64_t kmer_used_flag;
	char new_seed_re, new_seed_le, left_le, left_re;
	int result;
	char cur_base, *tmpKmer;
	shared[] contig_ptr_box_list_t *contig_ptr_box = cur_contig->myself;
	
	kmer_used_flag = UNUSED;
	result = UNFINISHED;
	cur_base = '\0';
	
	assert( upc_threadof( cur_contig ) == MYTHREAD );
	assert( cur_contig->end - cur_contig->start + 1 >= KMER_LENGTH );
	assert( upc_threadof( cur_contig->sequence ) == MYTHREAD );
	
	// get the last kmer
	tmpKmer = (char*) &(cur_contig->sequence[cur_contig->start]);
#ifdef PAPYRUSKV
	lookup_res = lookup_least_kmer_and_copy(dist_hashtable, tmpKmer, (unsigned char*) packed_key, &copy, &is_least, &pos);
#else
	lookup_res = lookup_least_kmer_and_copy(dist_hashtable, tmpKmer, &copy, &is_least);
#endif /* PAPYRUSKV */
#ifdef LHS_PERF
   lookups++;
   if ( (MYTHREAD/CORES_PER_NODE) == (upc_threadof(lookup_res)/CORES_PER_NODE) ) success++;
#endif
	assert(lookup_res != NULL);
	assert(copy.used_flag == USED);
	assert(copy.my_contig != NULL);
	remote_assert( lookup_res->my_contig->contig == cur_contig );
	
#ifdef PAPYRUSKV
	unpackKmerAndExtensionsLocalCopyOrient((unsigned char*) packed_key, &copy, &kmer_and_ext, is_least);
#else
	unpackKmerAndExtensionsLocalCopyOrient(&copy, &kmer_and_ext, is_least);
#endif /* PAPYRUSKV */
	if (VERBOSE>1) {
		LOG("Thread %d: walk_left: Starting %.*s ext %s with %c\n", MYTHREAD, KMER_LENGTH, (char*) &(cur_contig->sequence[cur_contig->start]), getLeft(&kmer_and_ext), seed_le);
	}
	assert( kmer_and_ext.left_ext == seed_le );
	assert( kmer_and_ext.kmer[0] == cur_contig->sequence[cur_contig->start] );
	
	// get the next kmer to the right
#ifdef PAPYRUSKV
	lookup_res = lookup_least_kmer_and_copy(dist_hashtable, getLeft(&kmer_and_ext), (unsigned char*) packed_key, &copy, &is_least, &pos);
#else
	lookup_res = lookup_least_kmer_and_copy(dist_hashtable, getLeft(&kmer_and_ext), &copy, &is_least);
#endif /* PAPYRUSKV */
#ifdef LHS_PERF
   lookups++;
   if ( (MYTHREAD/CORES_PER_NODE) == (upc_threadof(lookup_res)/CORES_PER_NODE) ) success++;
#endif
	if (lookup_res == NULL) {
		if (VERBOSE > 1) {
			LOG("Thread %d: no left kmer for %s\n", MYTHREAD, getLeft(&kmer_and_ext));
		}
		return FINISHED;
	}

	while(kmer_used_flag == UNUSED && result == UNFINISHED) {
		assert(isACGT(seed_le));
		assert( lookup_res != NULL );
		left_kmer_ptr = NULL;
		tmpKmer = (char*) &(cur_contig->sequence[cur_contig->start]);

		UPC_POLL;
		
		/* build the sequence */
#ifdef PAPYRUSKV
		unpackKmerAndExtensionsLocalCopyOrient((unsigned char*) packed_key, &copy, &kmer_and_ext, is_least);
#else
		unpackKmerAndExtensionsLocalCopyOrient(&copy, &kmer_and_ext, is_least);
#endif /* PAPYRUSKV */
		assert( kmer_and_ext.kmer[0] == seed_le );
		assert( kmer_and_ext.kmer[1] == cur_contig->sequence[cur_contig->start] );
		new_seed_le = kmer_and_ext.left_ext;
		new_seed_re = kmer_and_ext.right_ext;
		
		/*
		 If the new kmer's right extension is not unique
		 or right extension is not compatible with the current contig,
		 do not visit */
		if ( !isACGT(new_seed_le)
			|| new_seed_re != cur_contig->sequence[cur_contig->start + KMER_LENGTH-1]) {
			if (VERBOSE > 0) {
				LOG( "Thread %d: walk_left finished. %s left has invalid extensions le (%c) l:%c r:%c\n", MYTHREAD, getLeft(&kmer_and_ext), cur_contig->sequence[cur_contig->start], new_seed_le, new_seed_re);
			}
			// Done with extension
			return FINISHED;
		}
		assert ( memcmp(getRight(&kmer_and_ext), tmpKmer, KMER_LENGTH) == 0 );

		// good so far...
		// build and find the next kmer, check for UU
#ifdef PAPYRUSKV
		left_kmer_ptr = lookup_least_kmer_and_copy(dist_hashtable, getLeft(&kmer_and_ext), (unsigned char*) l_packed_key, &left_copy, &is_left_least, &l_pos);
#else
		left_kmer_ptr = lookup_least_kmer_and_copy(dist_hashtable, getLeft(&kmer_and_ext), &left_copy, &is_left_least);
#endif /* PAPYRUSKV */
#ifdef LHS_PERF
      lookups++;
      if ( (MYTHREAD/CORES_PER_NODE) == (upc_threadof(left_kmer_ptr)/CORES_PER_NODE) ) success++;
#endif
		if (left_kmer_ptr == NULL) {
			if (VERBOSE > 1) {
				LOG("Thread %d: walk_left finished. no left kmer %s, adding last base %c\n", MYTHREAD, getLeft(&kmer_and_ext), new_seed_le);
			}
			// terminate walk, but add the last base, after locking kmer
			result = FINISHED;
			
		} else {
			if (is_left_least) {
				convertPackedCodeToExtension(left_copy.packed_extensions, &left_le, &left_re);
			} else {
				convertPackedCodeToExtension(left_copy.packed_extensions, &left_re, &left_le);
				reverseComplementBase(&left_le);
				reverseComplementBase(&left_re);
			}
			
			if ( !isACGT(left_re) || left_re != kmer_and_ext.kmer[KMER_LENGTH-1]) {
				if (VERBOSE > 1) {
					LOG( "Thread %d: walk_left finished. left ext kmer (%s) is not UU. exp_le:%c l:%c r:%c\n", MYTHREAD, getLeft(&kmer_and_ext), cur_contig->sequence[cur_contig->end - KMER_LENGTH +1], new_seed_le, new_seed_re);
				}
				// terminate walk, do not add the last base
				return FINISHED;
			}
			
			if ( !isACGT(left_le) ) {
				if (VERBOSE > 1) {
					LOG("Thread %d: walk_left finished. left kmer %s, has invalid left ext.  adding last base %c\n", MYTHREAD, getLeft(&kmer_and_ext), new_seed_le);
				}
				// terminate walk, but add the last base, after locking kmer
				result = FINISHED;
			}
		}
		
		/* Now attempt to lock this kmer as used by this contig and add the base */
		
		kmer_used_flag = copy.used_flag;
		if (kmer_used_flag == USED) {
			assert( IS_VALID_UPC_PTR( copy.my_contig ) );
			if (VERBOSE > 1) LOG( "Thread %d: walk_left kmer %s is used by %ld!\n", MYTHREAD, getLeft(&kmer_and_ext), copy.my_contig == NULL ? -1 : (copy.my_contig->contig == NULL ? -2 : IS_VALID_UPC_PTR(copy.my_contig->contig) ? copy.my_contig->contig->contig_id : -3));
			break;
		}
		
        _w0(6);
#ifdef PAPYRUSKV
        lookup_res->used_flag = USED;
        papyruskv_update(dist_hashtable, (const char*) packed_key, KMER_PACKED_LENGTH, &pos, 0xdeadcafe, (void*) &contig_ptr_box, sizeof(void*), (void*) &kmer_used_flag, sizeof(int64_t));
#else
		kmer_used_flag = UPC_ATOMIC_CSWAP_USED_FLAG(&(lookup_res->used_flag), UNUSED, USED);
#endif /* PAPYRUSKV */
        _w1(6);
		if (kmer_used_flag != UNUSED) {
			if (VERBOSE > 1) LOG( "Thread %d: walk_left kmer %s is now used! ... expecting a lock in my future!\n", MYTHREAD, getLeft(&kmer_and_ext));
			break;
		}
		remote_assert( kmer_used_flag == UNUSED && lookup_res->used_flag == USED );
		
		/* Fix the kmer to point to current contig box */
		assert( upc_threadof( contig_ptr_box ) == MYTHREAD );
		assert( contig_ptr_box->contig != NULL );
		assert( upc_threadof( contig_ptr_box->contig ) == MYTHREAD );
		assert( contig_ptr_box->contig->state == ACTIVE );
		assert( contig_ptr_box->next == NULL );
		assert( copy.my_contig == NULL );
		remote_assert( lookup_res->my_contig == NULL );
		lookup_res->my_contig = contig_ptr_box;
		
		/* Add the next base */
		reallocateContigIfNeeded(cur_contig);
		cur_contig->start--;
		
		/* Set the new leftmost base */
		cur_base = seed_le;
		seed_le = new_seed_le;
		cur_contig->sequence[cur_contig->start] = cur_base;

		if (VERBOSE > 1) {
			LOG( "Thread %d: walk_left %s added %c to %ld\n", MYTHREAD, getLeft(&kmer_and_ext), cur_base, cur_contig->contig_id);
		}

#ifndef PAPYRUSKV_FENCE
		upc_fence;
#endif /* PAPYRUSKV */
		
		if (result == FINISHED)
			break;
		
		remote_assert( isLeftKmerUU(dist_hashtable, &kmer_and_ext) );

		// shift for the next kmer
		lookup_res = left_kmer_ptr;
		copy = left_copy;
#ifdef PAPYRUSKV
        memcpy(packed_key, l_packed_key, KMER_PACKED_LENGTH);
        pos = l_pos;
#endif
		is_least = is_left_least;
		
#ifdef PROFILE
		bases_added++;
#endif
		
	}
	
	/* Return pointer to the left contig as well (needed for the synchronization protocol) */
	if (kmer_used_flag == USED) {
		if (VERBOSE>0) LOG("Thread %d: walk_left Kmer is used %s\n", MYTHREAD, getLeft(&kmer_and_ext));
#ifdef PAPYRUSKV
		(*left_contig) = get_contig_of_kmer(dist_hashtable, (unsigned char*) packed_key, lookup_res, 0);
#else
		(*left_contig) = get_contig_of_kmer(lookup_res, 0);
#endif /* PAPYRUSKV */
		if (result == FINISHED) {
			// the last base was not added because the kmer was not acquired
			result = UNFINISHED;
		}
	}
	
	// disallow USED & FINSHED
	assert( kmer_used_flag == UNUSED || result == UNFINISHED);
	
	(*last_left_extension_found) = seed_le;
#ifndef PAPYRUSKV_FENCE
	upc_fence;
#endif /* PAPYRUSKV */

	return result;
}

/* unsafe in parallel; use only on local thread or when locked */
void add_contig_to_box_list(shared[] contig_ptr_box_list_t **contig_list, shared[] contig_ptr_box_list_t *new_box) {
	assert(contig_list != NULL);
	assert(new_box != NULL);
	assert(new_box->next == NULL);
	shared[] contig_ptr_box_list_t *tmp = *contig_list;
	
	new_box->next = *contig_list;
	*contig_list = new_box;
	
	if (VERBOSE>1) LOG( "Thread %d: add_contig_to_box_list added %ld (first %ld, last %ld)\n", MYTHREAD, new_box->contig->contig_id, (*contig_list)->contig->contig_id, tmp->contig->contig_id);
}

shared[] contig_ptr_box_list_t * alloc_new_contig_ptr_list_box(contig_ptr_t contig) {
	shared[] contig_ptr_box_list_t *new_box = (shared[] contig_ptr_box_list_t *) upc_alloc(sizeof(contig_ptr_box_list_t));
	if (new_box == NULL) {
		LOG( "FATAL ERROR: alloc failed to create a new contig box\n");
		exit(1);
	}
	new_box->next = NULL;
	new_box->contig = contig;
	new_box->original = contig;
	return new_box;
}

void printContigBox( shared[] contig_ptr_box_list_t *head ) {
	while (head != NULL) {
		LOG("Thread %d: printContigBox %ld (orig %ld) -> %ld\n", MYTHREAD, head->contig->contig_id, head->original->contig_id, head->next == NULL ? -1 : head->next->contig->contig_id);
		head = head->next;
	}
}

void assign_contig_to_box_list( shared[] contig_ptr_box_list_t *head, contig_ptr_t contig ) {
	shared[] contig_ptr_box_list_t *contig_list, *last;
	contig_ptr_box_list_t localCopy;
	int hasSelf, allAreClaimed;

	assert( head != NULL );
	assert( IS_VALID_UPC_PTR(head) );
	assert( IS_VALID_UPC_PTR(contig) );
	assert(head->contig != NULL && IS_VALID_UPC_PTR(head->contig));
	assert(head->original != NULL && IS_VALID_UPC_PTR(head->original));
	assert(head->contig == head->original);
	assert(head->contig->state == CLAIMED);
	assert(head->contig != contig);
	assert(contig != NULL);
	assert( upc_threadof( contig ) == MYTHREAD );
	assert(contig->myself != NULL);
	assert(contig->myself->contig == contig);
	assert(contig->state == ACTIVE);

	do {
		allAreClaimed = 1;
		hasSelf = 0;
		contig_list = head;
		last = NULL;
		while (contig_list != NULL) {
			UPC_POLL;
			if ( !IS_VALID_UPC_PTR(contig_list) ) {
				LOG("Thread %d: Warning: assign_contig_to_box_list detected corrupted contig_list\n", MYTHREAD);
				allAreClaimed = 0;
				break;
			}
			// copy the contig_ptr_box
			loop_until( localCopy = *contig_list, IS_VALID_UPC_PTR( localCopy.contig ) && IS_VALID_UPC_PTR( localCopy.original ) && IS_VALID_UPC_PTR(localCopy.next) );
			if (VERBOSE > 0) LOG( "Thread %d: reassigning box from %ld to %ld (orig: %ld)\n", MYTHREAD, localCopy.contig->contig_id, contig->contig_id, localCopy.original->contig_id);
			if (contig_list == contig->myself) {
				if (VERBOSE>0) LOG("Thread %d: found target contig in box list. skipping this entry\n", MYTHREAD);
				hasSelf = 1;
			} else {
				if (localCopy.original->state != CLAIMED || localCopy.contig->state != CLAIMED) {
					LOG("Thread %d: Warning: assign_contig_to_box_list detected unclaimed contig. retrying\n", MYTHREAD);
					allAreClaimed = 0;
					break;
				} else if (localCopy.contig != contig) {
					contig_list->contig = contig;
				}
			}
			last = contig_list;
			contig_list = localCopy.next;
			if (contig_list != last->next) {
				LOG("Thread %d: Warning assign_contig_to_box_list detected list modification while traversing!\n", MYTHREAD);
				allAreClaimed = 0;
				break;
			}
		}
		if (VERBOSE > 0 && !allAreClaimed) LOG("Thread %d: assign_contig_to_box_list found problem traversing contig list %ld to %ld (orig: %ld). repeating\n", MYTHREAD, head->contig->contig_id, contig->contig_id, head->original->contig_id);
		upc_fence;
	} while (!allAreClaimed);

	assert(contig_list == NULL && last != NULL && allAreClaimed);
	// preserve head as the next of contig->myself list, append all existing
	if (! hasSelf ) {
		assert(last->next == NULL);
		last->next = contig->myself->next;
		contig->myself->next = head;
	}
	upc_fence;
	if (VERBOSE>0) { printContigBox(head); printContigBox(contig->myself); }
}

#ifdef PAPYRUSKV
void clean_kmer_contig_ptr(int hashtable) {
#else
void clean_kmer_contig_ptr(hash_table_t *hashtable) {
#endif /* PAPYRUSKV */
	/* cleanup kmer boxes */
	int64_t seed_index = MYTHREAD;
#ifdef PAPYRUSKV
    unsigned char packed_key[KMER_PACKED_LENGTH];
	list_t *new_kmer_seed_ptr = NULL, *kmer_iterator = NULL;
#else
	shared[] list_t *new_kmer_seed_ptr = NULL, *kmer_iterator = NULL;
#endif /* PAPYRUSKV */
	list_t copy;
	kmer_and_ext_t kmer_and_ext;
	contig_t contig_copy;
	
#ifdef PAPYRUSKV
    papyruskv_iter_t it = NULL;
    _w0(7);
    papyruskv_iter_local(hashtable, &it);
    _w1(7);
    kmer_iterator = (list_t*) it->val;

	while (kmer_iterator != NULL) {
#else
	while ((kmer_iterator = getNextHashTableEntry(hashtable, &seed_index, kmer_iterator)) != NULL) {
#endif /* PAPYRUSKV */
		new_kmer_seed_ptr = kmer_iterator;
		
#ifndef PAPYRUSKV
		assert( upc_threadof( new_kmer_seed_ptr ) == MYTHREAD );
#endif
		copy = *new_kmer_seed_ptr;
#ifdef PAPYRUSKV
		unpackKmerAndExtensionsLocalCopy((unsigned char*) it->key, &copy, &kmer_and_ext);
#else
		unpackKmerAndExtensionsLocalCopy(&copy, &kmer_and_ext);
#endif /* PAPYRUSKV */
		
		if (copy.used_flag == USED) {
			assert(copy.my_contig != NULL);
			if (VERBOSE>0) LOG("Thread %d: clean_kmer_contig_ptr, kmer is used %s\n", MYTHREAD, getLeft(&kmer_and_ext));
#ifdef PAPYRUSKV
			contig_ptr_t best_contig = get_contig_of_kmer(hashtable, (unsigned char*) it->key, new_kmer_seed_ptr, 0);
#else
			contig_ptr_t best_contig = get_contig_of_kmer(new_kmer_seed_ptr, 0);
#endif /* PAPYRUSKV */
			assert(best_contig != NULL);
			contig_copy = *best_contig;
			
			remote_assert(contig_copy.myself->original == best_contig);
			assert(contig_copy.state == COMPLETE);
			
			// To be used at least one direction needs to be UU
			if ( !( isLeftKmerUU(hashtable, &kmer_and_ext) || isRightKmerUU(hashtable, &kmer_and_ext) ) ) {
				LOG("Thread %d: ERROR! Kmer is used in contig %ld but is not UU to the left or the right! %s\n", MYTHREAD, best_contig->contig_id, getLeft(&kmer_and_ext));
			}
			remote_assert( isLeftKmerUU(hashtable, &kmer_and_ext) && isRightKmerUU(hashtable, &kmer_and_ext) );
			
#ifdef DEBUG
			// test the tail of the contig pointer box
#ifdef PAPYRUSKV
			contig_ptr_t best_contig2 = get_contig_of_kmer( hashtable, (unsigned char*) it->key, new_kmer_seed_ptr, 1 );
#else
			contig_ptr_t best_contig2 = get_contig_of_kmer( new_kmer_seed_ptr, 1 );
#endif /* PAPYRUSKV */
			assert(best_contig == best_contig2);
			assert(best_contig2 != NULL);
			remote_assert(best_contig2 == best_contig->myself->contig);
			remote_assert(best_contig2->myself->next == NULL);
			remote_assert(best_contig2->myself->original == best_contig);
			remote_assert(best_contig2->state == COMPLETE);
#endif
			
			// assign the proper pointer box to this kmer
			if (copy.my_contig != best_contig->myself) {
				new_kmer_seed_ptr->my_contig = best_contig->myself;
#ifdef PAPYRUSKV
                _w0(8);
                papyruskv_put(hashtable, (const char*) it->key, KMER_PACKED_LENGTH, (const char*) new_kmer_seed_ptr, sizeof(list_t));
                _w1(8);
#endif /* PAPYRUSKV */
			}
			
			if ( VERBOSE > 2 ) {
				LOG( "Thread %d: state for kmer %s contig: %ld (%ld)\n", MYTHREAD, getLeft(&kmer_and_ext), new_kmer_seed_ptr->my_contig->contig->state, new_kmer_seed_ptr->my_contig->contig->contig_id );
			}
			
		} else { // UNUSED
			assert(copy.my_contig == NULL);
		}

#ifdef PAPYRUSKV
        _w0(9);
        papyruskv_iter_next(hashtable, &it);
        _w1(9);
        if (it == NULL) kmer_iterator = NULL;
        else kmer_iterator = (list_t*) it->val;
#endif /* PAPYRUSKV */
	}
	
#ifndef PAPYRUSKV_FENCE
	upc_fence;
#endif /* PAPYRUSKV */
	upc_barrier;
	if (VERBOSE > 0 && MYTHREAD == 0) LOG( "Thread %d: Cleaned all kmer contig references\n", MYTHREAD);

}

#ifdef PAPYRUSKV
void validate_kmers_and_contigs(int hashtable) {
#else
void validate_kmers_and_contigs(hash_table_t *hashtable) {
#endif /* PAPYRUSKV */
	int64_t seed_index = MYTHREAD;
	kmer_and_ext_t kmer_and_ext;
#ifdef PAPYRUSKV
	list_t *kmer_iterator = NULL;
    papyruskv_iter_t it = NULL;
    int init_it = 0;
	while ((kmer_iterator = getNextHashTableEntry(hashtable, &it, &init_it, &kmer_iterator)) != NULL) {
#else
	shared[] list_t *kmer_iterator = NULL;
	while ((kmer_iterator = getNextHashTableEntry(hashtable, &seed_index, kmer_iterator)) != NULL) {
		assert( upc_threadof( kmer_iterator ) == MYTHREAD );
#endif /* PAPYRUSKV */
		
		list_t copy = *kmer_iterator;
		if (copy.used_flag != USED)
			continue;
		
#ifdef PAPYRUSKV
		unpackKmerAndExtensionsLocalCopy((unsigned char*) it->key, &copy, &kmer_and_ext);
#else
		unpackKmerAndExtensionsLocalCopy(&copy, &kmer_and_ext);
#endif /* PAPYRUSKV */
		if (isLeftKmerUU(hashtable, &kmer_and_ext) || isRightKmerUU(hashtable, &kmer_and_ext)) {
			
			if ( VERBOSE > 1 ) {
				LOG( "Thread %d: state for kmer %s contig: %ld (%ld)\n", MYTHREAD, getLeft(&kmer_and_ext), kmer_iterator->my_contig->contig->state, kmer_iterator->my_contig->contig->contig_id );
			}
			
			assert(copy.used_flag == USED);
			assert(copy.my_contig != NULL);
			remote_assert(copy.my_contig->contig != NULL);
			remote_assert(copy.my_contig->next == NULL);
			remote_assert(copy.my_contig->contig->state == COMPLETE);
			
		}
	}
	upc_fence;
	upc_barrier;

}


#ifdef PAPYRUSKV
contig_ptr_t initialize_new_contig(unsigned char* packed_key, list_t *new_kmer_seed_ptr)
#else
contig_ptr_t initialize_new_contig(shared[] list_t *new_kmer_seed_ptr)
#endif /* PAPYRUSKV */
{
	kmer_and_ext_t kne;
	remote_assert(new_kmer_seed_ptr != NULL && new_kmer_seed_ptr->used_flag == USED);
	/***********************************/
	/* Initialize a new contig         */
	/* Assumes new_kmer_seed_ptr is UU */
	/***********************************/
	int minimum_size = (MINIMUM_CONTIG_SIZE > 3*KMER_LENGTH) ? MINIMUM_CONTIG_SIZE : 3*KMER_LENGTH;
#ifdef CONTIG_LOCK_ALLOC_PROFILE
	UPC_TICK_T start, end;
#endif
	
	contig_ptr_t cur_contig = (contig_ptr_t) upc_alloc(sizeof(contig_t));
	if (cur_contig == NULL)
		printf("ERROR: upc_alloc failed - can not allocate new contig\n");
	
	cur_contig->sequence = (shared[] char*) upc_alloc(minimum_size * sizeof(char));
	if (cur_contig->sequence == NULL)
		printf("ERROR: upc_alloc failed - can not allocate new contig string\n");
	
	cur_contig->start = minimum_size/2 - KMER_LENGTH/2;     // index of first base in contig
	cur_contig->end = cur_contig->start + KMER_LENGTH - 1;  // index of last base in contig
	cur_contig->size = minimum_size;                        // available size in the contig
	
	// kmerAndEExtension is sized KMER_LENGTH+2, so write just before start of contig...
	int kmer_ext_start = cur_contig->start - 1;
	assert(kmer_ext_start >= 0);
	assert( upc_threadof( &(cur_contig->sequence[kmer_ext_start]) ) == MYTHREAD );
	
#ifdef PAPYRUSKV
	unpackKmerAndExtensionsLocalCopy(packed_key, new_kmer_seed_ptr, &kne);
#else
	unpackKmerAndExtensions(new_kmer_seed_ptr, &kne);
#endif /* PAPYRUSKV */
	memcpy((char*) &(cur_contig->sequence[kmer_ext_start]), getLeft(&kne), KMER_LENGTH+2);
	
#ifdef PROFILE
	bases_added += KMER_LENGTH;
#endif
	cur_contig->is_circular = 0;
	cur_contig->state = ACTIVE;
#ifdef CONTIG_LOCK_ALLOC_PROFILE
	start = UPC_TICKS_NOW();
#endif
	cur_contig->state_lock = upc_global_lock_alloc();
#ifdef CONTIG_LOCK_ALLOC_PROFILE
	end = UPC_TICKS_NOW();
        lockAllocTime += UPC_TICKS_TO_SECS(end-start);
#endif
	cur_contig->contig_id = -1 - MYTHREAD;
	
	cur_contig->sequence[cur_contig->end+2] = '\0';
	if (VERBOSE > 1) LOG( "Thread %d: initialized new contig. %s \n", MYTHREAD, (char*) &(cur_contig->sequence[kmer_ext_start]));
	
	cur_contig->myself = alloc_new_contig_ptr_list_box(cur_contig);
	upc_fence;
	
	return cur_contig;
}

void destroy_contig(contig_ptr_t cur_contig) {
	/* free all the box pointers */
	remote_assert( cur_contig->state != COMPLETE && cur_contig->state != ACTIVE );
	
	// destroy my own box_ptr only
	if (cur_contig->myself != NULL && cur_contig->myself->original == cur_contig) {
		cur_contig->myself->contig = NULL;
		cur_contig->myself->next = NULL;
		upc_free(cur_contig->myself);
		cur_contig->myself = NULL;
	}
	
	cur_contig->myself = NULL;
	/* free the sequence */
	upc_free(cur_contig->sequence);
	cur_contig->sequence = NULL;
	/* free the structure */
	upc_free(cur_contig);
}

/* 
   find the canonical orientation for linear contigs
   for circular contigs, find the least kmer, 
   reverse complement if necessary and
   rearrange to start with that least kmer
 */
void canonicalize_contig(contig_ptr_t cur_contig) {
	assert( cur_contig != NULL );
	assert( upc_threadof( cur_contig ) == MYTHREAD );
	assert( cur_contig->state == COMPLETE );
	assert( upc_threadof( cur_contig->sequence ) == MYTHREAD);

	int64_t cur_length = cur_contig->end - cur_contig->start + 1;
	
	if (cur_contig->is_circular == 1) {
		
		/* 
		 FIXME TODO detect & verify cyclic repeat kmer (must be an edge)
		 mer is KMER_LENGTH-1
		 
		 straight circle
		 mer ... mer
		 
		 reverse complement:
		 mer ... rev
		 mer .......   mer == rev
		 ....... mer   mer == rev
		 
		 TODO find canonical kmer and shift appropriately
		*/
		char start[KMER_LENGTH], end[KMER_LENGTH], start_rev[KMER_LENGTH], end_rev[KMER_LENGTH];
		
		memcpy(start, (char*) (cur_contig->sequence + cur_contig->start), KMER_LENGTH-1);
		memcpy(start_rev, start, KMER_LENGTH-1);
		reverseComplementINPLACE(start_rev, KMER_LENGTH-1);
		memcpy(end, (char*) (cur_contig->sequence + cur_contig->end - (KMER_LENGTH-1) + 1), KMER_LENGTH-1);
		memcpy(end_rev, end, KMER_LENGTH-1);
		reverseComplementINPLACE(end_rev, KMER_LENGTH-1);
		
		start[KMER_LENGTH-1] = end[KMER_LENGTH-1] = start_rev[KMER_LENGTH-1] = end_rev[KMER_LENGTH-1] = '\0';
		
		
		int revcomp = -1;
		if (strcmp(start, start_rev) == 0 || strcmp(end, end_rev) == 0 || strcmp(start, end_rev) == 0) {
			revcomp = 1;
		} else if (strcmp(start, end) == 0) {
			revcomp = 0;
		}
	
		if (VERBOSE > 0 || revcomp < 0) {
			LOG( "Thread %d: circular contig %ld %s: %s\nstart:    %s\nrc_start: %s\nend:      %s\nrc_end:   %s\n", MYTHREAD, cur_contig->contig_id, (revcomp == 1 ? "revcomp" : (revcomp == 0 ? "straight" : "unknown")), (char*) (cur_contig->sequence + cur_contig->start), start, start_rev, end, end_rev );
		}
		if (revcomp == 1) {
			// edge is only repeated kmer, just take the canonical orientation
			if ( isLeastOrientation( (char*) &( cur_contig->sequence[ cur_contig->start ] ), cur_length ) == 0 ) {
				reverseComplementINPLACE( (char*) &( cur_contig->sequence[ cur_contig->start ] ), cur_length );
			}
		} else if (revcomp == 0) { // straight

			if (1) {

				if ( isLeastOrientation( (char*) &( cur_contig->sequence[ cur_contig->start ] ), cur_length ) == 0 ) {
					reverseComplementINPLACE( (char*) &( cur_contig->sequence[ cur_contig->start ] ), cur_length );
				}

			} else { // TODO test this code before rearranging contig.

				// find the least canonical kmer
				char *least_kmer, best[KMER_LENGTH+1], tmp[KMER_LENGTH+1];
				int64_t bestPos = 0;
				best[KMER_LENGTH] = tmp[KMER_LENGTH] = '\0';
				for(int64_t i = cur_contig->start; i < cur_contig->end - KMER_LENGTH + 1; i++) {
					least_kmer = getLeastKmer((char*) (cur_contig->sequence + i), tmp);
					if (i == cur_contig->start || strncmp(least_kmer, best, KMER_LENGTH) < 0) {
						memcpy(best, least_kmer, KMER_LENGTH);
						bestPos = i;
					}
				}

				if (isLeastKmer( (char*) (cur_contig->sequence + bestPos)) == 0) {
					reverseComplementINPLACE( (char*) &( cur_contig->sequence[ cur_contig->start ] ), cur_length );
					bestPos = cur_length - bestPos - KMER_LENGTH;
				}

				if (bestPos != cur_contig->start) {
					// shift
					char *tmpBuffer = (char*) malloc((cur_length + 1) * sizeof(char));
					memcpy(tmpBuffer, (char*) (cur_contig->sequence + bestPos), cur_contig->end - bestPos + 1);
					memcpy(tmpBuffer + bestPos - cur_contig->start, (char*) (cur_contig->sequence + cur_contig->start),  bestPos - cur_contig->start);
					memcpy((char*) (cur_contig->sequence + cur_contig->start), tmpBuffer, cur_length);
				}
			}
		}
		
	} else {
		// find canonical orientation
		if ( isLeastOrientation( (char*) &( cur_contig->sequence[ cur_contig->start ] ), cur_length ) == 0 ) {
			reverseComplementINPLACE( (char*) &( cur_contig->sequence[ cur_contig->start ] ), cur_length );
		}
	}

}

void finish_contig(contig_ptr_t cur_contig, FILE *output_file, int min_contig_length, shared[] contig_ptr_box_list_t **written, shared[] contig_ptr_box_list_t **notWritten)
{
	// assumes cur_contig->state_lock has been acquired previously!
	int64_t contig_no, cur_length = cur_contig->end - cur_contig->start + 1;
	assert( upc_threadof( cur_contig ) == MYTHREAD );
	assert( cur_contig->state == ACTIVE );
	// possibly reclaim some space and ensure one byte at end remains
	//if (cur_contig->size - cur_length >= MINIMUM_CONTIG_SIZE || cur_contig->size <= cur_contig->end + 1)
	//	reallocateContig(cur_contig, cur_length + 1, 0);
	
	assert( cur_contig->end+1 < cur_contig->size );
	cur_contig->sequence[(cur_contig->end)+1] = '\0';
	cur_contig->state = COMPLETE;
	
	canonicalize_contig(cur_contig);
	
	if ((cur_contig->end - cur_contig->start + 1) >= min_contig_length) {
		contig_no = UPC_ATOMIC_FADD_I64(&contig_id, 1);
      myContigs++;
		if (VERBOSE > 0) LOG( "Thread %d: Writing contig %ld (aka %ld), %ld bases\n", MYTHREAD, contig_no, cur_contig->contig_id, cur_contig->end - cur_contig->start + 1);
		
		cur_contig->contig_id = contig_no;
		if (output_file != NULL) {
		   if (cur_contig->is_circular == 1)
		      fprintf(output_file, ">CircularContig_%ld (len: %ld)\n%s\n", contig_no , cur_contig->end - cur_contig->start + 1, (char*) &(cur_contig->sequence[cur_contig->start]));
			else
		      fprintf(output_file, ">Contig_%ld\n%s\n", contig_no , (char*) &(cur_contig->sequence[cur_contig->start]));
		}
		if (written != NULL)
			add_contig_to_box_list(written, alloc_new_contig_ptr_list_box( cur_contig ));
	} else if (notWritten != NULL) {
		add_contig_to_box_list(notWritten, alloc_new_contig_ptr_list_box( cur_contig ));
	}
	
	upc_fence;
}

/* Performs the UU-graph traversal using the walk_right and walk_left routines */
#ifdef PAPYRUSKV
int UU_graph_traversal(int hashtable, FILE *output_file, int min_contig_length)
#else
int UU_graph_traversal(hash_table_t *hashtable, FILE *output_file, int min_contig_length)
#endif /* PAPYRUSKV */
{
	int64_t seed_index, cur_length, concat_length, new_end, new_start, new_size;
	int lex_ind;
	char unpacked_kmer[KMER_LENGTH+1];
	char seed_aux[KMER_LENGTH+1];
	seed_aux[KMER_LENGTH] = '\0';
	char auxiliary_unpacked_kmer[KMER_LENGTH+1];
	char packed_extensions;
	shared[] char *prev_buffer;
#ifdef PAPYRUSKV
    unsigned char packed_key[KMER_PACKED_LENGTH];
	list_t *kmer_iterator, *new_kmer_seed_ptr, *lookup_res;
#else
	shared[] list_t *kmer_iterator, *new_kmer_seed_ptr, *lookup_res;
#endif /* PAPYRUSKV */
	contig_ptr_t cur_contig;
	int64_t myThreadContigId;
	int64_t v, first, second, third;
	char seed_le, seed_re;
	contig_ptr_t right_contig, left_contig, first_contig, second_contig, third_contig;
	right_contig = NULL;
	left_contig = NULL;
	first_contig = NULL;
	second_contig = NULL;
	third_contig = NULL;
	int my_id, l_neighbor_id, r_neighbor_id, locks_to_obtain;
	char contig_tail[KMER_LENGTH+1];
	char fixing_endpoint[KMER_LENGTH+1];
	char auxiliary_fixing_kmer[KMER_LENGTH+1];
	char *kmer_to_search, *tmpKmer;
	contig_t copy_of_contig;
	int skip_right_check, left_state, right_state, right_ret_value, left_ret_value, walking;
	int64_t  ovlp_pos, my_start, chars_to_copy, my_end;
	char last_right_extension_found, last_left_extension_found, foo;
	char *real_contig;
	auxiliary_unpacked_kmer[KMER_LENGTH] = '\0';
	auxiliary_fixing_kmer[KMER_LENGTH] = '\0';
	unpacked_kmer[KMER_LENGTH] = '\0';
	char scratchpad[KMER_LENGTH+1];
	char auxiliary_kmer[KMER_LENGTH+1];
	auxiliary_kmer[KMER_LENGTH] = '\0';
	scratchpad[KMER_LENGTH] = '\0';
	contig_tail[KMER_LENGTH] = '\0';
	int attach_straight, attach_rev_compl, attaching_left, attaching_right;
	shared[] contig_ptr_box_list_t *my_contigs = NULL, *my_short_contigs = NULL, *failed_contigs = NULL, *tmp_contig_ptr_box;
	UPC_TICK_T start_UU, end_UU, start_walking, end_walking;
	
#ifdef DEBUG
	// first verify all kmers are UNUSED
	seed_index = MYTHREAD;
	kmer_iterator = NULL;
	int64_t numKmers = 0;
	while ((kmer_iterator = getNextHashTableEntry(hashtable, &seed_index, kmer_iterator)) != NULL) {
		assert(kmer_iterator->used_flag == UNUSED);
		assert(kmer_iterator->my_contig == NULL);
		assert( upc_threadof( kmer_iterator ) == MYTHREAD );
		numKmers++;
	}
	if (VERBOSE>0) LOG( "Thread %d: Has %ld kmers in my part of the hashtable\n", MYTHREAD, numKmers);
	upc_barrier;
#endif
	
#if defined(UU_TRAV_PROFILE) || defined(DETAILED_UU_TRAV_PROFILE)
	start_UU = UPC_TICKS_NOW();
#endif
	
    _wi();

	myThreadContigId = -1 - MYTHREAD;
	/* initialze seed_index and new_kmer_seed_ptr */
	seed_index = MYTHREAD;
	kmer_iterator = NULL;

#ifdef PAPYRUSKV
    papyruskv_iter_t it = NULL;
    int init_it = 0;
#endif /* PAPYRUSKV */
	
	while (1) {
#ifdef PAPYRUSKV
		kmer_iterator = getNextUnusedUUKmer(hashtable, &it, &init_it, &seed_index, kmer_iterator, &seed_le, &seed_re);
#else
		kmer_iterator = getNextUnusedUUKmer(hashtable, &seed_index, kmer_iterator, &seed_le, &seed_re);
#endif /* PAPYRUSKV */
		if (kmer_iterator == NULL)
			break;
#ifdef PAPYRUSKV
        unsigned char* packed_key = (unsigned char*) it->key;
#else
		assert( upc_threadof( kmer_iterator ) == MYTHREAD );
#endif /* PAPYRUSKV */
		new_kmer_seed_ptr = kmer_iterator;
		
		/**********************************************/
		/* Start building contig using the found seed */
		/**********************************************/
		UPC_POLL;
		assert(new_kmer_seed_ptr->used_flag == USED);
		
#ifdef PAPYRUSKV
		cur_contig = initialize_new_contig(packed_key, new_kmer_seed_ptr);
#else
		cur_contig = initialize_new_contig(new_kmer_seed_ptr);
#endif /* PAPYRUSKV */
		
		// incomplete contigs get temporary, unique, negative contig_id assigned
		cur_contig->contig_id = myThreadContigId;
		myThreadContigId -= THREADS;
		
		new_kmer_seed_ptr->my_contig = cur_contig->myself;
#ifdef PAPYRUSKV
        _w0(4);
        papyruskv_put(hashtable, (const char*) packed_key, KMER_PACKED_LENGTH, (const char*) new_kmer_seed_ptr, sizeof(list_t));
        _w1(4);
#endif /* PAPYRUSKV */
#ifndef PAPYRUSKV_FENCE
		upc_fence;
#endif /* PAPYRUSKV */
		
		/* Initialize a walk */
		walking = TRUE;
		right_ret_value = isACGT(seed_re) ? UNFINISHED : FINISHED;
		left_ret_value = isACGT(seed_le) ? UNFINISHED : FINISHED;
		walking = (right_ret_value == UNFINISHED) | (left_ret_value == UNFINISHED);
		while ( walking == TRUE ) {
			assert(upc_threadof(cur_contig) == MYTHREAD);
#ifndef PAPYRUSKV
			assert(upc_threadof(new_kmer_seed_ptr) == MYTHREAD);
#endif /* PAPYRUSKV */
			assert(new_kmer_seed_ptr->used_flag == USED);
			assert(upc_threadof(cur_contig->myself) == MYTHREAD);
			assert(new_kmer_seed_ptr->my_contig->contig == cur_contig);
			assert(cur_contig->state == ACTIVE);
			
			if (VERBOSE > 1) LOG( "Thread %d: Walking with contig sized %ld. %d %d\n", MYTHREAD, (cur_contig->end - cur_contig->start + 1), left_ret_value, right_ret_value);
			UPC_POLL;
			
#if defined(UU_TRAV_PROFILE) || defined(DETAILED_UU_TRAV_PROFILE)
			start_walking = UPC_TICKS_NOW();
#endif
			/* Walk right */
			if (right_ret_value == UNFINISHED) {
				right_contig = NULL;
				right_ret_value = walk_right(hashtable ,cur_contig,  seed_re, &last_right_extension_found, &right_contig);
				seed_re = last_right_extension_found;
			}
			
			UPC_POLL;
			
			/* Walk left */
			if (left_ret_value == UNFINISHED) {
				left_contig = NULL;
				left_ret_value = walk_left(hashtable, cur_contig,  seed_le, &last_left_extension_found, &left_contig);
				seed_le  = last_left_extension_found;
			}
			
			UPC_POLL;
			
#if defined(UU_TRAV_PROFILE) || defined(DETAILED_UU_TRAV_PROFILE)
			end_walking = UPC_TICKS_NOW();
			walking_time += UPC_TICKS_TO_SECS(end_walking-start_walking);
#endif
			
			/* Examine the states of the walks & store contig if both endpoints have been reached */
			/* otherwise follow synchronization protocol                                          */
			if ((right_ret_value == FINISHED) && (left_ret_value == FINISHED)) {
				
				upc_lock(cur_contig->state_lock);
            finish_contig(cur_contig, output_file, min_contig_length, &my_contigs, &my_short_contigs);
            
#ifdef MARK_POS_IN_CONTIG
            printf("[%s:%d]\n", __FILE__, __LINE__);
				markPosInContig(cur_contig, hashtable);
#endif
				upc_unlock(cur_contig->state_lock);
				walking = FALSE;
				
			} else { // walking needs to lock -- at least one kmer is already used
			 
				/* At least have to obtain my contig's lock */
				locks_to_obtain = 1;
				
				if (left_ret_value == UNFINISHED) {
					locks_to_obtain++;
					if (left_contig == NULL) { printf("FATAL ERROR: Left contig should be != NULL\n"); assert(0); }
					l_neighbor_id = (int) upc_threadof(left_contig);
					assert(l_neighbor_id >= 0 && l_neighbor_id < THREADS);
				} else {
					l_neighbor_id = -2;
				}
				
				if (right_ret_value == UNFINISHED) {
					locks_to_obtain++;
					skip_right_check = FALSE;
					if (right_contig == NULL) { printf("FATAL ERROR: Right contig should be != NULL\n"); assert(0); }
					r_neighbor_id = (int) upc_threadof(right_contig);
					assert(r_neighbor_id >= 0 && r_neighbor_id < THREADS);
				} else {
					r_neighbor_id = -1;
					skip_right_check = TRUE;
				}
				
				my_id = MYTHREAD;
				
				/* Naive way to sort the three thread_ids in descending order */
				if (my_id < l_neighbor_id) {
					if (my_id < r_neighbor_id) {
						if (l_neighbor_id < r_neighbor_id) {
							first = r_neighbor_id;
							first_contig = right_contig;
							second = l_neighbor_id;
							second_contig = left_contig;
							third = my_id;
							third_contig = cur_contig;
						} else {
							first = l_neighbor_id;
							first_contig = left_contig;
							second = r_neighbor_id;
							second_contig = right_contig;
							third = my_id;
							third_contig = cur_contig;
						}
					} else {
						first = l_neighbor_id;
						first_contig = left_contig;
						second = my_id;
						second_contig = cur_contig;
						third = r_neighbor_id;
						third_contig = right_contig;
					}
				} else {
					if (l_neighbor_id < r_neighbor_id) {
						if (my_id < r_neighbor_id) {
							first = r_neighbor_id;
							first_contig = right_contig;
							second = my_id;
							second_contig = cur_contig;
							third = l_neighbor_id;
							third_contig = left_contig;
						} else {
							first = my_id;
							first_contig = cur_contig;
							second = r_neighbor_id;
							second_contig = right_contig;
							third = l_neighbor_id;
							third_contig = left_contig;
						}
					} else {
						first = my_id;
						first_contig = cur_contig;
						second = l_neighbor_id;
						second_contig = left_contig;
						third = r_neighbor_id;
						third_contig = right_contig;
					}
				}

				assert(locks_to_obtain > 1);
				if (VERBOSE > 0) LOG( "Thread %d: obtaining %d locks. %ld %ld %ld\n", MYTHREAD, locks_to_obtain, first_contig->contig_id, second_contig->contig_id, locks_to_obtain > 2 ? third_contig->contig_id : 0);
				
				/* make sure all changes are refreshed before lock */
				upc_fence;

				/* Case where there are three locks to obtain */
				if (locks_to_obtain == 3) {
					if ((first_contig == second_contig) && (second_contig == third_contig)) {
						if (first_contig == NULL) printf("ERROR: Thread %d exhibits unexpected behavior while obtaining locks\n", MYTHREAD);
						if (VERBOSE > 0) LOG( "Thread %d: Found circular contig - 3 locks\n", MYTHREAD);
						/* Found a circlular contig */
						cur_contig->is_circular = 1;
						upc_lock(cur_contig->state_lock);
						
						finish_contig(cur_contig, output_file, min_contig_length, &my_contigs, &my_short_contigs);
						
#ifdef MARK_POS_IN_CONTIG
						markPosInContig(cur_contig, hashtable);
#endif
						
						upc_unlock(cur_contig->state_lock);
						/* Should terminate walk */
						left_ret_value = FINISHED;
						right_ret_value = FINISHED;
						walking = FALSE;
						
						// lock is released and contig is finished, so break
						break;
						
					} else if (first_contig == second_contig) {
						upc_lock(first_contig->state_lock);
						upc_lock(third_contig->state_lock);
					} else if (third_contig == second_contig) {
						upc_lock(first_contig->state_lock);
						upc_lock(second_contig->state_lock);
					} else if ((first_contig != second_contig) && (second_contig != third_contig) && (first_contig != third_contig)) {
						upc_lock(first_contig->state_lock);
						upc_lock(second_contig->state_lock);
						upc_lock(third_contig->state_lock);
					}
				}
				
				/* Case where there are two locks to obtain */
				if (locks_to_obtain == 2) {
					if (first_contig == second_contig) {
						if (first_contig == NULL) printf("ERROR: Thread %d exhibits unexpected behavior while obtaining locks\n", MYTHREAD);
						if (VERBOSE > 0) LOG( "Thread %d: Found circular contig\n", MYTHREAD);
						/* Found a circlular contig */
						cur_contig->is_circular = 1;
						upc_lock(cur_contig->state_lock);
						
						finish_contig(cur_contig, output_file, min_contig_length, &my_contigs, &my_short_contigs);
						
#ifdef MARK_POS_IN_CONTIG
						markPosInContig(cur_contig, hashtable);
#endif
						
						/* Should terminate walk */
						upc_unlock(cur_contig->state_lock);
						left_ret_value = FINISHED;
						right_ret_value = FINISHED;
						walking = FALSE;
						
						// lock is released, contig is finished, so break
						break;
						
					} else {
						upc_lock(first_contig->state_lock);
						upc_lock(second_contig->state_lock);
					}
				}
				
				/* Have obtained all locks in the neighborhood of contigs, so check condition of neighbors and decide what to do (state of current contig is ACTIVE) */
				assert (upc_threadof(cur_contig) == MYTHREAD);
				assert (cur_contig->state == ACTIVE);
				
				UPC_POLL;
				upc_fence;
				if (left_ret_value == UNFINISHED) {
					left_state = left_contig->state;
					if (VERBOSE > 0) LOG( "Thread %d: left_state: %d contig(%ld)\n", MYTHREAD, left_state, left_contig->contig_id);
				}
				if (right_ret_value == UNFINISHED) {
					right_state = right_contig->state;
					if (VERBOSE > 0) LOG( "Thread %d: right_state: %d contig(%ld)\n", MYTHREAD, right_state, right_contig->contig_id);
				}
					
				if (left_ret_value == UNFINISHED) {
					if (left_state == CLAIMED) {
						UPC_POLL;
						upc_fence;
						/* Just release locks and retry */
						skip_right_check = TRUE;
						if (VERBOSE > 0) LOG( "Thread %d: Found left CLAIMED state, retrying!!!\n", MYTHREAD);
						
					}
					
					else if (left_state == ABORTED) {
						UPC_POLL;
						if (VERBOSE > 0) LOG( "Thread %d: Found left ABORTED, claiming and attaching %ld\n", MYTHREAD, left_contig->contig_id);
						
						/* Attach aborted contig to my left */
						copy_of_contig = *left_contig;
						ovlp_pos = copy_of_contig.end - KMER_LENGTH + 1;
						my_start = cur_contig->start;
						assert( upc_threadof( cur_contig->sequence) == MYTHREAD );
						real_contig = (char*) cur_contig->sequence;
						chars_to_copy = ovlp_pos - copy_of_contig.start + 1;
						attaching_left = TRUE;
						
						/* Check if last KMER_LENGTH chars in left contigs are valid (since we don't know if the rc of subcontig has been generated) */
						upc_memget(scratchpad, &(copy_of_contig.sequence[copy_of_contig.end - (KMER_LENGTH -1)]), KMER_LENGTH * sizeof(char) );
						contig_tail[0] = last_left_extension_found;
						for (v=1; v < KMER_LENGTH; v++)
							contig_tail[v] = real_contig[my_start + v -1];
						
						if (memcmp(contig_tail, scratchpad, KMER_LENGTH * sizeof(char)) == 0) {
							attach_straight = TRUE;
							attach_rev_compl = FALSE;
						} else {
							upc_memget(scratchpad, &(copy_of_contig.sequence[copy_of_contig.start]), KMER_LENGTH * sizeof(char) );
							reverseComplementINPLACE( (char*) scratchpad, KMER_LENGTH);
							if (memcmp(contig_tail, scratchpad, KMER_LENGTH * sizeof(char)) == 0) {
								attach_rev_compl = TRUE;
								attach_straight = FALSE;
							} else {
								/* Should just set left_ret_value = FINISHED since I am falling in the "middle" of another contig and do NOT concatenate ABORTED */
								printf("Concatenation in middle for current contig %d and remote contig %d\n", (int) cur_contig->contig_id , (int) copy_of_contig.contig_id);
								attaching_left = FALSE;
								left_ret_value = FINISHED;
							}
						}
						
						if (attaching_left == TRUE) {
							
							/* Assure there is enough space to attach the contig in the current allocated buffer */
							reallocateContigIfNeededLR(cur_contig, chars_to_copy+MINIMUM_CONTIG_SIZE, MINIMUM_CONTIG_SIZE, INCR_FACTOR);
							
							// reset cached variables as they may have changed...
							my_start = cur_contig->start;
							assert( upc_threadof( cur_contig->sequence ) == MYTHREAD );
							real_contig = (char*) cur_contig->sequence;
							assert(((int)my_start - (int)chars_to_copy) >= 1);
							
							if ( attach_straight == TRUE ) {
								upc_memget((char*) &(cur_contig->sequence[my_start-chars_to_copy]), &(copy_of_contig.sequence[copy_of_contig.start]), chars_to_copy * sizeof(char) );
							}
							
							if ( attach_rev_compl == TRUE ) {
								upc_memget((char*) &(cur_contig->sequence[my_start-chars_to_copy]), &(copy_of_contig.sequence[copy_of_contig.end - chars_to_copy + 1]), chars_to_copy * sizeof(char) );
								reverseComplementINPLACE( (char*) &(cur_contig->sequence[my_start-chars_to_copy]), chars_to_copy );
							}
							
							// free aborted contig sequence
							assert(left_contig->sequence == copy_of_contig.sequence);
							upc_free(&(copy_of_contig.sequence[0]));
							left_contig->sequence = NULL;
							
							cur_contig->start = cur_contig->start - chars_to_copy;
							assert(cur_contig->start > 0);
							
							/* FIX CONTIG POINTER OF THE ATTACHED KMERS */
							for (v = 0; v < KMER_LENGTH; v++ )	
								scratchpad[v] = cur_contig->sequence[cur_contig->start+v];
							scratchpad[KMER_LENGTH] = '\0';
#ifdef PAPYRUSKV
							lookup_res = lookup_and_get_ext_of_kmer(hashtable, scratchpad, &seed_le, &foo, (unsigned char*) packed_key, NULL);
#else
							lookup_res = lookup_and_get_ext_of_kmer(hashtable, scratchpad, &seed_le, &foo);
#endif /* PAPYRUSKV */

							if (lookup_res == NULL) printf("FATAL ERROR in left CONCAT protocol from thread %d\n", MYTHREAD);
							assert(lookup_res != NULL);
							remote_assert(lookup_res->used_flag == USED);
							
							left_contig->state = CLAIMED;
							upc_fence;
							do {
								if (VERBOSE>0) LOG("Thread %d: attach left changing kmer %s from %ld to %ld\n", MYTHREAD, scratchpad, left_contig->contig_id, cur_contig->contig_id);
#ifdef PAPYRUSKV
								tmp_contig_ptr_box = get_contig_box_of_kmer(hashtable, (unsigned char*) packed_key, lookup_res, 0);
#else
								tmp_contig_ptr_box = get_contig_box_of_kmer(lookup_res, 0);
#endif /* PAPYRUSKV */
								remote_assert( tmp_contig_ptr_box->contig == left_contig );
								remote_assert( tmp_contig_ptr_box->next == NULL );
								assign_contig_to_box_list( left_contig->myself, cur_contig );
							} while (lookup_res->my_contig->contig != cur_contig);
							
							add_contig_to_box_list(&failed_contigs, alloc_new_contig_ptr_list_box( left_contig ));
							
							upc_fence;
#ifdef PAPYRUSKV
							remote_assert( get_contig_of_kmer(hashtable, (unsigned char*) packed_key, lookup_res, 0) == cur_contig );
							remote_assert( get_contig_of_kmer(hashtable, (unsigned char*) packed_key, lookup_res, 1) == cur_contig );
#else
							remote_assert( get_contig_of_kmer(lookup_res, 0) == cur_contig );
							remote_assert( get_contig_of_kmer(lookup_res, 1) == cur_contig );
#endif /* PAPYRUSKV */
							
							if (VERBOSE > 1) LOG( "Thread %d: claimed left %ld into active %ld. +%ld = %ld\n", MYTHREAD, left_contig->contig_id, cur_contig->contig_id, chars_to_copy, (cur_contig->end - cur_contig->start + 1));

#ifdef PAPYRUSKV
                            _w0(10);
                            if (lookup_res != NULL) papyruskv_free(lookup_res);
                            _w1(10);
#endif /* PAPYRUSKV */
						}
						
						if (right_ret_value == UNFINISHED) {
							skip_right_check = FALSE;
						} else {
							/* No right neighbor */
							seed_re = last_right_extension_found;
						}
					}
					
					else if (left_state == ACTIVE) {
						if (VERBOSE > 0) LOG( "Thread %d: Found left ACTIVE state.  Setting my contig %ld to aborted. Skipping right check.\n", MYTHREAD, cur_contig->contig_id);
						cur_contig->state = ABORTED;
						upc_fence;
						skip_right_check = TRUE;
						walking = FALSE;
					}
					
					else if (left_state == COMPLETE) {
						UPC_POLL;
						upc_fence;
						fprintf(stderr, "Thread %d: ERROR! Found left COMPLETE state! THIS SHOULD NOT HAPPEN!\n", MYTHREAD);
						assert(0);
						left_ret_value = UNFINISHED;
						skip_right_check = FALSE;
					}
					
					else {
						fprintf(stderr, "Thread %d: ERROR! Found UNDEFINED left state!!!\n", MYTHREAD);
						assert(0);
					}
				}
				
				if ((right_ret_value == UNFINISHED) && (skip_right_check == FALSE)) {

					right_state = right_contig->state;

					if (right_state == CLAIMED) {
						UPC_POLL;
						upc_fence;
						/* Just release locks and retry!!! */
						right_ret_value = UNFINISHED;
						if (VERBOSE > 0) LOG( "Thread %d: Found right CLAIMED state, retrying!!!\n", MYTHREAD);
					}
					
					else if (right_state == ABORTED) {
						UPC_POLL;
						if (VERBOSE > 0) LOG( "Thread %d: Found right ABORTED, claiming and attaching %ld\n", MYTHREAD, right_contig->contig_id);
						/* Attach aborted contig to my right */
						copy_of_contig = *right_contig;
						ovlp_pos = copy_of_contig.start;
						my_end = cur_contig->end;
						real_contig = (char*) cur_contig->sequence;
						chars_to_copy = copy_of_contig.end - (ovlp_pos+KMER_LENGTH-2);
						attaching_right = TRUE;
						
						/* Check if first KMER_LENGTH chars in right contigs are valid (since we don't know if the rc of subcontig has been generated) */
						upc_memget( scratchpad, &(copy_of_contig.sequence[copy_of_contig.start]), KMER_LENGTH * sizeof(char));
						contig_tail[KMER_LENGTH-1] = last_right_extension_found;
						for (v=0; v < KMER_LENGTH-1; v++)
							contig_tail[v] = real_contig[my_end - (KMER_LENGTH-2-v)];
						
						if (memcmp(contig_tail, scratchpad, KMER_LENGTH * sizeof(char)) == 0) {
							attach_straight = TRUE;
							attach_rev_compl = FALSE;
						} else {
							upc_memget(scratchpad, &(copy_of_contig.sequence[copy_of_contig.end-(KMER_LENGTH-1)]), KMER_LENGTH * sizeof(char) );
							reverseComplementINPLACE( (char*) scratchpad, KMER_LENGTH);
							if (memcmp(contig_tail, scratchpad, KMER_LENGTH * sizeof(char)) == 0) {
								attach_straight = FALSE;
								attach_rev_compl = TRUE;
							} else {
								printf("Concatenation in middle for current contig %d and remote contig %d\n", (int) cur_contig->contig_id , (int) copy_of_contig.contig_id);
								right_ret_value = FINISHED;
								attaching_right = FALSE;
							}
						}
						
						if (attaching_right == TRUE) {
							
							reallocateContigIfNeededLR(cur_contig, MINIMUM_CONTIG_SIZE, MINIMUM_CONTIG_SIZE+chars_to_copy, INCR_FACTOR);
							
							// resets cached variables as they might have changed
							my_end = cur_contig->end;
							real_contig = (char*) cur_contig->sequence;
							
							assert(cur_contig->size - 1 - cur_contig->end > chars_to_copy);
							/* There is enough space to attach the contig */
							
							if ( attach_straight == TRUE ) {
								upc_memget( (char*) &(cur_contig->sequence[my_end+1]), &(copy_of_contig.sequence[ovlp_pos-1+KMER_LENGTH]), chars_to_copy * sizeof(char));
							}
								
							if ( attach_rev_compl == TRUE ) {
								upc_memget( (char*) &(cur_contig->sequence[my_end+1]), &(copy_of_contig.sequence[copy_of_contig.start]), chars_to_copy * sizeof(char));
								reverseComplementINPLACE( (char*) &(cur_contig->sequence[my_end+1]), chars_to_copy );
							}
							
							// free aborted contig sequence
							assert( right_contig->sequence == copy_of_contig.sequence);
							upc_free(&(copy_of_contig.sequence[0]));
							right_contig->sequence = NULL;
							
							cur_contig->end = cur_contig->end + chars_to_copy;
							
							/* FIX CONTIG POINTER OF THE ATTACHED KMERS */
							for (v=0; v<KMER_LENGTH; v++)
								scratchpad[v] = cur_contig->sequence[cur_contig->end - (KMER_LENGTH - v - 1)];
							scratchpad[KMER_LENGTH] = '\0';
							
#ifdef PAPYRUSKV
							lookup_res = lookup_and_get_ext_of_kmer(hashtable, scratchpad, &foo, &seed_re, (unsigned char*) packed_key, NULL);
#else
							lookup_res = lookup_and_get_ext_of_kmer(hashtable, scratchpad, &foo, &seed_re);
#endif /* PAPYRUSKV */

							if (lookup_res == NULL) printf("FATAL ERROR in right CONCAT protocol from thread %d\n", MYTHREAD);
							assert(lookup_res != NULL);
							
							right_contig->state = CLAIMED;
							upc_fence;
							do {
								if (VERBOSE>0) LOG("Thread %d: attach right changing kmer %s from %ld to %ld\n", MYTHREAD, scratchpad, right_contig->contig_id, cur_contig->contig_id);

#ifdef PAPYRUSKV
								tmp_contig_ptr_box = get_contig_box_of_kmer(hashtable, (unsigned char*) packed_key, lookup_res, 0);
#else
								tmp_contig_ptr_box = get_contig_box_of_kmer(lookup_res, 0);
#endif /* PAPYRUSKV */
								remote_assert( tmp_contig_ptr_box->contig == right_contig );
								remote_assert( tmp_contig_ptr_box->next == NULL );
								assign_contig_to_box_list( right_contig->myself, cur_contig );
							} while (lookup_res->my_contig->contig != cur_contig );

							add_contig_to_box_list(&failed_contigs, alloc_new_contig_ptr_list_box( right_contig ));
							
							upc_fence;
#ifdef PAPYRUSKV
							remote_assert( get_contig_of_kmer(hashtable, (unsigned char*) packed_key, lookup_res, 0) == cur_contig );
							remote_assert( get_contig_of_kmer(hashtable, (unsigned char*) packed_key, lookup_res, 1) == cur_contig );
#else
							remote_assert( get_contig_of_kmer(lookup_res, 0) == cur_contig );
							remote_assert( get_contig_of_kmer(lookup_res, 1) == cur_contig );
#endif /* PAPYRUSKV */
							
							if (VERBOSE > 1) LOG( "Thread %d: claimed right %ld into active %ld. +%ld = %ld\n", MYTHREAD, right_contig->contig_id, cur_contig->contig_id, chars_to_copy, (cur_contig->end - cur_contig->start + 1));

#ifdef PAPYRUSKV
                            _w0(10);
                            if (lookup_res != NULL) papyruskv_free(lookup_res);
                            _w1(10);
#endif /* PAPYRUSKV */
						}
						
					}
					
					else if (right_state == ACTIVE) {
						UPC_POLL;
						/* Abort my contig */
						if (VERBOSE > 0) LOG( "Thread %d: Found right ACTIVE. Setting my contig %ld to aborted\n", MYTHREAD, cur_contig->contig_id);
						cur_contig->state = ABORTED;
						upc_fence;
						walking = FALSE;
						
					}
					
					else if (right_state == COMPLETE) {
						UPC_POLL;
						upc_fence;
						fprintf(stderr, "Thread %d: ERROR! Found right COMPLETE state! THIS SHOULD NOT HAPPEN\n", MYTHREAD);
						assert(0);
						right_ret_value = UNFINISHED;
					}
					
					else {
						fprintf(stderr, "Thread %d: ERROR! Found UNDEFINED right state!!!\n", MYTHREAD);
						assert(0);
					}
				}
				
				// release all acquired locks
				if (locks_to_obtain == 3) {
					if (first_contig == second_contig) {
						upc_unlock(third_contig->state_lock);
						upc_unlock(first_contig->state_lock);
					} else if (third_contig == second_contig) {
						upc_unlock(second_contig->state_lock);
						upc_unlock(first_contig->state_lock);
					} else if ((first_contig != second_contig) && (second_contig != third_contig) && (first_contig != third_contig)) {
						upc_unlock(third_contig->state_lock);
						upc_unlock(second_contig->state_lock);
						upc_unlock(first_contig->state_lock);
					}
				}
				
				if (locks_to_obtain == 2) {
					upc_unlock(second_contig->state_lock);
					upc_unlock(first_contig->state_lock);
				}
				
			} // walking needs to lock -- at least one kmer is already used
		} // while walking
		assert( cur_contig->state != ACTIVE );
		upc_fence;
		
	} // while there are seeds to process
	
	assert(kmer_iterator == NULL);
	
#if defined(UU_TRAV_PROFILE) || defined(DETAILED_UU_TRAV_PROFILE)
	end_UU = UPC_TICKS_NOW();
	UU_time = UPC_TICKS_TO_SECS(end_UU-start_UU);
#endif
	
	UPC_POLL;
	upc_fence;
	if (VERBOSE > 0) LOG( "Thread %d: Finished UU traversal\n", MYTHREAD);

	upc_barrier;
	
#ifdef STORE_CONTIGS_IN_ARRAY

	/* store all contigs in a globally indexed array by contig_id */
	contig_list = (shared[BS] contig_ptr_t *) upc_all_alloc(contig_id+1, sizeof(contig_ptr_t) );
	if (contig_list == NULL) {
		fprintf(stderr, "Thread %d: ERROR! Could not allocate memory for global contig list!\n", MYTHREAD);
		exit(1);
	}
	while ( my_contigs != NULL ) {
		tmp_contig_ptr_box = my_contigs;
		contig_ptr_t contig = my_contigs->contig;
		assert( upc_threadof( contig ) == MYTHREAD );
		assert( contig->state == COMPLETE );
		assert( contig->contig_id >= 0 );
		assert( upc_threadof( contig->myself ) == MYTHREAD );
		assert( contig->myself->next == NULL );
		contig_list[ contig->contig_id ] = contig;
		my_contigs = my_contigs->next;
		upc_free( tmp_contig_ptr_box );
	}
	upc_fence;
	upc_barrier;
	if (VERBOSE > 0 && MYTHREAD == 0) LOG( "Thread %d: Stored all long contigs\n", MYTHREAD);

	clean_kmer_contig_ptr(hashtable);
	
	/* destroy all the failed contigs */
	 
	while ( failed_contigs != NULL ) {
		tmp_contig_ptr_box = failed_contigs;
		failed_contigs = failed_contigs->next;
		if (VERBOSE>2) LOG( "Thread %d: Destroying %ld original(%ld)\n", MYTHREAD, tmp_contig_ptr_box->contig->contig_id, tmp_contig_ptr_box->original->contig_id);
		remote_assert( tmp_contig_ptr_box->contig->state == CLAIMED );

		destroy_contig( tmp_contig_ptr_box->contig );
		
		tmp_contig_ptr_box->contig = NULL;
		tmp_contig_ptr_box->next = NULL;
		upc_free( tmp_contig_ptr_box );
		
	}
	
	upc_fence;
	upc_barrier;
	
	if (VERBOSE > 0 && MYTHREAD == 0) LOG( "Thread %d: Destroyed all claimed contigs\n", MYTHREAD);
	
	if (MYTHREAD == 0)
		printf("All Finished UU traversal\n");
	
#ifdef DEBUG
	// last verify all USED kmers are UU in one direction and have contig set and COMPLETE
	validate_kmers_and_contigs(hashtable);
	
#endif
#endif

#if 0
    printf("[%d]0:%lf,1:%lf,2:%lf,3:%lf,4:%lf,5:%lf,6:%lf,7:%lf,8:%lf,9:%lf,10:%lf,11:%lf,12:%lf,13:%lf,14:%lf,15:%lf,16:%lf\n", MYTHREAD, _w(0), _w(1), _w(2), _w(3), _w(4), _w(5), _w(6), _w(7), _w(8), _w(9), _w(10), _w(11), _w(12), _w(13), _w(14), _w(15), _w(16));
#endif
	
	return 1;
}

#endif // UU_TRAVERSAL_H
