#ifndef KMER_HANDLING_H
#define KMER_HANDLING_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

int isACGT(const char c) {
	if (c == 'A' || c == 'C' || c == 'G' || c == 'T')
		return 1;
	else
		return 0;
}

void reverseComplementBase(char *base)
{
	switch ( base[0] ) {
		case 'A':
			base[0] = 'T';
			break;
		case 'C':
			base[0] = 'G';
			break;
		case 'G':
			base[0] = 'C';
			break;
		case 'T':
			base[0] = 'A';
			break;
		default:
			break;
	}
}

char reverseComplementBaseStrict(const char base) {
	char rc;
	switch(base) {
		case 'A':
			rc = 'T'; break;
		case 'C':
			rc = 'G'; break;
		case 'G':
			rc = 'C'; break;
		case 'T':
			rc = 'A'; break;
		default:
			fprintf(stderr, "unexpected base in revereseComplementBaseStrict: %c %d\n", base, (int) base);
			assert(0);
	}
	return rc;
}

char reverseComplementBaseExt(const char base) {
	char rc;
	switch(base) {
		case 'A':
			rc = 'T'; break;
		case 'C':
			rc = 'G'; break;
		case 'G':
			rc = 'C'; break;
		case 'T':
			rc = 'A'; break;
		case 'F':
			rc = 'F'; break;
		case 'X':
			rc = 'X'; break;
		default:
			fprintf(stderr, "unexpected base in revereseComplementBaseExt: %c\n", base);
			assert(0);
	}
	return rc;
}

void reverseComplementSeq(const char *seq, char *rc_seq, size_t seqLen)
{
	
	int end = seqLen-1;
	int start = 0;
	char temp;
	
	if(seqLen % 2 == 1) {
		/* has odd length, so handle middle */
		rc_seq[(start+end)/2] = reverseComplementBaseStrict( seq[(start+end)/2] );
	}
	while( start < end ) {
		
		temp = seq[end]; // in case this is inplace! (seq == rc_seq)
		rc_seq[end] = reverseComplementBaseStrict( seq[start] );
		rc_seq[start] = reverseComplementBaseStrict( temp );
		
		++start;
		--end;
	}
}

void reverseComplementKmer(const char *kmer, char *rc_kmer) {
	// only works for kmers...
	reverseComplementSeq(kmer, rc_kmer, KMER_LENGTH);
}

void reverseComplementExtensions(const char *ext, char *rc_ext)
{
	rc_ext[1] = reverseComplementBaseExt( ext[0] );
	rc_ext[0] = reverseComplementBaseExt( ext[1] );
}

void reverseComplementINPLACE(char *subcontig, int64_t size)
{
	reverseComplementSeq(subcontig, subcontig, size);
}

char *getLeastKmer(const char *kmer, char *temp) {
	/* Search for the canonical kmer */
	reverseComplementKmer(kmer, temp);
	int lex_ind = strncmp(kmer, temp, KMER_LENGTH);
	if ( lex_ind < 0)
		return (char*) kmer;
	else  {
		return temp;
	}
}

int isLeastKmer(const char *kmer) {
	char tempKmer[KMER_LENGTH];
	return getLeastKmer(kmer, tempKmer) == (char*) kmer;
}

int isLeastOrientation( const char *seq, int64_t size ) {
	int64_t start = 0, end = size-1;
	while (start < end) {
		char rc = seq[end];
		reverseComplementBase(&rc);
		
		if (seq[start] < rc)
			return 1;
		else if (seq[start] > rc)
			return 0;
		start++; end--;
	}
	return 1;
}

#endif // KMER_HANDLING_H

