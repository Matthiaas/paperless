#ifndef _READUFX_H_
#define _READUFX_H_

#ifdef __cplusplus
extern "C" {
#endif
FILE * OpenDebugFile(char * prefix, FILE * pFile, int myrank);
FILE * UFXInitOpen(char * filename, int * dsize, int64_t * myshare, int nprocs, int myrank, int64_t *nEntries);
int64_t UFXRead(FILE * f, int dsize, char *** kmersarr, int ** counts, char ** lefts, char ** rights, int64_t requestedkmers, int dmin, int reuse, int myrank);
void DeAllocateAll(char *** kmersarr, int ** counts, char ** lefts, char ** rights, int64_t initialread);

#ifdef __cplusplus
}
#endif
#endif
