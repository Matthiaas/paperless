#include <upc.h>
#include <stdio.h>
#include <stdlib.h>


int main() {
	printf("Hello from %d of %d\n", MYTHREAD, THREADS);
	return 0;
}
