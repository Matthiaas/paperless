#define CATCH_CONFIG_RUNNER
#include <mpi_catch.hpp>
#include <mpi.h>

#include <stdio.h>
#include <execinfo.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

//https://stackoverflow.com/questions/77005/how-to-automatically-generate-a-stacktrace-when-my-program-crashes
void handler(int sig) {
  void *array[10];
  size_t size;

  // get void*'s for all entries on the stack
  size = backtrace(array, 10);

  // print out all the frames to stderr
  fprintf(stderr, "Error: signal %d:\n", sig);
  backtrace_symbols_fd(array, size, STDERR_FILENO);
  exit(1);
}

int main(int argc, char *argv[])
{
#ifdef VECTORIZE
  std::cerr << "Please recompile the MPI tests without vecorization.\n"
      << "They will be rewritten to work with it, but for now they will segfault b.c. of misaligned loads.\n"
      << "Sorry for the inconvenience.\n";
  return 0;
#else
  signal(SIGSEGV, handler);
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  if(MPI_THREAD_MULTIPLE != provided) return 1;
  int result = Catch::Session().run(argc, argv);
  MPI_Finalize();
  return result;
#endif
}

