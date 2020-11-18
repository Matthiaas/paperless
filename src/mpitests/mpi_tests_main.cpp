#define CATCH_CONFIG_RUNNER
#include <mpi_catch.hpp>
#include <mpi.h>

int main(int argc, char *argv[])
{
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  if(MPI_THREAD_MULTIPLE != provided) return 1;
  int result = Catch::Session().run(argc, argv);
  MPI_Finalize();
  return result;
}

