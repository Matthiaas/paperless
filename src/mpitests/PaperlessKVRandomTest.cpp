#include <mpi_catch.hpp>

#include <mpi.h>
#include <zconf.h>
#include "../PaperlessKV.h"





TEST_CASE("ManyPutsAndGets", "[4rank]")
{
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest" + std::to_string(rank);

  PaperlessKV paper(id, MPI_COMM_WORLD, 7, PaperlessKV::RELAXED);

  int size_per_rank = 100;

  int from = rank * size_per_rank;
  int to = (rank+1) * size_per_rank;

  for(int i = from; i < to; i++)  {
    std::string s = std::to_string(i);
    char* value = s.data();
    size_t len = s.length();
    paper.put(value, len, value, len);
  }
  paper.Fence();

  for(int i = from; i < to; i++)  {
    std::string s = std::to_string(i);
    char* value = s.data();
    size_t len = s.length();
    QueryResult qr = paper.get(value, len);
    CHECK(qr.hasValue());
    if(qr.hasValue()) {
      CHECK(qr->Length() == len);
      CHECK(std::memcmp(qr->Value(), value, len) == 0);
    }

  }
  paper.Fence();

  int ranks;
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  for(int i = 0; i < (ranks)*size_per_rank; i++)  {
    std::string s = std::to_string(i);
    char* value = s.data();
    size_t len = s.length();
    QueryResult qr = paper.get(value, len);
    CHECK(qr.hasValue());
    if(qr.hasValue()) {
      CHECK(qr->Length() == len);
      CHECK(std::memcmp(qr->Value(), value, len) == 0);
    }
  }
  paper.Fence();
}


TEST_CASE("ManyPutsAndGets READONLY Mode", "[4rank]")
{
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest" + std::to_string(rank);

  PaperlessKV paper(id, MPI_COMM_WORLD, 7, PaperlessKV::RELAXED);

  int size_per_rank = 100;

  int from = rank * size_per_rank;
  int to = (rank+1) * size_per_rank;

  for(int i = from; i < to; i++)  {
    std::string s = std::to_string(i);
    char* value = s.data();
    size_t len = s.length();
    paper.put(value, len, value, len);
  }
  paper.Fence();

  for(int i = from; i < to; i++)  {
    std::string s = std::to_string(i);
    char* value = s.data();
    size_t len = s.length();
    QueryResult qr = paper.get(value, len);
    CHECK(qr.hasValue());
    if(qr.hasValue()) {
      CHECK(qr->Length() == len);
      CHECK(std::memcmp(qr->Value(), value, len) == 0);
    }

  }
  paper.FenceAndChangeOptions(PaperlessKV::SEQUENTIAL, PaperlessKV::READONLY);

  int ranks;
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  for(int i = 0; i < (ranks)*size_per_rank; i++)  {
    std::string s = std::to_string(i);
    char* value = s.data();
    size_t len = s.length();
    QueryResult qr = paper.get(value, len);
    CHECK(qr.hasValue());
    if(qr.hasValue()) {
      CHECK(qr->Length() == len);
      CHECK(std::memcmp(qr->Value(), value, len) == 0);
    }
  }
  paper.Fence();
}