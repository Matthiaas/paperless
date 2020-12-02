#include <mpi_catch.hpp>

#include <mpi.h>
#include "../PaperlessKV.h"

inline int user_buff_len = 200;
inline char user_buff[200];

inline PaperlessKV::Options relaxed_options =
    PaperlessKV::Options()
        .Consistency(PaperlessKV::RELAXED)
        .Mode(PaperlessKV::READANDWRITE)
        .DispatchInChunks(true);


TEST_CASE("ManyPutsAndGets user provided buffer", "[4rank]")
{
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest";

  PaperlessKV paper(id, MPI_COMM_WORLD, 7, relaxed_options);

  int size_per_rank = 1000;

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
    auto qr = paper.get(value, len, user_buff, user_buff_len);
    CHECK(qr.first == QueryStatus::FOUND);
    CHECK(qr.second== len);
    CHECK(std::memcmp(user_buff, value, len) == 0);


  }
  paper.Fence();

  int ranks;
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  for(int i = 0; i < (ranks)*size_per_rank; i++)  {
    std::string s = std::to_string(i);
    char* value = s.data();
    size_t len = s.length();
    auto qr = paper.get(value, len, user_buff, user_buff_len);
    CHECK(qr.first == QueryStatus::FOUND);
    CHECK(qr.second== len);
    CHECK(std::memcmp(user_buff, value, len) == 0);
  }
  paper.Fence();
}


TEST_CASE("ManyPutsAndGets", "[4rank]")
{
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest";

  PaperlessKV paper(id, MPI_COMM_WORLD, 7, relaxed_options);

  int size_per_rank = 1000;

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
  std::string id = "/tmp/PaperlessTest";

  PaperlessKV paper(id, MPI_COMM_WORLD, 7, relaxed_options);

  int size_per_rank = 10000;

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