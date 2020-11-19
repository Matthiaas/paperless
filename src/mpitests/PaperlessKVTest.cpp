#include <mpi_catch.hpp>

#include <mpi.h>
#include "../PaperlessKV.h"

std::function<uint64_t(const char*,const size_t)> hash_fun =
    [] (const char* v, const size_t) -> uint64_t {
  return v[0] - '0';
};


const char key1[] = "1key";
const size_t klen1 = 4;
const char key2[] = "2key";
const size_t klen2 = 4;

const char value1[] = "1value";
const size_t vlen1 = 6;
const char value2[] = "2value";
const size_t vlen2 = 6;



TEST_CASE("LocalGetOnEmptyKV", "[1rank]")
{
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest" + std::to_string(rank);
  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, PaperlessKV::RELAXED);
  QueryResult qr = paper.get(key1, klen1);
  CHECK(qr == QueryStatus::NOT_FOUND);
}

TEST_CASE("LocalPut", "[1rank]")
{
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest" + std::to_string(rank);
  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, PaperlessKV::RELAXED);
  {
    paper.put(key1, klen1, value1, vlen1);
    QueryResult qr = paper.get(key1, klen1);
    CHECK(qr->Length() == vlen1);
    CHECK(std::memcmp(qr->Value(), value1, klen1) == 0);
  }
  {
    paper.put(key2, klen2, value2, vlen2);
    QueryResult qr = paper.get(key2, klen2);
    CHECK(qr->Length() == vlen2);
    CHECK(std::memcmp(qr->Value(), value2, klen2) == 0);
  }
  {
    QueryResult qr = paper.get(key1, klen1);
    CHECK(qr->Length() == vlen1);
    CHECK(std::memcmp(qr->Value(), value1, klen1) == 0);
  }
}



TEST_CASE("LocalOverride", "[1rank]")
{
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest" + std::to_string(rank);
  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, PaperlessKV::RELAXED);
  {
    paper.put(key1, klen1, value1, vlen1);
    QueryResult qr = paper.get(key1, klen1);
    CHECK(qr.hasValue());
    if(qr.hasValue()) {
      CHECK(qr->Length() == vlen1);
      CHECK(std::memcmp(qr->Value(), value1, klen1) == 0);
    }
  }
  {
    paper.put(key1, klen1, value2, vlen2);
    QueryResult qr = paper.get(key1, klen1);
    CHECK(qr.hasValue());
    if(qr.hasValue()) {
      CHECK(qr->Length() == vlen2);
      CHECK(std::memcmp(qr->Value(), value2, klen2) == 0);
    }
  }
}


// TODO: Test with 2 ranks
TEST_CASE("RemoteGet", "[2rank]")
{

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest" + std::to_string(rank);

  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, PaperlessKV::RELAXED);
  if(rank == 1) {
    paper.put(key1, klen1, value1, vlen1);
  }
  paper.Fence();

  if(rank == 1) {
    QueryResult qr = paper.get(key1, klen1);
    CHECK(qr.hasValue());
    if(qr.hasValue()) {
      CHECK(qr->Length() == vlen1);
      CHECK(std::memcmp(qr->Value(), value1, klen1) == 0);
    }
  } else {
    QueryResult qr = paper.get(key1, klen1);
    CHECK(qr.hasValue());
    if(qr.hasValue()) {
      CHECK(qr->Length() == vlen1);
      CHECK(std::memcmp(qr->Value(), value1, klen1) == 0);
    }
  }

  paper.Fence();

}


TEST_CASE("RemotePutAndGet SEQUENTIAL", "[2rank]")
{


  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest" + std::to_string(rank);
  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, PaperlessKV::SEQUENTIAL);

  if(rank == 0) {
    paper.put(key1, klen1, value1, vlen1);
  }
  paper.Fence();

  QueryResult qr = paper.get(key1, klen1);
  CHECK(qr.hasValue());
  if(qr.hasValue()) {
    CHECK(qr->Length() == vlen1);
    CHECK(std::memcmp(qr->Value(), value1, klen1) == 0);
  }
  paper.Fence();
}


TEST_CASE("RemotePutAndGet Relaxed", "[2rank]")
{


  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest" + std::to_string(rank);

  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, PaperlessKV::RELAXED);
  if(rank == 0) {

    paper.put(key1, klen1, value1, vlen1);
  }


  if(rank == 0) {
    QueryResult qr = paper.get(key1, klen1);
    CHECK(qr.hasValue());
    if(qr.hasValue()) {
      CHECK(qr->Length() == vlen1);
      CHECK(std::memcmp(qr->Value(), value1, klen1) == 0);
    }
  } else {
    QueryResult qr = paper.get(key1, klen1);
    CHECK(qr == QueryStatus::NOT_FOUND);
  }

  paper.Fence();
}

TEST_CASE("RemotePutAndGet Relaxed Fence", "[2rank]")
{


  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest" + std::to_string(rank);

  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, PaperlessKV::RELAXED);
  if(rank == 0) {

    paper.put(key1, klen1, value1, vlen1);
  }

  paper.Fence();

  if(rank == 0) {
    QueryResult qr = paper.get(key1, klen1);
    CHECK(qr.hasValue());
    if(qr.hasValue()) {
      CHECK(qr->Length() == vlen1);
      CHECK(std::memcmp(qr->Value(), value1, klen1) == 0);
    }
  } else {
    QueryResult qr = paper.get(key1, klen1);
    CHECK(qr.hasValue());
    if(qr.hasValue()) {
      CHECK(qr->Length() == vlen1);
      CHECK(std::memcmp(qr->Value(), value1, klen1) == 0);
    }
  }

  paper.Fence();
}



