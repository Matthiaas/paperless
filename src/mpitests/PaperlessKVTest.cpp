#include <mpi_catch.hpp>

#include <mpi.h>
#include <zconf.h>
#include "../PaperlessKV.h"

#include "../benchmark/OptionReader.h"
#include "PaperLessKVFriend.h"

std::function<uint64_t(const char*,const size_t)> hash_fun =
    [] (const char* v, const size_t) -> uint64_t {
  return v[0] - '0';
};


const char key1[] = "1key";
const size_t klen1 = 4;
const char key2[] = "2key";
const size_t klen2 = 4;
const char key3[] = "3key";
const size_t klen3 = 4;

const char value1[] = "1value";
const size_t vlen1 = 6;
const char value2[] = "2value";
const size_t vlen2 = 6;
const char value[] = "3value";
const size_t vlen3 = 6;


inline int user_buff_len = 200;
inline char user_buff[200];

inline PaperlessKV::Options relaxed_options =
    ReadOptionsFromEnvVariables()
    .Consistency(PaperlessKV::RELAXED)
    .Mode(PaperlessKV::READANDWRITE);

inline PaperlessKV::Options sequential =
    ReadOptionsFromEnvVariables()
        .Consistency(PaperlessKV::SEQUENTIAL)
        .Mode(PaperlessKV::READANDWRITE);



TEST_CASE("LocalGetOnEmptyKV", "[1rank]")
{
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest";
  std::cout << "asdfasdf" << relaxed_options.StorageLocation("hallo").strorage_location << std::endl;
  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, relaxed_options);
  QueryResult qr = paper.get(key1, klen1);
  CHECK(qr == QueryStatus::NOT_FOUND);
}


TEST_CASE("Local Put Checkpoint", "[1rank]")
{
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest";
  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, relaxed_options);
  PaperLessKVFriend paperFriend(&paper);

  paper.put(key1, klen1, value1, vlen1);
  paper.put(key2, klen2, value2, vlen2);

  paper.FenceAndCheckPoint();

  StorageManager& sm = paperFriend.getStorageManger();
  {
    QueryResult qr = sm.readFromDisk(ElementView(key1, klen1));
    CHECK(qr->Length() == vlen1);
    CHECK(std::memcmp(qr->Value(), value1, klen1) == 0);
  }
  {
    QueryResult qr = sm.readFromDisk(ElementView(key2, klen2));
    CHECK(qr->Length() == vlen2);
    CHECK(std::memcmp(qr->Value(), value2, klen2) == 0);
  }

}

TEST_CASE("Local Put local_cache", "[1rank]")
{
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest";
  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, relaxed_options.MaxLocalCacheSize(2000000));
  PaperLessKVFriend paperFriend(&paper);

  paper.put(key1, klen1, value1, vlen1);
  paper.put(key2, klen2, value2, vlen2);

  paper.FenceAndCheckPoint();

  LRUTreeCache& lruCache = paperFriend.getLocalCache();
  {
    std::optional<QueryResult> qr = lruCache.get(ElementView(key1, klen1), hash_fun(key1, klen1));
    CHECK(qr.has_value());
    if(qr.has_value()) {
      CHECK((*qr)->Length() == vlen1);
      CHECK(std::memcmp((*qr)->Value(), value1, klen1) == 0);
    }
  }
  {
    std::optional<QueryResult> qr = lruCache.get(ElementView(key2, klen2), hash_fun(key2, klen2));
    CHECK(qr.has_value());
    if(qr.has_value()) {
      CHECK((*qr)->Length() == vlen2);
      CHECK(std::memcmp((*qr)->Value(), value2, klen2) == 0);
    }
  }
  {
    std::optional<QueryResult> qr = lruCache.get(ElementView(key3, klen3), hash_fun(key3, klen3));
    CHECK(!qr.has_value());
  }
  paper.DeleteKey(key1, klen1);
  {
    std::optional<QueryResult> qr = lruCache.get(ElementView(key1, klen1), hash_fun(key1, klen1));
    CHECK(qr.has_value());
    CHECK((*qr).Status() == QueryStatus::DELETED);
  }
}


TEST_CASE("LocalGet into user provided buffer ", "[1rank]")
{
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest";
  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, relaxed_options);

  paper.put(key1, klen1, value1, vlen1);

  auto qr = paper.get(key1, klen1, user_buff, user_buff_len);
  CHECK(qr.first == QueryStatus::FOUND);
  CHECK(qr.second == vlen1);
  CHECK(memcmp(user_buff, value1, vlen1) == 0);

  qr = paper.get(key1, klen1, user_buff, 0);
  CHECK(qr.first == QueryStatus::BUFFER_TOO_SMALL);
  CHECK(qr.second == vlen1);
}

TEST_CASE("RemoteGet into user provided buffer ", "[2rank]")
{

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest";

  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, relaxed_options);

  if(rank == 1) {
    paper.put(key1, klen1, value1, vlen1);
  }

  MPI_Barrier(MPI_COMM_WORLD);

  auto qr = paper.get(key1, klen1, user_buff, user_buff_len);
  CHECK(qr.first == QueryStatus::FOUND);
  CHECK(qr.second == vlen1);
  CHECK(memcmp(user_buff, value1, vlen1) == 0);

  qr = paper.get(key1, klen1, user_buff, 0);
  CHECK(qr.first == QueryStatus::BUFFER_TOO_SMALL);
  CHECK(qr.second == vlen1);
  MPI_Barrier(MPI_COMM_WORLD);

}


TEST_CASE("Remote Get remote_caching in READONLY mode", "[2rank]")
{
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest";
  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, sequential);
  PaperLessKVFriend paperFriend(&paper);

  if(rank == 1) {
    paper.put(key1, klen1, value1, vlen1);
  }

  MPI_Barrier(MPI_COMM_WORLD);

  LRUTreeCache& lruCache = paperFriend.getRemoteCache();
  if(rank == 0){
    // Remote value should not get cached
    paper.get(key1, klen1);
    std::optional<QueryResult> qr = lruCache.get(ElementView(key1, klen1), hash_fun(key1, klen1));
    CHECK(!qr.has_value());
  }

  paper.FenceAndChangeOptions(PaperlessKV::SEQUENTIAL, PaperlessKV::READONLY);

  if(rank == 0) {
    // Remote value should get cached.
    paper.get(key1, klen1);
    std::optional<QueryResult> qr = lruCache.get(ElementView(key1, klen1), hash_fun(key1, klen1));
    CHECK(qr.has_value());
    if (qr.has_value()) {
      CHECK((*qr)->Length() == vlen1);
      CHECK(std::memcmp((*qr)->Value(), value1, klen1) == 0);
    }
  }

  paper.Fence();


}

TEST_CASE("LocalPut", "[1rank]")
{
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest";
  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, relaxed_options);
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
  std::string id = "/tmp/PaperlessTest";
  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, relaxed_options);
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
  std::string id = "/tmp/PaperlessTest";

  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, relaxed_options);
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
  std::string id = "/tmp/PaperlessTest";
  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, sequential);

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
  std::string id = "/tmp/PaperlessTest";

  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, relaxed_options);
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
    CHECK(!qr.hasValue());
    CHECK(qr.Status() == QueryStatus::NOT_FOUND);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  paper.Fence();

}

TEST_CASE("RemotePutAndGet Relaxed Fence", "[2rank]")
{
 int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest";

  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, relaxed_options);
  if(rank == 0) {

    paper.put(key1, klen1, value1, vlen1);
  }

  paper.Fence();


  if(rank == 0) {
    int a = 1;
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

  MPI_Barrier(MPI_COMM_WORLD);
}



TEST_CASE("TestHelpers", "[1rank]")
{
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::string id = "/tmp/PaperlessTest";
  PaperlessKV paper(id, MPI_COMM_WORLD, hash_fun, relaxed_options);
  PaperLessKVFriend paperFriend(&paper);

  char buff[10];



  for(int i = 0; i < 500000; i+= 1) {
    paperFriend.WriteIntToBuff(buff, i);
    paperFriend.WriteIntToBuff(buff + 4, i + 1);
    CHECK(paperFriend.ReadIntFromBuff(buff) == static_cast<unsigned int>(i));
    CHECK(paperFriend.ReadIntFromBuff(buff + 4) == static_cast<unsigned int>(i+1));
  }


}