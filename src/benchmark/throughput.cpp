//
// Created by matthias on 07.12.20.
//

//#define I_PAPERLESS_BENCHMARK

#include "KVWrapper.h"
#include <chrono>
#include <filesystem>
#include <iostream>
#include <mpi.h>

#define KILO    (1024UL)
#define MEGA    (1024 * KILO)
#define GIGA    (1024 * MEGA)



long put_time = 0;
long get_update_time = 0;
int batch_size = 1000;
bool check_point_data = false;


void BenchmarkRandomData() {
  char* key;
  char* put_val = (char*) PAPERLESS::malloc(vallen);
  char* get_val = (char*) PAPERLESS::malloc(vallen);
  size_t get_vallen = vallen;
  size_t cnt_put = 0;
  size_t cnt_get = 0;

  int seed = rank * 17;
  srand(seed);
  generate_key_set();


  MPI_Barrier(MPI_COMM_WORLD);

  auto puts_phase_start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < count; i++) {
    key = get_key(i);
    KV::Put(key, keylen, put_val, vallen);
  }

  if(check_point_data) {
    KV::Checkpoint();
  } else {
    if (update_ratio == 0) {
      KV::SetMode(PaperlessKV::RELAXED, PaperlessKV::READONLY);
    } else {
      KV::Fence();
    }
  }



  auto puts_phase_end = std::chrono::high_resolution_clock::now();
  put_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
      puts_phase_end-puts_phase_start).count();



  KV::SetQueryAmount(batch_size);

  auto gets_phase_start = std::chrono::high_resolution_clock::now();

  for (size_t i = 0; i < count; i++) {
    key = get_key(i);
    if (i % 100 < (size_t)update_ratio) {
      KV::Put(key, keylen, put_val, vallen);
      cnt_put++;
    } else {
      KV::Get(key, keylen, get_val, get_vallen);
      cnt_get++;
    }

    if(count % 1000 == 0)  {
      KV::WaitForGetComplete();
      KV::SetQueryAmount(batch_size);

    }
  }

  KV::WaitForGetComplete();



  auto gets_phase_end = std::chrono::high_resolution_clock::now();
  get_update_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
      gets_phase_end-gets_phase_start).count();

  delete[] put_val;
  delete[] get_val;
}


int main(int argc, char** argv)  {

  if (argc < 6) {
    printf("[%s:%d] usage: %s keylen vallen count update_ratio[0:100] storage_path consistency['SEQ', default='REL'] [batch_size_for_Iget] [CHECKPOINT]\n", __FILE__, __LINE__, argv[0]);
    return 0;
  }

  KV::Init(argc, argv, "mydb");

  keylen = atol(argv[1]);
  vallen = atol(argv[2]);
  count = atol(argv[3]);
  update_ratio = atoi(argv[4]);
  std::string storage_directory = argv[5];
  std::string consistency = (argc >= 7) ? argv[6] : "REL";

  if(argc >= 8) {
    batch_size = atol(argv[7]);
  }

  std::cout << argc << std::endl;
  if (argc >= 9) {

    check_point_data = std::string(argv[8]) == "CHECKPOINT";
    if(check_point_data) {
      std::cout << "Checkpointing data..." << std::endl;
    }
  }

  int size;

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  if (consistency == "SEQ") {
    KV::SetMode(PaperlessKV::Consistency_t::SEQUENTIAL, PaperlessKV::Mode_t::READANDWRITE);
  }

  // Benchmark code here:

  BenchmarkRandomData();

  KV::Finalize();
  // Write data to file:


  std::filesystem::create_directories(storage_directory);
  std::ofstream put_file(storage_directory + "/out" + std::to_string(rank) +".txt", std::ios_base::app);
  put_file << "rank_count:" << size;
  put_file << ",keylen:" << keylen;
  put_file << ",vallen:" << vallen;
  put_file << ",count:" << count;
  put_file << ",update_ratio:" << update_ratio;
  put_file << ",batch_size:" << batch_size;
  put_file << ",put_time:" << put_time;
  put_file << ",get_update_time:" << get_update_time << std::endl;
  put_file.close();

  MPI_Finalize();
}
