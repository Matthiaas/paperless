//
// Created by matthias on 07.12.20.
//

#include "KVWrapper.h"
#include <filesystem>

#define KILO    (1024UL)
#define MEGA    (1024 * KILO)
#define GIGA    (1024 * MEGA)


std::vector<std::pair<long,int>> put_time;
std::vector<std::pair<long,int>> get_time;
std::vector<std::pair<long,int>> update_time;


void BenchmarkRandomData() {
  char* key;
  char* put_val = new char[vallen];
  char* get_val = new char[vallen];
  size_t get_vallen = 0UL;
  size_t cnt_put = 0;
  size_t cnt_get = 0;

  int seed = rank * 17;
  srand(seed);
  generate_key_set();


  put_time.reserve(count);
  get_time.reserve(count * (100-update_ratio)/ 100 + 100);
  update_time.reserve(count * update_ratio/ 100 + 100);

  MPI_Barrier(MPI_COMM_WORLD);

  for (size_t i = 0; i < count; i++) {
    key = get_key(i);
    put_time.emplace_back(TimedKV::Put(key, keylen, put_val, vallen));
  }


  KV::SetMode(PaperlessKV::RELAXED, PaperlessKV::READONLY);
  if (update_ratio == 0) {

  } else {
    //KV::Fence();
  }
  std::cout << "afterKill" << std::endl;
  for (size_t i = 0; i < count; i++) {
    key = get_key(i);
    if (i % 100 < (size_t)update_ratio) {
      update_time.emplace_back(TimedKV::Put(key, keylen, put_val, vallen));
      cnt_put++;
    } else {
      get_time.emplace_back(TimedKV::Get(key, keylen, get_val, get_vallen));
      cnt_get++;
    }
  }

  delete[] put_val;
  delete[] get_val;
}


int main(int argc, char** argv)  {

  if (argc < 6) {
    printf("[%s:%d] usage: %s keylen vallen count update_ratio[0:100] storage_path\n", __FILE__, __LINE__, argv[0]);
    return 0;
  }

  KV::Init(argc, argv, "mydb");

  keylen = atol(argv[1]);
  vallen = atol(argv[2]);
  count = atol(argv[3]);
  update_ratio = atoi(argv[4]);
  std::string storage_directory = argv[5];

  int size;

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  if (rank == 0) {
    double cb = (keylen + vallen) * count;
    double total = (keylen + vallen) * count * size;
    printf("[%s:%d] update_ratio[%d%%] keylen[%lu] vallen[%lu] count[%lu] size[%0.lf]KB [%lf]MB [%lf]GB nranks[%d] total count[%lu] size[%0.lf]KB [%lf]MB [%lf]GB\n", __FILE__, __LINE__, update_ratio, keylen, vallen, count, cb / KILO, cb / MEGA, cb / GIGA, size, count * size, total / KILO, total / MEGA, total / GIGA);
  }

  // Benchmark code here:

  BenchmarkRandomData();


  // Write data to file:
  std::filesystem::create_directories(storage_directory);

  if (rank == 0) {
    std::ofstream put_file(storage_directory + "/runconfig.txt");
    put_file << "rank_count: " << size << std::endl;
    put_file << "keylen: " << keylen << std::endl;
    put_file << "vallen: " << vallen << std::endl;
    put_file << "count: " << count << std::endl;
    put_file << "update_ratio: " << update_ratio << std::endl;
    put_file.close();
  }


  std::ofstream put_file(storage_directory + "/put" + std::to_string(rank) + ".txt");
  for(const auto& [t, r] : put_time) {
    put_file << t << " " << r << std::endl;
  }
  put_file.close();

  std::ofstream get_file(storage_directory + "/get" + std::to_string(rank) + ".txt");
  for(const auto& [t, r] : get_time) {
    get_file << t << " " << r << std::endl;
  }
  get_file.close();

  std::ofstream update_file(storage_directory + "/update" + std::to_string(rank) + ".txt");
  for(const auto& [t, r] : update_time) {
    update_file << t << " " << r << std::endl;
  }
  update_file.close();

  MPI_Barrier(MPI_COMM_WORLD);

  KV::Finalize();


}
