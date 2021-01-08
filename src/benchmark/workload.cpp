// https://code.ornl.gov/eck/papyrus/-/blob/master/kv/apps/sc17/workload.cpp

#include <mpi.h>

#include <filesystem>

#include "../PaperlessKV.h"
#include "OptionReader.h"
#include "timer.h"

//#define VERBOSE

#define KILO (1024UL)
#define MEGA (1024 * KILO)
#define GIGA (1024 * MEGA)

int rank, size;
int left, right;
char name[256];
int ret;
int db;
char* key_set;
size_t keylen;
size_t vallen;
size_t count;
int update_ratio;
int seed;
size_t key_index_off;
size_t key_index;

void rand_str(size_t len, char* str) {
  static char charset[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  int l = (int)(sizeof(charset) - 1);
  for (size_t i = 0; i < len - 1; i++) {
    int key = rand() % l;
    str[i] = charset[key];
  }
  str[len - 1] = 0;
}

char* get_key(int idx) {
  return key_set + (idx * PAPERLESS::padLength(keylen + 1));
}

void generate_key_set() {
  key_set = (char*) PAPERLESS::malloc(PAPERLESS::padLength(keylen + 1) * count);
  for (size_t i = 0; i < count; i++) {
    rand_str(keylen, get_key(i));
  }
}

int main(int argc, char** argv) {
  // MPI_Init(&argc, &argv);
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  if (MPI_THREAD_MULTIPLE != provided) return 1;
  // papyruskv_init(&argc, &argv, "$PAPYRUSKV_REPOSITORY");

  if (argc < 5) {
    printf("[%s:%d] usage: %s keylen vallen count update_ratio[0:100]\n",
           __FILE__, __LINE__, argv[0]);
    // papyruskv_finalize();
    MPI_Finalize();
    return 0;
  }

  keylen = atol(argv[1]);
  vallen = atol(argv[2]);
  count = atol(argv[3]);
  update_ratio = atoi(argv[4]);

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Get_processor_name(name, &ret);

  left = rank == 0 ? size - 1 : rank - 1;

  key_index_off = count * rank;
  key_index = 0;

  if (rank == 0) {
    double cb = (keylen + vallen) * count;
    double total = (keylen + vallen) * count * size;
    printf(
        "[%s:%d] update_ratio[%d%%] keylen[%lu] vallen[%lu] count[%lu] "
        "size[%0.lf]KB [%lf]MB [%lf]GB nranks[%d] total count[%lu] "
        "size[%0.lf]KB [%lf]MB [%lf]GB\n",
        __FILE__, __LINE__, update_ratio, keylen, vallen, count, cb / KILO,
        cb / MEGA, cb / GIGA, size, count * size, total / KILO, total / MEGA,
        total / GIGA);
  }

  // papyruskv_option_t opt;
  // opt.keylen = keylen;
  // opt.vallen = vallen;
  // opt.hash = NULL;

  auto options = ReadOptionsFromEnvVariables()
                     .Consistency(PaperlessKV::RELAXED)
                     .Mode(PaperlessKV::READANDWRITE);
  // TODO: FixId
  std::string db_path = "/scratch/mydb";

  PaperlessKV paper(db_path, MPI_COMM_WORLD, 1, options);
  // ret = papyruskv_open("mydb", PAPYRUSKV_CREATE | PAPYRUSKV_RELAXED |
  // PAPYRUSKV_RDWR, &opt, &db); if (ret != PAPYRUSKV_OK) printf("[%s:%d]
  // ret[%d]\n", __FILE__, __LINE__, ret);

  char* key;
  char* put_val = new char[vallen];
  char* get_val = new char[vallen];
  size_t get_vallen = 0UL;
  size_t err = 0UL;
  size_t cnt_put = 0;
  size_t cnt_get = 0;

  snprintf(put_val, vallen, "THIS IS (%lu)B VAL FROM %d", vallen, rank);

  seed = rank * 17;
  srand(seed);
  generate_key_set();

  MPI_Barrier(MPI_COMM_WORLD);

  _w(0);
  for (size_t i = 0; i < count; i++) {
    key = get_key(i);
    paper.put(key, keylen, put_val, vallen);
    // if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__,
    // ret);
    //#ifdef VERBOSE
    // if (i % 100 == 0) printf("[%s:%d] rank[%d] put[%lu]\n", __FILE__,
    // __LINE__, rank, i);
    //#endif
  }
  // ret = papyruskv_barrier(db, PAPYRUSKV_MEMTABLE);
  // if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__,
  // ret);
  _w(1);

  if (update_ratio == 0) {
    // ret = papyruskv_protect(db, PAPYRUSKV_RDONLY);
    paper.FenceAndChangeOptions(PaperlessKV::RELAXED, PaperlessKV::READONLY);
    // if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__,
    // ret);
  } else {
    paper.Fence();
  }

  _w(2);
  for (size_t i = 0; i < count; i++) {
    key = get_key(i);
    if (i % 100 < (size_t)update_ratio) {
      paper.put(key, keylen, put_val, vallen);
      // if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__,
      // __LINE__, ret);
      cnt_put++;
    } else {
      paper.get(key, keylen, get_val, get_vallen);
      // if (ret != PAPYRUSKV_OK) {
      //  printf("ERROR [%s:%d] [%d/%d] key[%s]\n", __FILE__, __LINE__, rank,
      //  size, key); err++;
      //}
      cnt_get++;
    }
    //#ifdef VERBOSE
    //    if (i % 100 == 0) printf("[%s:%d] rank[%d] pgt[%lu]\n", __FILE__,
    //    __LINE__, rank, i);
    //#endif
  }
  _w(3);

  delete[] put_val;
  delete[] get_val;

  paper.Shutdown();
  // ret = papyruskv_close(db);
  // if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__,
  // ret);

  double time_put = _ww(0, 1);
  double time_pgt = _ww(2, 3);

  double time[2] = {time_put, time_pgt};

  double time_min[2];
  double time_max[2];
  double time_sum[2];

  if (err > 0)
    printf("[%s:%d] rank[%d] err[%lu]\n", __FILE__, __LINE__, rank, err);

  MPI_Reduce(time, time_min, 2, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(time, time_max, 2, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  MPI_Reduce(time, time_sum, 2, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

  if (rank == 0) {
    printf(
        "[%s:%d] ALL[%lu] PUT[%lu] GET[%lu] AVG[PUT,PGT] [%lf,%lf] "
        "[MIN,MAX,AVG] PUT[%lf,%lf,%lf], PGT[%lf,%lf,%lf]\n",
        __FILE__, __LINE__, cnt_put + cnt_get, cnt_put, cnt_get,
        time_sum[0] / size, time_sum[1] / size, time_min[0], time_max[0],
        time_sum[0] / size, time_min[1], time_max[1], time_sum[1] / size);
  }

  MPI_Finalize();

  return 0;
}
