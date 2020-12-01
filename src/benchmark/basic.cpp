// https://code.ornl.gov/eck/papyrus/-/blob/master/kv/apps/sc17/basic.cpp

#include <mpi.h>
#include "../PaperlessKV.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "timer.h"

//#define VERBOSE
//#define VERIFICATION

#define KILO    (1024UL)
#define MEGA    (1024 * KILO)
#define GIGA    (1024 * MEGA)

int rank, size;
int left, right;
char name[256];
int ret;
int db;
char* key_set;
size_t keylen;
size_t vallen;
size_t count;
int level;
int seed;

void rand_str(size_t len, char* str) {
  static char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  int l = (int) (sizeof(charset) -1);
  for (int i = 0; i < (int) len - 1; i++) {
    int key = rand() % l;
    str[i] = charset[key];
  }
  str[len - 1] = 0;
}

char* get_key(int idx) {
  return key_set + (idx * (keylen + 1));
}

void generate_key_set() {
  key_set = new char[(keylen + 1) * count];
  for (size_t i = 0; i < count; i++) {
    rand_str(keylen, get_key(i));
  }
}

int main(int argc, char** argv) {
  //MPI_Init(&argc, &argv);
  //papyruskv_init(&argc, &argv, "$PAPYRUSKV_REPOSITORY");
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  if(MPI_THREAD_MULTIPLE != provided) return 1;

  if (argc < 5) {
    printf("[%s:%d] usage: %s keylen vallen count [1:MEMTABLE/2:SSTABLE]\n", __FILE__, __LINE__, argv[0]);
    // papyruskv_finalize();
    MPI_Finalize();
    return 0;
  }

  keylen = atol(argv[1]);
  vallen = atol(argv[2]);
  count = atol(argv[3]);
  level = atoi(argv[4]);

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Get_processor_name(name, &ret);

  left = rank == 0 ? size - 1 : rank - 1;

  if (rank == 0) {
    double cb = (keylen + vallen) * count;
    double total = (keylen + vallen) * count * size;
    printf("[%s:%d] keylen[%lu] vallen[%lu] count[%lu] level[%d] size[%0.lf]KB [%lf]MB [%lf]GB nranks[%d] total count[%lu] size[%0.lf]KB [%lf]MB [%lf]GB\n", __FILE__, __LINE__, keylen, vallen, count, level, cb / KILO, cb / MEGA, cb / GIGA, size, count * size, total / KILO, total / MEGA, total / GIGA);
  }

  //papyruskv_option_t opt;
  //opt.keylen = keylen;
  //opt.vallen = vallen;
  //opt.hash = NULL;

  auto options =
      PaperlessKV::Options()
          .Consistency(PaperlessKV::RELAXED)
          .Mode(PaperlessKV::READANDWRITE);

  PaperlessKV paper("/tmp/mydb", MPI_COMM_WORLD, 1, options);

  //ret = papyruskv_open("mydb", PAPYRUSKV_CREATE | PAPYRUSKV_RELAXED | PAPYRUSKV_RDWR, &opt, &db);
  //if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);

  char* key;
  char* input_val = new char[vallen];
  char* v = new char[vallen];

  size_t err = 0;

  seed = rank * 17;
  srand(seed);
  generate_key_set();

  MPI_Barrier(MPI_COMM_WORLD);

  _w(0);
  for (size_t i = 0; i < count; i++) {
    key = get_key(i);
#ifdef VERIFICATION
    snprintf(input_val, vallen, "%s-%lu-%lu-%d.", key, keylen, vallen, rank);
#endif
    paper.put(key, keylen, input_val, vallen);
    //if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);
#ifdef VERBOSE
    if (i % 10 == 0) printf("[%s:%d] rank[%d] put[%lu] key[%s]\n", __FILE__, __LINE__, rank, i, key);
#endif
  }
  _w(1);

  // TODO: bungeroth map level to modes! And checkpointing

  paper.Fence();
  //ret = papyruskv_barrier(db, level);
  //if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);

  _w(2);

  paper.FenceAndChangeOptions(PaperlessKV::RELAXED, PaperlessKV::READONLY);
  //if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);

  /*
  ret = papyruskv_hash(db, NULL);
  if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);
  */

  // changing options includes this call:
  //MPI_Barrier(MPI_COMM_WORLD);

  _w(3);
  for (size_t i = 0; i < count; i++) {
    key = get_key(i);
    paper.get(key, keylen, v, vallen);
    //if (ret != PAPYRUSKV_OK) {
    //  printf("ERROR [%s:%d] [%d/%d] key[%s]\n", __FILE__, __LINE__, rank, size, key);
    //  err++;
    //}
#ifdef VERIFICATION
    snprintf(vv, vallen, "%s-%lu-%lu", key, keylen, vallen);
        if (strncmp(v, vv, vallen < keylen+4 ? vallen : keylen+4) != 0) {
            printf("ERROR [%s:%d] [%d/%d] key[%s] val[%s] vv[%s]\n", __FILE__, __LINE__, rank, size, key, v, vv);
            err++;
        }
#endif
#ifdef VERBOSE
    if (i % 10 == 0) printf("[%s:%d] rank[%d] get[%lu]\n", __FILE__, __LINE__, rank, i);
#endif
  }
  _w(4);

  //ret = papyruskv_protect(db, PAPYRUSKV_RDWR);
  paper.FenceAndChangeOptions(PaperlessKV::RELAXED, PaperlessKV::READANDWRITE);
  //if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);

  // changing options includes this call:
  //MPI_Barrier(MPI_COMM_WORLD);

  _w(5);
  for (size_t i = 0; i < count; i++) {
    key = get_key(i);
    paper.deleteKey(key, keylen);
    //if (ret != PAPYRUSKV_OK) {
    //  printf("ERROR [%s:%d] [%d/%d] key[%s]\n", __FILE__, __LINE__, rank, size, key);
    //  err++;
    //}
#ifdef VERBOSE
    if (i % 10 == 0) printf("[%s:%d] rank[%d] get[%lu]\n", __FILE__, __LINE__, rank, i);
#endif
  }
  _w(6);

  delete[] v;
  delete[] input_val;

  paper.Shutdown();
  //if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);

  double time_put = _ww(0, 1);
  double time_bar = _ww(0, 2);
  double time_get = _ww(3, 4);
  double time_del = _ww(5, 6);

  double time[4] = { time_put, time_bar, time_get, time_del };

  double time_min[4];
  double time_max[4];
  double time_sum[4];
  double time_avg[4];

  double krps[4];
  double mbps[4];

  if (err > 0) printf("[%s:%d] rank[%d] err[%lu] put[%lf] get[%lf]\n", __FILE__, __LINE__, rank, err, time_put, time_get);

  MPI_Reduce(time, time_min, 4, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(time, time_max, 4, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  MPI_Reduce(time, time_sum, 4, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

  if (rank == 0) {
    time_avg[0] = time_sum[0] / size;
    time_avg[1] = time_sum[1] / size;
    time_avg[2] = time_sum[2] / size;
    time_avg[3] = time_sum[3] / size;

    krps[0] = (size * count) / time_avg[0] / 1000;
    krps[1] = (size * count) / time_avg[1] / 1000;
    krps[2] = (size * count) / time_avg[2] / 1000;
    krps[3] = (size * count) / time_avg[3] / 1000;

    mbps[0] = (double) ((keylen + vallen) * size * count) / time_avg[0] / MEGA;
    mbps[1] = (double) ((keylen + vallen) * size * count) / time_avg[1] / MEGA;
    mbps[2] = (double) ((keylen + vallen) * size * count) / time_avg[2] / MEGA;

    printf("[%s:%d] "
           "KRPS[PUT,BAR,GET,DEL] [%lf,%lf,%lf,%lf] "
           "MBPS[PUT,BAR,GET] [%lf,%lf,%lf] "
           "AVG[PUT,BAR,GET,DEL] [%lf,%lf,%lf,%lf] "
           "[MIN,MAX,AVG] PUT[%lf,%lf,%lf] BAR[%lf,%lf,%lf] GET[%lf,%lf,%lf] DEL[%lf,%lf,%lf]\n",
           __FILE__, __LINE__,
           krps[0], krps[1], krps[2], krps[3], mbps[0], mbps[1], mbps[2],
           time_avg[0], time_avg[1], time_avg[2], time_avg[3],
           time_min[0], time_max[0], time_sum[0] / size,
           time_min[1], time_max[1], time_sum[1] / size,
           time_min[2], time_max[2], time_sum[2] / size,
           time_min[3], time_max[3], time_sum[3] / size);
  }


  MPI_Finalize();

  return 0;
}
