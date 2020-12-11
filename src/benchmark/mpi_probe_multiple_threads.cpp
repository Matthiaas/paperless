

#include <mpi.h>

#include <atomic>

#include "thread"

#define WAIT_ONLY_TAG 1
#define QUESTION_TAG 2
#define ANSWER_TAG 3

#define QUESTION_LEN 16
#define ANSWER_LEN 32


volatile int work = 0;

bool use_useless_waiter;


void doWork(long count) {
  for(int i = 0; i < count; i++) {
    work++;
  }
}

void WaitOnlyThread() {
  if(use_useless_waiter) {
    int size, rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    MPI_Probe(MPI_ANY_SOURCE, WAIT_ONLY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(nullptr, 0, MPI_CHAR, MPI_ANY_SOURCE, WAIT_ONLY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  }

}

void AnswerTread() {
  int size, rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);


  while(true) {
    MPI_Status status;
    MPI_Probe(MPI_ANY_SOURCE, QUESTION_TAG, MPI_COMM_WORLD, &status);

    char* q = static_cast<char*>(std::malloc(QUESTION_LEN * sizeof(char)));
    MPI_Recv(q, QUESTION_LEN, MPI_CHAR, MPI_ANY_SOURCE, QUESTION_TAG, MPI_COMM_WORLD, &status);
    if(q[0] == 0) {
      free(q);
      return;
    }

    doWork(200);

    char* a = static_cast<char*>(std::malloc(ANSWER_LEN));
    MPI_Send(a, ANSWER_LEN, MPI_CHAR, status.MPI_SOURCE, ANSWER_TAG, MPI_COMM_WORLD);
    free(q);
    free(a);
  }

}

void askQuestions(int count) {
  int size, rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  for(int i = 0; i < count; i++) {
    if(i % size == rank) {
      doWork( 200);
    } else {
      char* q = static_cast<char*>(std::malloc(QUESTION_LEN));
      char* a = static_cast<char*>(std::malloc(ANSWER_LEN));
      q[0] = 1;
      MPI_Send(q,QUESTION_LEN, MPI_CHAR,i % size , QUESTION_TAG, MPI_COMM_WORLD);

      MPI_Status status;
      MPI_Probe(i % size, ANSWER_TAG, MPI_COMM_WORLD, &status);
      MPI_Recv(a, ANSWER_LEN, MPI_CHAR, i % size, ANSWER_TAG, MPI_COMM_WORLD, &status);
      doWork(200);
      free(a);
      free(q);
    }


  }

  MPI_Barrier(MPI_COMM_WORLD);
  char* q = static_cast<char*>(std::malloc(QUESTION_LEN));
  q[0] = 0;
  MPI_Send(q,QUESTION_LEN, MPI_CHAR,rank , QUESTION_TAG, MPI_COMM_WORLD);
  if(use_useless_waiter)
    MPI_Send(nullptr,0, MPI_CHAR,rank , WAIT_ONLY_TAG, MPI_COMM_WORLD);
}

int main(int argc, char** argv) {

  use_useless_waiter = false;

  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

  auto start = std::chrono::high_resolution_clock::now();


  std::thread t1(&WaitOnlyThread);
  std::thread t2(&AnswerTread);
  std::cout << "Threads started" << std::endl;

  MPI_Barrier(MPI_COMM_WORLD);

  askQuestions(100);

  std::cout << "Work completed" << std::endl;

  auto end = std::chrono::high_resolution_clock::now();
  std::cout << std::chrono::duration_cast<std::chrono::nanoseconds>((end-start)).count() << std::endl;


  t1.join();
  t2.join();

  MPI_Finalize();

}

/*
 *
 * int provided;

 */




