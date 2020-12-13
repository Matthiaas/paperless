

#include <mpi.h>

#include <atomic>
#include <iostream>

#include <functional>
#include "thread"

#define WAIT_ONLY_TAG 1
#define QUESTION_TAG 2
#define ANSWER_TAG 3

#define QUESTION_LEN 16
#define ANSWER_LEN 32


volatile int work = 0;

bool use_probe;
bool use_useless_waiter;


void doWork(long count) {
  // Lets eat some cpu
  for(long i = 0; i < count; i++) {
    work++;
    //if(count == 5000000001 && i % 10000000 == 0)
      //std::cout << "progress" << std::endl;
  }
 // std::cout << "Done" << std::endl;
}

void WaitOnlyThread() {
  if(use_useless_waiter) {
    int size, rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    if(use_probe) {
      MPI_Status status;
      MPI_Probe(MPI_ANY_SOURCE, WAIT_ONLY_TAG, MPI_COMM_WORLD, &status);
      int value_size;
      MPI_Get_count(&status, MPI_CHAR, &value_size);
    }
    MPI_Recv(nullptr, 0, MPI_CHAR, MPI_ANY_SOURCE, WAIT_ONLY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  }

}

void AnswerTread() {
  int size, rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);


  while(true) {
    MPI_Status status;
    if(use_probe) {
      MPI_Probe(MPI_ANY_SOURCE, QUESTION_TAG, MPI_COMM_WORLD, &status);
      int value_size;
      MPI_Get_count(&status, MPI_CHAR, &value_size);
    }

    if(use_probe) {
      MPI_Status status;
      MPI_Probe(MPI_ANY_SOURCE, QUESTION_TAG, MPI_COMM_WORLD, &status);
      int value_size;
      MPI_Get_count(&status, MPI_CHAR, &value_size);
    }
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
    if(i % size == rank && false) {
      doWork( 100);
    } else {
      int to =  i % size;
      char* q = static_cast<char*>(std::malloc(QUESTION_LEN));
      char* a = static_cast<char*>(std::malloc(ANSWER_LEN));
      q[0] = 1;
      MPI_Send(q,QUESTION_LEN, MPI_CHAR,to, QUESTION_TAG, MPI_COMM_WORLD);

      MPI_Status status;

      if(use_probe) {
        MPI_Status status;
        MPI_Probe(to, ANSWER_TAG, MPI_COMM_WORLD, &status);
        int value_size;
        MPI_Get_count(&status, MPI_CHAR, &value_size);
      }
      MPI_Recv(a, ANSWER_LEN, MPI_CHAR, to, ANSWER_TAG, MPI_COMM_WORLD, &status);
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

void Run(int count) {
  std::thread t1(&WaitOnlyThread);
  std::thread t2(&AnswerTread);
  MPI_Barrier(MPI_COMM_WORLD);
  askQuestions(count);
  t1.join();
  t2.join();
}

void RunUseFull(int count) {
  use_useless_waiter = false;
  int size, rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  auto start = std::chrono::high_resolution_clock::now();
  Run(count);
  auto end = std::chrono::high_resolution_clock::now();
  if(rank == 0)
    std::cout << "Usefull: " << std::chrono::duration_cast<std::chrono::nanoseconds>((end-start)).count() << std::endl;



}

void RunUseless(int  count) {
  use_useless_waiter = true;
  int size, rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  auto start = std::chrono::high_resolution_clock::now();
  Run(count);
  auto end = std::chrono::high_resolution_clock::now();
  if(rank == 0)
    std::cout << "Useless: " << std::chrono::duration_cast<std::chrono::nanoseconds>((end-start)).count() << std::endl;
}

void RunRandomThreads() {
  std::thread t1(std::bind(&doWork, 5000000000));
  std::thread t2(std::bind(&doWork, 5000000000));
  //MPI_Barrier(MPI_COMM_WORLD);
  doWork(5000000000);
  t1.join();
  t2.join();
}

int main(int argc, char** argv) {
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  int size, rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  //RunRandomThreads();
  use_probe = true;
  if(rank == 0)
    std::cout << "With probe" << std::endl;
  int count = 5000;
  MPI_Barrier(MPI_COMM_WORLD);
  RunUseless(count);
  MPI_Barrier(MPI_COMM_WORLD);
  RunUseFull(count);
  MPI_Barrier(MPI_COMM_WORLD);
  if(rank == 0)
    std::cout << "Without probe" << std::endl;
  use_probe = false;
  MPI_Barrier(MPI_COMM_WORLD);
  RunUseless(count);
  MPI_Barrier(MPI_COMM_WORLD);
  RunUseFull(count);
  MPI_Barrier(MPI_COMM_WORLD);

  MPI_Finalize();
}

/*
 *
 * int provided;

 */




