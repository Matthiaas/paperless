//
// Created by matthias on 13.12.20.
//

#ifndef PAPERLESS_RESPONDER_H
#define PAPERLESS_RESPONDER_H

#include <mpi.h>

#include <condition_variable>

#include "Message.h"
#include "Status.h"
#include "Types.h"

class PaperlessKV;

class Responder {
 public:
  bool Respond();
  void HandleSync();
  void HandlePut(const Message& msg, int src);
  void HandleGet(const Message& msg, int src);
  void SendQueryResult(const std::pair<QueryStatus, size_t>& val, int target,
                       int tag);
  Responder(PaperlessKV* kv, MPI_Comm comm, bool dispatch_data_in_chunks);
  ~Responder();

  void WaitForSync();

 private:
  PaperlessKV* kv_;
  // This is to store intermediate results

  MPI_Comm comm_;
  int rank_size_;
  bool dispatch_data_in_chunks_;
  char* big_buffer_;

  volatile int fence_calls_received = 0;
  std::mutex fence_mutex;
  std::condition_variable fence_wait;
};

#include "PaperlessKV.h"

#endif  // PAPERLESS_RESPONDER_H
