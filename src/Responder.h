//
// Created by matthias on 13.12.20.
//

#ifndef PAPERLESS_RESPONDER_H
#define PAPERLESS_RESPONDER_H

#include <mpi.h>
#include "Status.h"
#include "Message.h"
#include "Types.h"
#include <condition_variable>

class PaperlessKV;


class Responder {



 public:

  bool Respond();
  void HandleSync();
  void HandlePut(PutMessage* msg, int src);
  void HandleGet(GetMessage* msg, int src);
  void SendQueryResult(const std::pair<QueryStatus, size_t>& val, int target, int tag);
  Responder(PaperlessKV* kv, bool dispatch_data_in_chunks);


  void WaitForSync();
 private:

  int rank_size_;
  PaperlessKV* kv_;
  // This is to store intermediate results
  char* big_buffer_;
  MPI_Comm comm_;
  bool dispatch_data_in_chunks_;


  volatile int fence_calls_received = 0;
  std::mutex fence_mutex;
  std::condition_variable fence_wait;
};

#include "PaperlessKV.h"

#endif  // PAPERLESS_RESPONDER_H
