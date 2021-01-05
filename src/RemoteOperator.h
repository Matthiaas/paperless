//
// Created by matthias on 14.12.20.
//

#ifndef PAPERLESS_REMOTEOPERATOR_H
#define PAPERLESS_REMOTEOPERATOR_H

#include <mpi.h>

#include <atomic>

#include "FutureQueryInfo.h"
#include "Message.h"
#include "Status.h"
#include "Types.h"

class RemoteOperator {
 public:
  RemoteOperator(MPI_Comm comm, bool dispatch_data_in_chunks_);
  std::pair<QueryStatus, size_t> Get(const ElementView &key,
                                     const ElementView &v_buff, Hash hash);
  FutureQueryInfo IGet(const ElementView &key,
                                     const ElementView &v_buff, Hash hash);


  QueryResult Get(const ElementView &key, Hash hash);
  void PutSequential(const ElementView &key, Hash hash,
                     const Tomblement &value);

  // rqs should be an array of at least length 3
  void IPutSequential(const ElementView &key, Hash hash,
                     const Tomblement &value, MPI_Request* rqs);

  void InitSync();
  void Kill();

  int getTag();
  MPI_Comm comm_;
  bool dispatch_data_in_chunks_;
  int rank_size_;
  int rank_;

 private:

  Message InitGet(const ElementView &key, Hash hash);

  int min_get_key;
  int max_get_key;
  std::atomic<int> cur_get_key = 0;
  Message InitGetAsync(const ElementView &key, Hash hash);
};

#endif  // PAPERLESS_REMOTEOPERATOR_H
