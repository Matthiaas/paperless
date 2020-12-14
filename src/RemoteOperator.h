//
// Created by matthias on 14.12.20.
//

#ifndef PAPERLESS_REMOTEOPERATOR_H
#define PAPERLESS_REMOTEOPERATOR_H

#include <mpi.h>

#include <atomic>

#include "Status.h"
#include "Types.h"

class RemoteOperator {

 public:
  RemoteOperator(MPI_Comm comm, bool dispatch_data_in_chunks_);
  std::pair<QueryStatus, size_t> Get(
      const ElementView &key, const ElementView &v_buff, Hash hash);
  QueryResult Get(const ElementView &key, Hash hash);
  void PutSequential(const ElementView &key, Hash hash,
                           const Tomblement &value);

  void InitSync();

 private:
  class Tagger {
   public:
    Tagger(int min, int max)
        : min_get_key(min), max_get_key(max) {
    }
    int getNextTag() {
      return  min_get_key + (cur_get_key++ % max_get_key);
    }
   private:
    const int min_get_key;
    const int max_get_key;
    std::atomic<int> cur_get_key = 0;
  };



  int getTag();
  MPI_Comm comm_;
  bool dispatch_data_in_chunks_;
  Tagger get_value_tagger;
  int rank_size_;
  int rank_;


};

#endif  // PAPERLESS_REMOTEOPERATOR_H
