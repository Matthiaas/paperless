//
// Created by matthias on 04.11.20.
//

#include "PaperlessKV.h"
#include "Element.h"

int getCommSize(MPI_Comm comm) {
  int rank_size_;
  MPI_Comm_size(comm, &rank_size_);
  return rank_size_;
}

PaperlessKV::PaperlessKV(std::string id, MPI_Comm comm,
                         Consistency c = Consistency::RELAXED)
    : PaperlessKV(id, comm, 0, c) {
  // TODO: Set default HashFunction.
}

PaperlessKV::PaperlessKV(std::string id, MPI_Comm comm, HashFunction hf,
                         Consistency c = Consistency::RELAXED)
    : id_(id),
      consistency_(c),
      local_(getCommSize(comm),10),
      remote_(getCommSize(comm), 10),
      hash_function_(hf),
      compactor_(&PaperlessKV::compact, this),
      dispatcher_(&PaperlessKV::dispatch, this),
      get_responder_(&PaperlessKV::respond_get, this),
      put_responder_(&PaperlessKV::respond_put, this),
      storage_manager_("/tmp/..."),
      shutdown_(false),
      get_value_tagger(0,100000000) {
  MPI_Comm_rank(comm, &rank_);
  MPI_Comm_size(comm, &rank_size_);
}

PaperlessKV::~PaperlessKV() { shutdown_ = true; }

void PaperlessKV::deleteKey(Element key) {
  // TODO: Implement me.
}

void PaperlessKV::compact() {
  while (!shutdown_) {
    MemQueu::Chunk handler = local_.getChunk();
    // v  storage_manager_.flushToDisk(handler.get());
    handler.clear();
  }
}

void PaperlessKV::dispatch() {
  while (!shutdown_) {
    MemQueu::Chunk handler = remote_.getChunk();
    // TODO: Dispatch Data.
    handler.clear();
  }
}

void PaperlessKV::respond_get() {
  /*
  while(!shutdown_) {
    MPI_Status status;
    QueryResult key = receiveElement(MPI_ANY_SOURCE, MPI_ANY_TAG, &status);
    if(!key) {
      continue;
    }
    auto element = localGet(*key, hash_function_(key->v, key->len));
    // TODO: There might be an unnecessary value allocate/ copy/ free happening here.
    free(element->value);

    // Send result, might be of 0 len if result is empty.
    if(element) {
      sendElement(*element, status.MPI_SOURCE, status.MPI_TAG);
      free(element->value);
    } else {
      sendElement({nullptr,0}, status.MPI_SOURCE, status.MPI_TAG);
    }

  }
   */
}

void PaperlessKV::respond_put() {
  // TODO: Implement me.
}

void PaperlessKV::put(Element key, Tomblement value) {
/*
  Hash hash = hash_function_(key->value, key->len);
  Owner o = hash % rank_size_;
  if (o == rank_) {
    // Local insert.
    local_.put(key, value, hash, rank_);
  } else {
    if (consistency_ == RELAXED) {
      remote_.put(key, value, hash, rank_);
    } else {
      // TODO:
      // Directly dispatch data and wait for response.
    }
  }
*/
}

QueryResult PaperlessKV::get(Element key) {
  /*
  Hash hash = hash_function_(key.Value(), key.Length());
  Owner o = hash % rank_size_;
  if (o == rank_) {
    return localGet(key, hash);
  } else {
    if (consistency_ == RELAXED) {
      return remoteGetRelaxed(key, hash);
    } else {
      return remoteGetValue(key, hash);
    }
  }
   */
}

QueryResult PaperlessKV::localGet(Element key, Hash hash) {
  /*
  QueryResult e_cache = local_cache_.get(key);
  if (e_cache == QueryStatus::NOT_FOUND) {
    QueryResult el = local_.get(key, hash, rank_);
    if (el == QueryStatus::NOT_FOUND) {
      return storage_manager_.readFromDisk(key);
    } else {
      return el;
    }
  } else {
    return e_cache;
  }
   */
}

QueryResult PaperlessKV::remoteGetRelaxed(Element key, Hash hash) {
  /*
  QueryResult e_cache = remote_cache_.get(key);
  if (e_cache == QueryStatus::NOT_FOUND) {
    QueryResult el = remote_.get(key, hash, rank_);
    if (el == QueryStatus::NOT_FOUND) {
      return remoteGetValue(key, hash);
    } else {
      return el;
    }
  } else {
    return e_cache;
  }
  */
}

QueryResult PaperlessKV::remoteGetValue(Element key, Hash hash) {
  /*
  Owner o = hash & rank_size_;
  int tag = get_value_tagger.getNextTag();
  sendElement(key, o, tag);
  MPI_Status status;
  return receiveElement(o, tag, &status);
   */
}

void PaperlessKV::put(char *key, size_t key_len, char *value,
                      size_t value_len) {
  put(Element(key, key_len),
      Tomblement(value, value_len));
}

void PaperlessKV::get(char *key, size_t key_len) {
  get(Element(key, key_len));
}

void PaperlessKV::deleteKey(char *key, size_t key_len) {
  deleteKey(Element(key, key_len));
}
/*
void PaperlessKV::sendElement(Element key, int target, int tag) {
  MPI_Send(key.value, key.len, MPI_CHAR, target, tag, comm);
}

QueryResult PaperlessKV::receiveElement(int source, int tag, MPI_Status* status) {
  /*
  int value_size;
  MPI_Probe(source, tag, comm, status);
  MPI_Get_count(status, MPI_INT, &value_size);
  if(value_size == 0) {
    MPI_Recv(nullptr, 0, MPI_CHAR, source, tag,comm, status);
    return QueryStatus::NOT_FOUND;
  }
  char* value_buff = static_cast<char*>(std::malloc(value_size));
  MPI_Recv(value_buff, value_size, MPI_CHAR,
           status->MPI_SOURCE, status->MPI_TAG,comm, status);
  return Element(value_buff, value_size);
   *
}

*/