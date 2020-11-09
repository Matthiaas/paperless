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
  // TODO: Set default HashFunctino.
}

PaperlessKV::PaperlessKV(std::string id, MPI_Comm comm, HashFunction hf,
                         Consistency c = Consistency::RELAXED)
    : id_(id),
      consistency(c),
      local_(getCommSize(comm)),
      remote_(getCommSize(comm)),
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

void PaperlessKV::compact() {
  while (!shutdown_) {
    DUMMY::Handler handler = local_.getDataChunck();
    storage_manager_.flushToDisk(handler.get());
    handler.clear();
  }
}

void PaperlessKV::dispatch() {
  while (!shutdown_) {
    DUMMY::Handler handler = remote_.getDataChunck();
    // TODO: Dispatch Data.
    handler.clear();
  }
}

void PaperlessKV::respond_get() {
  while(!shutdown_) {
    MPI_Status status;
    QueryResult key = receiveElement(MPI_ANY_SOURCE, MPI_ANY_TAG, &status);
    if(!key) {
      continue;
    }
    auto element = localGet(*key, hash_function_(key->value, key->len));
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
}

void PaperlessKV::put(Element key, Element value) {
  Hash hash = hash_function_(key.value, key.len);
  Owner o = hash % rank_size_;
  if (o == rank_) {
    // Local insert.
    local_.put(key, value, hash, rank_);
  } else {
    if (consistency == RELAXED) {
      remote_.put(key, value, hash, rank_);
    } else {
      // TODO:
      // Directly dispatch data and wait for response.
    }
  }
}

QueryResult PaperlessKV::get(Element key) {
  Hash hash = hash_function_(key.value, key.len);
  Owner o = hash % rank_size_;
  if (o == rank_) {
    return localGet(key, hash);
  } else {
    if (consistency == RELAXED) {
      return remoteGetRelaxed(key, hash);
    } else {
      return remoteGetNow(key, hash);
    }
  }
}

QueryResult PaperlessKV::localGet(Element key, Hash hash) {
  QueryResult e_cache = local_cache_.get(key);
  if (e_cache) {
    return Element::copyElementContent(*e_cache);
  } else {
    QueryResult el = local_.get(key, hash, rank_);
    if (el) {
      return el;
    } else {
      return storage_manager_.readFromDisk(key);
    }
  }
}

QueryResult PaperlessKV::remoteGetRelaxed(Element key, Hash hash) {
  QueryResult e_cache = remote_cache_.get(key);
  if (e_cache) {
    return Element::copyElementContent(*e_cache);
  } else {
    QueryResult el = remote_.get(key, hash, rank_);
    if (el) {
      return el;
    } else {
      return remoteGetNow(key, hash);
    }
  }
}

QueryResult PaperlessKV::remoteGetNow(Element key, Hash hash) {
  Owner o = hash & rank_size_;
  int tag = get_value_tagger.getNextTag();
  sendElement(key, o, tag);
  MPI_Status status;
  return receiveElement(o, tag, &status);
}

void PaperlessKV::put(char *key, size_t key_len, char *value,
                      size_t value_len) {
  put(Element(key, key_len), Element(value, value_len));
}

void PaperlessKV::get(char *key, size_t key_len) {
  get(Element(key, key_len));
}

void PaperlessKV::deleteKey(char *key, size_t key_len) {
  deleteKey(Element(key, key_len));
}

void PaperlessKV::sendElement(Element key, int target, int tag) {
  MPI_Send(key.value, key.len, MPI_CHAR, target, tag, comm);
}

QueryResult PaperlessKV::receiveElement(int source, int tag, MPI_Status* status) {
  int value_size;
  MPI_Probe(source, tag, comm, status);
  MPI_Get_count(status, MPI_INT, &value_size);
  if(value_size == 0) {
    MPI_Recv(nullptr, 0, MPI_CHAR, source, tag,comm, status);
    return QueryStatus ::NOT_FOUND;
  }
  char* value_buff = static_cast<char*>(std::malloc(value_size));
  MPI_Recv(value_buff, value_size, MPI_CHAR,
           status->MPI_SOURCE, status->MPI_TAG,comm, status);
  return Element(value_buff, value_size);
}
