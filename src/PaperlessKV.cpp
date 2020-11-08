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
    : PaperlessKV(id, comm, 0, c) {}

PaperlessKV::PaperlessKV(std::string id, MPI_Comm comm, HashFunction hf,
                         Consistency c = Consistency::RELAXED)
    : id_(id),
      consistency(c),
      local_(getCommSize(comm)),
      remote_(getCommSize(comm)),
      hash_function_(hf),
      compactor_(&PaperlessKV::compact, this),
      dispatcher_(&PaperlessKV::dispatch, this),
      responder_(&PaperlessKV::respond, this),
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

void PaperlessKV::respond() {
  while(!shutdown_) {
    MPI_Status status;
    int key_size;
    MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, comm, &status);
    MPI_Get_count(&status, MPI_INT, &key_size);
    char* key_buff = static_cast<char*>(std::malloc(key_size));
    MPI_Recv(key_buff, key_size, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG,
             comm, &status);

    // Query for the element
    Element key(key_buff, key_size);
    std::optional<Element> el =
        localGet(key, hash_function_(key.value, key.len));
    free(key_buff);

    // Send result might be of 0 len if result is empty.
    if(el) {
      MPI_Send(el->value, el->len, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, comm);
    } else {
      MPI_Send(nullptr, 0, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, comm);
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

std::optional<Element> PaperlessKV::get(Element key) {
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

std::optional<Element> PaperlessKV::localGet(Element key, Hash hash) {
  std::optional<Element> e_cache = local_cache_.get(key);
  if (e_cache) {
    return Element::copyElementContent(*e_cache);
  } else {
    std::optional<Element> el = local_.get(key, hash, rank_);
    if (el) {
      return el;
    } else {
      return storage_manager_.readFromDisk(key);
    }
  }
}

std::optional<Element> PaperlessKV::remoteGetRelaxed(Element key, Hash hash) {
  std::optional<Element> e_cache = remote_cache_.get(key);
  if (e_cache) {
    return Element::copyElementContent(*e_cache);
  } else {
    std::optional<Element> el = remote_.get(key, hash, rank_);
    if (el) {
      return el;
    } else {
      return remoteGetNow(key, hash);
    }
  }
}

std::optional<Element> PaperlessKV::remoteGetNow(Element key, Hash hash) {
  Owner o = hash & rank_size_;
  int tag = get_value_tagger.getNextTag();

  MPI_Send(key.value, key.len, MPI_CHAR, o, tag, comm);

  MPI_Status status;
  int value_size;
  MPI_Probe(o, tag, comm, &status);
  MPI_Get_count(&status, MPI_INT, &value_size);
  if(value_size == 0) {
    MPI_Recv(nullptr, 0, MPI_CHAR, o, tag,comm, &status);
    return std::nullopt;
  }
  char* value_buff = static_cast<char*>(std::malloc(value_size));
  MPI_Recv(value_buff, value_size, MPI_CHAR, o, tag,comm, &status);
  return Element(value_buff, value_size);
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



