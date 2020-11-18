//
// Created by matthias on 04.11.20.
//

#include "PaperlessKV.h"
#include <iostream>
#include "Element.h"

PaperlessKV::PaperlessKV(std::string id, MPI_Comm comm,
                         Consistency c = Consistency::RELAXED)
    : PaperlessKV(id, comm, 0, c) {
  // TODO: Set default HashFunction.
}

PaperlessKV::PaperlessKV(std::string id, MPI_Comm comm, HashFunction hf,
                         Consistency c = Consistency::RELAXED)
    : id_(id),
      consistency_(c),
      hash_function_(hf),
      local_(1,10000),
      remote_(1, 10000),
      storage_manager_("/tmp/..."),
      shutdown_(false),
      comm_(comm),
      compactor_(&PaperlessKV::compact, this),
      dispatcher_(&PaperlessKV::dispatch, this),
      get_responder_(&PaperlessKV::respond_get, this),
      put_responder_(&PaperlessKV::respond_put, this),
      get_value_tagger(0,100000000) {
  MPI_Comm_rank(comm_, &rank_);
  MPI_Comm_size(comm_, &rank_size_);
}

PaperlessKV::~PaperlessKV() {


  shutdown_ = true;
  local_.Shutdown();
  remote_.Shutdown();
  // Send Poisonpill to own respond get thread:
  MPI_Send(nullptr, 0, MPI_CHAR, rank_, KEY_TAG, comm_);
  compactor_.join();
  dispatcher_.join();
  get_responder_.join();
  put_responder_.join();
}

void PaperlessKV::deleteKey(const Element& key) {
  // TODO: Implement me.
}

void PaperlessKV::compact() {

  while (true) {
    MemQueue::Chunk handler = local_.GetChunk();
    if(handler.IsPoisonPill()) {
      handler.Clear();
      return;
    }
    // v  storage_manager_.flushToDisk(handler.get());
    handler.Clear();
  }

}

void PaperlessKV::dispatch() {

  while (true) {
    MemQueue::Chunk handler = remote_.GetChunk();
    if(handler.IsPoisonPill()) {
      handler.Clear();
      return;
    }
    // TODO: Dispatch Data.
    handler.Clear();
  }

}

void PaperlessKV::respond_get() {

  while(true) {
    MPI_Status status;
    MPI_Probe(MPI_ANY_SOURCE, KEY_TAG, comm_, &status);

    if(status.MPI_SOURCE == rank_) {
      MPI_Recv(nullptr, 0, MPI_CHAR, rank_ ,KEY_TAG,comm_, &status);
      break;
    }
    OwningElement key = receiveKey(MPI_ANY_SOURCE, KEY_TAG, &status);

    QueryResult element = localGet(key.GetView(), hash_function_(key.Value(), key.Length()));
    sendValue(element, status.MPI_SOURCE, VALUE_TAG);
  }

}

void PaperlessKV::respond_put() {
  // TODO: Implement me.
}

void PaperlessKV::put(const Element& key, Tomblement value) {
  Hash hash = hash_function_(key.Value(), key.Length());
  Owner o = hash % rank_size_;
  if (o == rank_) {
    // Local insert.
    local_.Put(key, std::move(value), hash, rank_);
  } else {
    if (consistency_ == RELAXED) {
      remote_.Put(key, std::move(value), hash, rank_);
    } else {
      // TODO:
      // Directly dispatch data and wait for response.
    }
  }
}

QueryResult PaperlessKV::get(const Element& key) {
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
}

QueryResult PaperlessKV::localGet(const Element& key, Hash hash) {

  QueryResult e_cache = local_cache_.get(key);
  if (e_cache == QueryStatus::NOT_FOUND) {
    QueryResult el = local_.Get(key, hash, rank_);
    if (el == QueryStatus::NOT_FOUND) {
      return storage_manager_.readFromDisk(key);
    } else {
      return el;
    }
  } else {
    return e_cache;
  }
}

QueryResult PaperlessKV::remoteGetRelaxed(const Element& key, Hash hash) {
  QueryResult e_cache = remote_cache_.get(key);
  if (e_cache == QueryStatus::NOT_FOUND) {
    QueryResult el = remote_.Get(key, hash, rank_);
    if (el == QueryStatus::NOT_FOUND) {
      return remoteGetValue(key, hash);
    } else {
      return el;
    }
  } else {
    return e_cache;
  }
}

QueryResult PaperlessKV::remoteGetValue(const Element& key, Hash hash) {
  Owner o = hash % rank_size_;
  int tag = get_value_tagger.getNextTag();
  sendKey(key, o, KEY_TAG);
  MPI_Status status;
  return receiveValue(o, VALUE_TAG, &status);
}

void PaperlessKV::put(const char *key, size_t key_len,const char *value,
                      size_t value_len) {
  put(Element(key, key_len), Tomblement(value, value_len));
}

QueryResult PaperlessKV::get(const char *key, size_t key_len) {
  return get(Element(key, key_len));
}

void PaperlessKV::deleteKey(char *key, size_t key_len) {
  deleteKey(Element(key, key_len));
}


void PaperlessKV::sendValue(const QueryResult& key, int target, int tag) {
  char queryStatus = key.hasValue() ? QueryStatus::FOUND : key.Status();
  MPI_Send(&queryStatus, 1, MPI_CHAR, target, tag, comm_);
  if(key) {
    MPI_Send(key->Value(), key->Length(), MPI_CHAR, target, tag, comm_);
  }
}


QueryResult PaperlessKV::receiveValue(int source, int tag, MPI_Status* status) {
  char queryStatus;

  MPI_Recv(&queryStatus, 1, MPI_CHAR, source, tag,comm_, status);
  if(queryStatus == QueryStatus::FOUND) {
    int value_size;
    MPI_Probe(status->MPI_SOURCE, status->MPI_TAG, comm_, status);
    MPI_Get_count(status, MPI_CHAR, &value_size);
    if(value_size == 0) {
      MPI_Recv(nullptr, 0, MPI_CHAR, status->MPI_SOURCE,
               status->MPI_TAG,comm_, status);
      return OwningElement(nullptr, 0);
    }

    char* value_buff = static_cast<char*>(std::malloc(value_size));
    MPI_Recv(value_buff, value_size, MPI_CHAR,
             status->MPI_SOURCE, status->MPI_TAG,comm_, status);
    return OwningElement::createFromBuffWithoutCopy(value_buff, value_size);
  } else {
    return static_cast<QueryStatus >(queryStatus);
  }
}

void PaperlessKV::sendKey(const Element& key, int target, int tag) {
  MPI_Send(key.Value(), key.Length(), MPI_CHAR, target, tag, comm_);
}

OwningElement PaperlessKV::receiveKey(int source, int tag, MPI_Status* status) {
  int value_size;
  MPI_Probe(source, tag, comm_, status);
  MPI_Get_count(status, MPI_CHAR, &value_size);
  // TODO: can keys have size zero?
  if(value_size == 0) {
    MPI_Recv(nullptr, 0, MPI_CHAR, status->MPI_SOURCE,
             status->MPI_TAG,comm_, status);
    return OwningElement(nullptr, 0);
  }
  char* value_buff = static_cast<char*>(std::malloc(value_size));
  MPI_Recv(value_buff, value_size, MPI_CHAR,
           status->MPI_SOURCE, status->MPI_TAG,comm_, status);
  return OwningElement::createFromBuffWithoutCopy(value_buff, value_size);
}

/*
void PaperlessKV::sendElement(Element key, int target, int tag) {

}

QueryResult PaperlessKV::receiveElement(int source, int tag, MPI_Status* status) {

  int value_size;
  MPI_Probe(source, tag, comm, status);
  MPI_Get_count(status, MPI_CHAR, &value_size);
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