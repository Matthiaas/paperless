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
      compactor_(&PaperlessKV::Compact, this),
      dispatcher_(&PaperlessKV::Dispatch, this),
      get_responder_(&PaperlessKV::RespondGet, this),
      put_responder_(&PaperlessKV::RespondPut, this)
      /*get_value_tagger(0,100000000)*/ {
  MPI_Comm_rank(comm_, &rank_);
  MPI_Comm_size(comm_, &rank_size_);
  MPI_Barrier(comm);
}

PaperlessKV::~PaperlessKV() {


  shutdown_ = true;
  local_.Shutdown();
  remote_.Shutdown();
  // Send Poisonpill to own respond threads:
  MPI_Send(nullptr, 0, MPI_CHAR, rank_, KEY_PUT_TAG, comm_);
  MPI_Send(nullptr, 0, MPI_CHAR, rank_, KEY_TAG, comm_);
  compactor_.join();
  dispatcher_.join();
  get_responder_.join();
  put_responder_.join();
}

void PaperlessKV::deleteKey(const Element& key) {
  // TODO: Implement me.
}

void PaperlessKV::Compact() {

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

void PaperlessKV::Dispatch() {
  while (true) {
    MemQueue::Chunk handler = remote_.GetChunk();
    if(handler.IsPoisonPill()) {
      handler.Clear();
      return;
    }
    // TODO: Dispatch Data.
    for(const auto& [key, value] : *handler.Get()) {
      Hash hash = hash_function_(key.Value(), key.Length());
      RemotePut(key.GetView(), hash, value.GetView());
    }
    handler.Clear();
  }
}

void PaperlessKV::RespondGet() {
  while(true) {
    MPI_Status status;
    MPI_Probe(MPI_ANY_SOURCE, KEY_TAG, comm_, &status);

    if(status.MPI_SOURCE == rank_) {
      MPI_Recv(nullptr, 0, MPI_CHAR, rank_ ,KEY_TAG,comm_, &status);
      break;
    }
    OwningElement key = ReceiveKey(MPI_ANY_SOURCE, KEY_TAG, &status);

    QueryResult element =
        LocalGet(key.GetView(), hash_function_(key.Value(), key.Length()));
    SendValue(element, status.MPI_SOURCE, VALUE_TAG);
  }

}

void PaperlessKV::RespondPut() {
  while(true) {
    MPI_Status status;
    MPI_Probe(MPI_ANY_SOURCE, KEY_PUT_TAG, comm_, &status);
    int count = 10;
    MPI_Get_count(&status, MPI_CHAR, &count);

    if(status.MPI_SOURCE == rank_) {
      MPI_Recv(nullptr, 0, MPI_CHAR, rank_ ,KEY_PUT_TAG,comm_, &status);
      break;
    } else if(count == 0) {
      MPI_Recv(nullptr, 0, MPI_CHAR, status.MPI_SOURCE ,KEY_PUT_TAG,comm_, &status);
      std::lock_guard<std::mutex> lck(fence_mutex);
      fence_calls_received++;
      if (fence_calls_received == rank_size_ - 1) {
        fence_wait.notify_all();
      }
      continue;
    }

    // TODO: change interfaces and avoid some copies
    OwningElement key = ReceiveKey( status.MPI_SOURCE, KEY_PUT_TAG, &status);
    OwningElement value = ReceiveKey( status.MPI_SOURCE, VALUE_PUT_TAG, &status);
    Hash hash = hash_function_(key.Value(), key.Length());
    Owner o = hash % rank_size_;
    local_.Put(key.GetView(), Tomblement(value.Value(), value.Length()),
               hash, o);

  }
}

void PaperlessKV::Put(const Element& key, Tomblement&& value) {
  Hash hash = hash_function_(key.Value(), key.Length());
  Owner o = hash % rank_size_;
  if (o == rank_) {
    // Local insert.
    local_.Put(key, std::move(value), hash, rank_);
  } else {
    if (consistency_ == RELAXED) {
      remote_.Put(key, std::move(value), hash, rank_);
    } else if (consistency_ == SEQUENTIAL) {
      RemotePut(key, hash, value.GetView());
    }
  }
}

QueryResult PaperlessKV::Get(const Element& key) {
  Hash hash = hash_function_(key.Value(), key.Length());
  Owner o = hash % rank_size_;
  if (o == rank_) {
    return LocalGet(key, hash);
  } else {
    if (consistency_ == RELAXED) {
      return RemoteGetRelaxed(key, hash);
    } else {
      return RemoteGetValue(key, hash);
    }
  }
}

QueryResult PaperlessKV::LocalGet(const Element& key, Hash hash) {

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

QueryResult PaperlessKV::RemoteGetRelaxed(const Element& key, Hash hash) {
  QueryResult e_cache = remote_cache_.get(key);
  if (e_cache == QueryStatus::NOT_FOUND) {
    QueryResult el = remote_.Get(key, hash, rank_);
    if (el == QueryStatus::NOT_FOUND) {
      return RemoteGetValue(key, hash);
    } else {
      return el;
    }
  } else {
    return e_cache;
  }
}

QueryResult PaperlessKV::RemoteGetValue(const Element& key, Hash hash) {
  Owner o = hash % rank_size_;
  //int tag = get_value_tagger.getNextTag();
  SendKey(key, o, KEY_TAG);
  MPI_Status status;
  return ReceiveValue(o, VALUE_TAG, &status);
}

void PaperlessKV::RemotePut(const Element& key, Hash hash, const Element& value) {
  Owner o = hash % rank_size_;
  SendKey(key, o, KEY_PUT_TAG);
  SendKey(value, o, VALUE_PUT_TAG);
}

void PaperlessKV::put(const char *key, size_t key_len,const char *value,
                      size_t value_len) {
  Put(Element(key, key_len), Tomblement(value, value_len));
}

QueryResult PaperlessKV::get(const char *key, size_t key_len) {
  return Get(Element(key, key_len));
}

void PaperlessKV::deleteKey(char *key, size_t key_len) {
  deleteKey(Element(key, key_len));
}


void PaperlessKV::SendValue(const QueryResult& key, int target, int tag) {
  char queryStatus = key.hasValue() ? QueryStatus::FOUND : key.Status();
  MPI_Send(&queryStatus, 1, MPI_CHAR, target, tag, comm_);
  if(key) {
    MPI_Send(key->Value(), key->Length(), MPI_CHAR, target, tag, comm_);
  }
}


QueryResult PaperlessKV::ReceiveValue(int source, int tag, MPI_Status* status) {
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

void PaperlessKV::SendKey(const Element& key, int target, int tag) {
  MPI_Send(key.Value(), key.Length(), MPI_CHAR, target, tag, comm_);
}

OwningElement PaperlessKV::ReceiveKey(int source, int tag, MPI_Status* status) {
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

void PaperlessKV::Fence() {
  Sync();
  MPI_Barrier(comm_);
}

void PaperlessKV::FenceAndSetConsistency(PaperlessKV::Consistency c) {
  Sync();
  consistency_ = c;
  MPI_Barrier(comm_);
}

void PaperlessKV::FenceAndCheckPoint() {
  Sync();
  local_.Flush();
  MPI_Barrier(comm_);
}

void PaperlessKV::Sync() {
  remote_.Flush();
  fence_calls_received = 0;
  MPI_Barrier(comm_);
  for(int i = 0; i < rank_size_; i++) {
    if(i == rank_) continue;
    MPI_Send(nullptr, 0, MPI_CHAR, i, KEY_PUT_TAG, comm_);
  }

  std::unique_lock<std::mutex> lck(fence_mutex);
  if(fence_calls_received != rank_size_ - 1) {
    fence_wait.wait(lck);
  }
}


