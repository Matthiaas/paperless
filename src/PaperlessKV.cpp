//
// Created by matthias on 04.11.20.
//

#include "PaperlessKV.h"
#include <smhasher/MurmurHash3.h>
#include <iostream>
#include "Element.h"

int GetRank(MPI_Comm comm) {
  int rank;
  MPI_Comm_rank(comm, &rank);
  return rank;
}

PaperlessKV::PaperlessKV(std::string id, MPI_Comm comm, uint32_t hash_seed,
                         PaperlessKV::Options options)
    : PaperlessKV(id, comm,
                  [hash_seed] (const char* value, size_t len) -> Hash {
                    uint32_t res;
                    MurmurHash3_x86_32(value, len, hash_seed, &res);
                    return static_cast<Hash>(res);
                  }, options) {
}

PaperlessKV::PaperlessKV(std::string id, MPI_Comm comm, HashFunction hf,
                         PaperlessKV::Options options)
    : id_(id),
      consistency_(options.consistency),
      mode_(options.mode),
      hash_function_(hf),
      local_(1,options.max_local_memtable_size),
      remote_(1, options.max_remote_memtable_size),
      local_cache_(options.max_local_cache_size),
      remote_cache_(options.max_remote_cache_size),
      storage_manager_(id + std::to_string(GetRank(comm))),
      shutdown_(false),
      comm_(comm),
      compactor_(&PaperlessKV::Compact, this),
      dispatcher_(&PaperlessKV::Dispatch, this),
      get_responder_(&PaperlessKV::RespondGet, this),
      put_responder_(&PaperlessKV::RespondPut, this),
      dispatch_data_in_chunks_(options.dispatch_data_in_chunks) {
  MPI_Comm_rank(comm_, &rank_);
  MPI_Comm_size(comm_, &rank_size_);
  MPI_Barrier(comm);
}

PaperlessKV::~PaperlessKV() {

  if(!shutdown_) {
    Shutdown();
  }
}

void PaperlessKV::Shutdown() {
  Fence();
  shutdown_ = true;
  local_.Shutdown();
  remote_.Shutdown();
  // Send Poisonpills to own respond threads:
  MPI_Send(nullptr, 0, MPI_CHAR, rank_, KEY_PUT_TAG, comm_);
  MPI_Send(nullptr, 0, MPI_CHAR, rank_, KEY_TAG, comm_);
  compactor_.join();
  dispatcher_.join();
  get_responder_.join();
  put_responder_.join();
}

void PaperlessKV::deleteKey(const ElementView& key) {
  Put(key, Tomblement::getATombstone());
}

void PaperlessKV::Compact() {
  while (true) {
    MemQueue::Chunk handler = local_.GetChunk();
    if(handler.IsPoisonPill()) {
      handler.Clear();
      return;
    }
    storage_manager_.flushToDisk(*handler.Get());
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
      RemotePutSequential(key.GetView(), hash, value);
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
    Element key = ReceiveKey(MPI_ANY_SOURCE, KEY_TAG, &status);
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
    Element key = ReceiveKey(status.MPI_SOURCE, KEY_PUT_TAG, &status);
    Tomblement value =
        ReceiveTomblementSequential(status.MPI_SOURCE, VALUE_PUT_TAG, &status);
    Hash hash = hash_function_(key.Value(), key.Length());
    Owner o = hash % rank_size_;
    local_.Put(key.GetView(), std::move(value), hash, o);

  }
}

void PaperlessKV::Put(const ElementView& key, Tomblement&& value) {
  Hash hash = hash_function_(key.Value(), key.Length());
  Owner o = hash % rank_size_;
  if (o == rank_) {
    // Local insert.
    if(value.Tombstone()) {
      local_cache_.put(key, hash, QueryStatus::DELETED);
    } else {
      local_cache_.put(key, hash, Element::copyElementContent(value.GetView()));
    }
    local_.Put(key, std::move(value), hash, rank_);
  } else {
    if (consistency_ == RELAXED) {
      if(value.Tombstone()) {
        remote_cache_.put(key, hash, QueryStatus::DELETED);
      } else {
        remote_cache_.put(key, hash, Element::copyElementContent(value.GetView()));
      }
      remote_.Put(key, std::move(value), hash, rank_);
    } else if (consistency_ == SEQUENTIAL) {
      RemotePutSequential(key, hash, value);
    }
  }
}

QueryResult PaperlessKV::Get(const ElementView& key) {
  Hash hash = hash_function_(key.Value(), key.Length());
  Owner o = hash % rank_size_;
  if (o == rank_) {
    return LocalGet(key, hash);
  } else {
    if (consistency_ == RELAXED) {
      return RemoteGetRelaxed(key, hash);
    } else { // SEQUENTIAL
      QueryResult qr = RemoteGetValue(key, hash);
      if(mode_ == READONLY) {
        remote_cache_.put(key, hash, qr);
      }
      return qr;
    }
  }
}

QueryResult PaperlessKV::LocalGet(const ElementView& key, Hash hash) {
  std::optional<QueryResult> cache_result = local_cache_.get(key, hash);
  if(cache_result) {
    return std::move(cache_result.value());
  } else {
    QueryResult el = local_.Get(key, hash, rank_);
    if (el == QueryStatus::NOT_FOUND) {
      el = storage_manager_.readFromDisk(key);
    }
    local_cache_.put(key, hash, el);
    return el;
  }
}

std::pair<QueryStatus, size_t> PaperlessKV::LocalGet(const ElementView& key,
                                                     const ElementView& buff,
                                                     Hash hash) {
  // TODO: implement this for cache.
  std::pair<QueryStatus, size_t> cache_result = local_cache_.get(key, hash, buff);
  if(cache_result.first== QueryStatus::NOT_IN_CACHE) {
    std::pair<QueryStatus, size_t> el =
        local_.Get(key, buff.Value(), buff.Length(), hash, rank_);
    if (el.first == QueryStatus::NOT_FOUND) {
      el = storage_manager_.readFromDisk(key, buff);
    }
    if(el.first == QueryStatus::FOUND) {
      local_cache_.put(key, hash, Element(buff.Value(), el.second));
    } else if(el.first == QueryStatus::DELETED
              || el.first == QueryStatus::NOT_FOUND) {
      local_cache_.put(key, hash, el.first);
    }
    return el;
  } else {
    return cache_result;
  }
}


QueryResult PaperlessKV::RemoteGetRelaxed(const ElementView& key, Hash hash) {
  std::optional<QueryResult> cache_result = remote_cache_.get(key, hash);
  if(cache_result) {
    return std::move(cache_result.value());
  } else {
    QueryResult el = remote_.Get(key, hash, rank_);
    if (el == QueryStatus::NOT_FOUND) {
      el = RemoteGetValue(key, hash);
    }
    remote_cache_.put(key, hash, el);
    return el;
  }
}

std::pair<QueryStatus, size_t> PaperlessKV::RemoteGetRelaxed(
    const ElementView& key, const ElementView& v_buff, Hash hash) {
  // TODO: implement this for cache.
  std::pair<QueryStatus, size_t> cache_result = local_cache_.get(key, hash, v_buff);
  if(cache_result.first== QueryStatus::NOT_IN_CACHE) {
    std::pair<QueryStatus, size_t> el = remote_.Get(
        key, v_buff.Value(), v_buff.Length(), hash, rank_);
    if (el.first == QueryStatus::NOT_FOUND) {
      el = RemoteGetValue(key, v_buff, hash);
    }
    if(el.first == QueryStatus::FOUND) {
      remote_cache_.put(key, hash, Element(v_buff.Value(), el.second));
    } else if(el.first == QueryStatus::DELETED
                  || el.first == QueryStatus::NOT_FOUND) {
      remote_cache_.put(key, hash, el.first);
    }

    return el;
  } else {
    return cache_result;
  }
}


QueryResult PaperlessKV::RemoteGetValue(const ElementView& key, Hash hash) {
  Owner o = hash % rank_size_;
  //int tag = get_value_tagger.getNextTag();
  SendKey(key, o, KEY_TAG);
  MPI_Status status;
  return ReceiveQueryResult(o, VALUE_TAG, &status);
}

std::pair<QueryStatus, size_t> PaperlessKV::RemoteGetValue(
    const ElementView& key, const ElementView& v_buff, Hash hash) {
  Owner o = hash % rank_size_;
  SendKey(key, o, KEY_TAG);
  MPI_Status status;
  return ReceiveValueIntoBuffer(v_buff,o, VALUE_TAG, &status);
}


void PaperlessKV::RemotePutSequential(const ElementView& key, Hash hash, const Tomblement& value) {
  Owner o = hash % rank_size_;
  SendKey(key, o, KEY_PUT_TAG);
  SendValue(value, o, VALUE_PUT_TAG);
}

void PaperlessKV::put(const char *key, size_t key_len,const char *value,
                      size_t value_len) {
  Put(ElementView(key, key_len), Tomblement(value, value_len));
}

QueryResult PaperlessKV::get(const char *key, size_t key_len) {
  return Get(ElementView(key, key_len));
}

std::pair<QueryStatus, size_t> PaperlessKV::get(
    const char* key_buff, size_t key_len, char* value_buff, size_t value_buff_len) {

  ElementView key(key_buff, key_len);
  ElementView buff(value_buff, value_buff_len);

  Hash hash = hash_function_(key_buff, key_len);
  Owner o = hash % rank_size_;
  if (o == rank_) {
    return LocalGet(key, buff, hash);
  } else {
    if (consistency_ == RELAXED) {
      return RemoteGetRelaxed(key, buff, hash);
    } else { // SEQUENTIAL
      std::pair<QueryStatus, size_t> el = RemoteGetValue(key, buff, hash);
      if(mode_ == READONLY) {
        if(el.first == QueryStatus::FOUND) {
          remote_cache_.put(key, hash, Element(buff.Value(), el.second));
        } else if(el.first == QueryStatus::DELETED
                  || el.first == QueryStatus::NOT_FOUND) {
          remote_cache_.put(key, hash, el.first);
        }
      }
      return el;
    }
  }


  return std::pair<QueryStatus, size_t>();
}


void PaperlessKV::deleteKey(const char *key, size_t key_len) {
  deleteKey(ElementView(key, key_len));
}

void PaperlessKV::SendValue(const Tomblement& value, int target, int tag) {
  MPI_Send(value.GetBuffer(), value.GetBufferLen(),
           MPI_CHAR, target, tag, comm_);
}

void PaperlessKV::SendValue(const QueryResult& key, int target, int tag) {
  char queryStatus = key.hasValue() ? QueryStatus::FOUND : key.Status();
  MPI_Send(&queryStatus, 1, MPI_CHAR, target, tag, comm_);
  if(key) {
    MPI_Send(key->Value(), key->Length(), MPI_CHAR, target, tag, comm_);
  }
}


QueryResult PaperlessKV::ReceiveQueryResult(int source, int tag, MPI_Status* status) {
  char queryStatus;

  MPI_Recv(&queryStatus, 1, MPI_CHAR, source, tag,comm_, status);
  if(queryStatus == QueryStatus::FOUND) {
    int value_size;
    MPI_Probe(status->MPI_SOURCE, status->MPI_TAG, comm_, status);
    MPI_Get_count(status, MPI_CHAR, &value_size);
    if(value_size == 0) {
      MPI_Recv(nullptr, 0, MPI_CHAR, status->MPI_SOURCE,
               status->MPI_TAG,comm_, status);
      return Element(nullptr, 0);
    }

    char* value_buff = static_cast<char*>(std::malloc(value_size));
    MPI_Recv(value_buff, value_size, MPI_CHAR,
             status->MPI_SOURCE, status->MPI_TAG,comm_, status);
    return Element::createFromBuffWithoutCopy(value_buff, value_size);
  } else {
    return static_cast<QueryStatus >(queryStatus);
  }
}

std::pair<QueryStatus, size_t> PaperlessKV::ReceiveValueIntoBuffer(
    const ElementView& v_buff, int source, int tag, MPI_Status* status) {
  char queryStatus;
  MPI_Recv(&queryStatus, 1, MPI_CHAR, source, tag,comm_, status);
  if(queryStatus == QueryStatus::FOUND) {
    int value_size;
    MPI_Probe(status->MPI_SOURCE, status->MPI_TAG, comm_, status);
    MPI_Get_count(status, MPI_CHAR, &value_size);
    if(value_size == 0) {
      MPI_Recv(nullptr, 0, MPI_CHAR, status->MPI_SOURCE,
               status->MPI_TAG,comm_, status);
      return {QueryStatus ::FOUND, 0};
    }
    if( value_size > (int) v_buff.Length()) {
      // TODO: MPI DISCARD/ DROP MSG
      char* drop_buff = static_cast<char*>(std::malloc(value_size));
      MPI_Recv(drop_buff, value_size, MPI_CHAR,
               status->MPI_SOURCE, status->MPI_TAG,comm_, status);
      free(drop_buff);
      return {QueryStatus::BUFFER_TOO_SMALL, value_size};
    } else {
      MPI_Recv(v_buff.Value(), value_size, MPI_CHAR,
               status->MPI_SOURCE, status->MPI_TAG,comm_, status);
      return {QueryStatus::FOUND, value_size};
    }
  } else {
    return {static_cast<QueryStatus >(queryStatus), 0};
  }
}

Tomblement PaperlessKV::ReceiveTomblementSequential(int source, int tag, MPI_Status* status) {
  int value_size;
  MPI_Probe(source, tag, comm_, status);
  MPI_Get_count(status, MPI_CHAR, &value_size);
  char* value_buff = static_cast<char*>(std::malloc(value_size));
  MPI_Recv(value_buff, value_size, MPI_CHAR,
           status->MPI_SOURCE, status->MPI_TAG,comm_, status);
  return Tomblement::createFromBuffWithoutCopy(value_buff, value_size);
}

void PaperlessKV::SendKey(const ElementView& key, int target, int tag) {
  MPI_Send(key.Value(), key.Length(), MPI_CHAR, target, tag, comm_);
}

Element PaperlessKV::ReceiveKey(int source, int tag, MPI_Status* status) {
  int value_size;
  MPI_Probe(source, tag, comm_, status);
  MPI_Get_count(status, MPI_CHAR, &value_size);
  // TODO: can keys have size zero?
  if(value_size == 0) {
    MPI_Recv(nullptr, 0, MPI_CHAR, status->MPI_SOURCE,
             status->MPI_TAG,comm_, status);
    return Element(nullptr, 0);
  }
  char* value_buff = static_cast<char*>(std::malloc(value_size));
  MPI_Recv(value_buff, value_size, MPI_CHAR,
           status->MPI_SOURCE, status->MPI_TAG,comm_, status);
  return Element::createFromBuffWithoutCopy(value_buff, value_size);
}

void PaperlessKV::Fence() {
  remote_cache_.clear();
  Sync();
  MPI_Barrier(comm_);
}

void PaperlessKV::FenceAndChangeOptions(PaperlessKV::Consistency_t c, Mode_t mode) {
  Sync();
  consistency_ = c;
  if(c != RELAXED) {
    remote_cache_.clear();
  }
  mode_ = mode;
  MPI_Barrier(comm_);
}

void PaperlessKV::FenceAndCheckPoint() {
  remote_cache_.clear();
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

