//
// Created by matthias on 04.11.20.
//

#include "PaperlessKV.h"

#include <smhasher/MurmurHash3.h>
#include <zconf.h>

#include <iostream>

#include "Common.h"
#include "Element.h"

int GetRank(MPI_Comm comm) {
  int rank;
  MPI_Comm_rank(comm, &rank);
  return rank;
}

int GetRankSize(MPI_Comm comm) {
  int rank;
  MPI_Comm_size(comm, &rank);
  return rank;
}

PaperlessKV::PaperlessKV(std::string id, MPI_Comm comm, uint32_t hash_seed,
                         PaperlessKV::Options options)
    : PaperlessKV(
          id, comm,
          [hash_seed](const char *value, size_t len) -> Hash {
            uint32_t res;
            MurmurHash3_x86_32(value, len, hash_seed, &res);
            return static_cast<Hash>(res);
          },
          options) {}

PaperlessKV::PaperlessKV(std::string id, MPI_Comm comm, HashFunction hf,
                         PaperlessKV::Options options)
    : id_(id),
      consistency_(options.consistency),
      mode_(options.mode),
      hash_function_(hf),
      local_(1, options.max_local_memtable_size),
      remote_(1, options.max_remote_memtable_size),
#ifdef  PAPERLESS_USE_HASH_CACHE
      local_cache_(options.max_local_cache_size, options.avg_key_size),
      remote_cache_(options.max_remote_cache_size, options.avg_key_size),
#else
      local_cache_(options.max_local_cache_size),
      remote_cache_(options.max_remote_cache_size),
#endif
      storage_manager_(options.strorage_location + std::string("/") +
                       std::to_string(GetRank(comm))),
      shutdown_(false),
      comm_(comm),
      responder_(this, comm, options.dispatch_data_in_chunks),
      remoteOperator_(comm, options.dispatch_data_in_chunks),
      compactor_(&PaperlessKV::Compact, this),
      dispatcher_(&PaperlessKV::Dispatch, this),
      responder_task(&PaperlessKV::Respond, this),
      dispatch_data_in_chunks_(options.dispatch_data_in_chunks),
      batch_size_(options.batch_size) {
  MPI_Comm_rank(comm_, &rank_);
  MPI_Comm_size(comm_, &rank_size_);
  MPI_Barrier(comm);
}

PaperlessKV::~PaperlessKV() {
  if (!shutdown_) {
    Shutdown();
  }
}

void PaperlessKV::Shutdown() {
  shutdown_ = true;
  Fence();
  // Send Poisonpills to own respond threads:
  local_.Shutdown();
  remote_.Shutdown();
  compactor_.join();
  dispatcher_.join();
  remoteOperator_.Kill();
  responder_task.join();
}

////////////////////////////////////////////////////////////////////////////////
/////                                                                      /////
/////                      Background Tasks /////
/////                                                                      /////
////////////////////////////////////////////////////////////////////////////////

void PaperlessKV::Compact() {
  while (true) {
    MemQueue::Chunk handler = local_.GetChunk();
    if (handler.IsPoisonPill()) {
      handler.Clear();
      return;
    }
    auto *mtable = handler.Get();
    storage_manager_.flushToDisk(*mtable);
    local_cache_.invalidateAll(*mtable, hash_function_);
    handler.Clear();
  }
}

void PaperlessKV::Dispatch() {
  while (true) {
    MemQueue::Chunk handler = remote_.GetChunk();
    if (handler.IsPoisonPill()) {
      handler.Clear();
      return;
    }
    // consistency_ == SEQUENTIAL should never come here.
    if (dispatch_data_in_chunks_ && consistency_ != SEQUENTIAL) {
      /*
      // First is count, second is size
      std::vector<std::pair<size_t, size_t>> per_rank(rank_size_);
      for (const auto&[key, value] : *handler.Get()) {
        Hash hash = hash_function_(key.Value(), key.Length());
        Owner o = hash % rank_size_;
        auto &p = per_rank[o];
        p.first += 2;
        p.second += key.Length() + value.GetBufferLen();
      }

      // fist is buffer second current location.
      std::vector<std::pair<char *, size_t >> send_buffers(rank_size_);
      for (int i = 0; i < rank_size_; i++) {
        if (i == rank_) continue;
        auto &p = per_rank[i];
        send_buffers[i] = std::make_pair(
            static_cast<char *>(PAPERLESS::malloc(p.second + p.first *
      sizeof(int))), 0);
      }
      // TODO: do not compute hash two times.
      for (const auto&[key, value] : *handler.Get()) {
        Hash hash = hash_function_(key.Value(), key.Length());
        Owner o = hash % rank_size_;
        auto &p = send_buffers[o];
        char *buff = p.first + p.second;
        WriteIntToBuff(buff, key.Length());
        WriteIntToBuff(buff + 4, value.GetBufferLen());
        buff += 8;
        std::memcpy(buff, key.Value(), key.Length());
        buff += key.Length();
        std::memcpy(buff, value.GetBuffer(), value.GetBufferLen());
        buff += value.GetBufferLen();
        p.second += 8 + key.Length() + value.GetBufferLen();
        // Send data with ISend
      }

      for (int i = 0; i < rank_size_; i++) {
        auto &p = send_buffers[i];
        if (p.second == 0 || i == rank_) {
          continue;
        }
        MPI_Send(p.first, p.second, MPI_CHAR, i, KEY_PUT_TAG, comm_);
        free(p.first);
      }
      */
      // TODO: Wait for all Isend to be completed:

      MPI_Request* rqs = static_cast<MPI_Request*>(
          PAPERLESS::malloc(sizeof(MPI_Request) * batch_size_ * 3));
      // This is required for messages to not destruct before they get send.
      std::vector<Message> msgs;

      auto it = handler.Get()->begin();
      auto end = handler.Get()->end();
      
      while (it != end)
      {
        msgs.clear();
        msgs.reserve(batch_size_);
        
        int pos = 0;
        for(; pos < batch_size_ * 3 && it != end; pos += 3, ++it)  {
          auto& key = it->first;
          auto& value = it->second;
          Hash hash = hash_function_(key.Value(), key.Length());
          msgs.push_back(
              remoteOperator_.IPut(key.GetView(), hash, value, &rqs[pos]));
        }
        MPI_Waitall(pos, rqs, MPI_STATUSES_IGNORE);
      }
      free(rqs);


    } else {
      // Send data one by one:

      for (const auto &[key, value] : *handler.Get()) {
        Hash hash = hash_function_(key.Value(), key.Length());
        // TODO: Send data with I_Send
        remoteOperator_.PutRelaxed(key.GetView(), hash, value);
      }

    }
    remote_cache_.clear();
    handler.Clear();
  }
}

void PaperlessKV::Respond() {
  while (responder_.Respond())
    ;
}

/*
void PaperlessKV::RespondGet() {
  while (true) {
    MPI_Status status;
    MPI_Probe(MPI_ANY_SOURCE, KEY_TAG, comm_, &status);

    if (status.MPI_SOURCE == rank_) {
      MPI_Recv(nullptr, 0, MPI_CHAR, rank_, KEY_TAG, comm_, &status);
      break;
    }
    Element key = ReceiveKey(MPI_ANY_SOURCE, KEY_TAG, &status);
    QueryResult element =
        LocalGet(key.GetView(), hash_function_(key.Value(), key.Length()));
    SendValue(element, status.MPI_SOURCE, VALUE_TAG);
  }
}

void PaperlessKV::RespondPut() {
  while (true) {
    MPI_Status status;
    MPI_Probe(MPI_ANY_SOURCE, KEY_PUT_TAG, comm_, &status);
    int count = 10;
    MPI_Get_count(&status, MPI_CHAR, &count);

    if (status.MPI_SOURCE == rank_) {
      MPI_Recv(nullptr, 0, MPI_CHAR, rank_, KEY_PUT_TAG, comm_, &status);
      break;
    } else if (count == 0) {
      MPI_Recv(nullptr, 0, MPI_CHAR, status.MPI_SOURCE, KEY_PUT_TAG, comm_,
&status); std::lock_guard<std::mutex> lck(fence_mutex); fence_calls_received++;
      if (fence_calls_received == rank_size_ - 1) {
        fence_wait.notify_all();
      }
      continue;
    }
    if (dispatch_data_in_chunks_ && consistency_ != SEQUENTIAL) {

      // We also need to receive data in chunks.
      char *original_buff = static_cast<char *>(PAPERLESS::malloc(count));
      MPI_Recv(original_buff, count, MPI_CHAR, status.MPI_SOURCE, KEY_PUT_TAG,
comm_, &status); int pos = 0; char *buff = original_buff; while (pos < count) {
        unsigned int key_len = ReadIntFromBuff(buff);
        unsigned int value_len = ReadIntFromBuff(buff + 4);
        buff += 8;
        ElementView key = ElementView(buff, key_len);
        buff += key_len;
        Tomblement value = Tomblement::createFromBuffWithCopy(buff, value_len);
        buff += value_len;

        Hash hash = hash_function_(key.Value(), key.Length());
        Owner o = hash % rank_size_;
        // TODO Change interface and avoid copies.
        //  Even though we did avoid it here with a hack.
        local_.Put(key, std::move(value), hash, o);
        pos += 8 + key_len + value_len;
      }
      free(original_buff);

    } else {
      // TODO: change interfaces and avoid some copies#
      // Add Element&& interfaces.
      Element key = ReceiveKey(status.MPI_SOURCE, KEY_PUT_TAG, &status);
      Tomblement value =
          ReceiveTomblementSequential(status.MPI_SOURCE, VALUE_PUT_TAG,
&status); Hash hash = hash_function_(key.Value(), key.Length()); Owner o = hash
% rank_size_; local_.Put(key.GetView(), std::move(value), hash, o);
    }
  }
}

*/

////////////////////////////////////////////////////////////////////////////////
/////                                                                      /////
/////                          Operations                                  /////
/////                                                                      /////
////////////////////////////////////////////////////////////////////////////////

void PaperlessKV::Put(const ElementView &key, Tomblement &&value) {
  Hash hash = hash_function_(key.Value(), key.Length());
  Owner o = hash % rank_size_;
  if (o == rank_) {
    local_.Put(key, std::move(value), hash, rank_);
  } else {
    if (consistency_ == RELAXED) {
      remote_.Put(key, std::move(value), hash, rank_);
    } else if (consistency_ == SEQUENTIAL) {
      remoteOperator_.PutSequential(key, hash, value);
    }
  }
}

QueryResult PaperlessKV::Get(const ElementView &key) {
  Hash hash = hash_function_(key.Value(), key.Length());
  Owner o = hash % rank_size_;
  if (o == rank_) {
    return LocalGet(key, hash);
  } else {
    if (consistency_ == RELAXED) {
      return RemoteGetRelaxed(key, hash);
    } else {  // SEQUENTIAL
      QueryResult qr = remoteOperator_.Get(key, hash);
      if (mode_ == READONLY) {
        remote_cache_.put(key, hash, qr);
      }
      return qr;
    }
  }
}

void PaperlessKV::DeleteKey(const ElementView &key) {
  Put(key, Tomblement::getATombstone());
}

QueryResult PaperlessKV::LocalGet(const ElementView &key, Hash hash) {
  QueryResult el = local_.Get(key, hash, rank_);
  if (el != QueryStatus::NOT_FOUND) {
    return el;
  } else {
    std::optional<QueryResult> cache_result = local_cache_.get(key, hash);
    if (!cache_result.has_value()) {
      el = storage_manager_.readFromDisk(key);
      local_cache_.put(key, hash, el);
      return el;
    } else   {
      return std::move(cache_result.value());
    }
  }
}

std::pair<QueryStatus, size_t> PaperlessKV::LocalGet(const ElementView &key,
                                                     const ElementView &buff,
                                                     Hash hash) {

  std::pair<QueryStatus, size_t> el =
      local_.Get(key, buff.Value(), buff.Length(), hash, rank_);
  if(el.first != QueryStatus::NOT_FOUND) {
    return el;
  } else {
    std::pair<QueryStatus, size_t> cache_result =
        local_cache_.get(key, hash, buff);
    if (cache_result.first == QueryStatus::NOT_IN_CACHE) {
      el = storage_manager_.readFromDisk(key, buff);
      local_cache_.put(key, hash, Element(buff.Value(), el.second));
      return el;
    } else {
      return cache_result;
    }
  }
}

QueryResult PaperlessKV::RemoteGetRelaxed(const ElementView &key, Hash hash) {

  QueryResult el = remote_.Get(key, hash, rank_);
  if (el != NOT_FOUND) {
    return el;
  } else {
    std::optional<QueryResult> cache_result = remote_cache_.get(key, hash);
    if (!cache_result.has_value()) {
      el = remoteOperator_.Get(key, hash);
      remote_cache_.put(key, hash, el);
      return el;
    }else {
      return std::move(cache_result.value());
    }
  }
}

std::pair<QueryStatus, size_t> PaperlessKV::RemoteGetRelaxed(
    const ElementView &key, const ElementView &v_buff, Hash hash) {


  std::pair<QueryStatus, size_t> el =
      remote_.Get(key, v_buff.Value(), v_buff.Length(), hash, rank_);
  if(el.first != NOT_FOUND){
    return el;
  } else {
    std::pair<QueryStatus, size_t> cache_result =
        local_cache_.get(key, hash, v_buff);
    if (cache_result.first != QueryStatus::NOT_IN_CACHE) {
      return cache_result;

    } else {
      el = remoteOperator_.Get(key, v_buff, hash);
      if (el.first == QueryStatus::FOUND) {
        remote_cache_.put(key, hash, Element(v_buff.Value(), el.second));
      } else if (el.first == QueryStatus::DELETED ||
                 el.first == QueryStatus::NOT_FOUND) {
        remote_cache_.put(key, hash, el.first);
      }
      return el;
    }
  }
/*
 // Old Cache Order
  std::pair<QueryStatus, size_t> cache_result =
      local_cache_.get(key, hash, v_buff);
  if (cache_result.first == QueryStatus::NOT_IN_CACHE) {
    std::pair<QueryStatus, size_t> el =
        remote_.Get(key, v_buff.Value(), v_buff.Length(), hash, rank_);
    if (el.first == QueryStatus::NOT_FOUND) {
      el = remoteOperator_.Get(key, v_buff, hash);
    }
    if (el.first == QueryStatus::FOUND) {
      remote_cache_.put(key, hash, Element(v_buff.Value(), el.second));
    } else if (el.first == QueryStatus::DELETED ||
               el.first == QueryStatus::NOT_FOUND) {
      remote_cache_.put(key, hash, el.first);
    }
      } else {
    return cache_result;
  }
*/

}

void PaperlessKV::put(const char *key, size_t key_len, const char *value,
                      size_t value_len) {
  Put(ElementView(key, key_len), Tomblement(value, value_len));
}

QueryResult PaperlessKV::get(const char *key, size_t key_len) {
  return Get(ElementView(key, key_len));
}

std::pair<QueryStatus, size_t> PaperlessKV::get(const char *key_buff,
                                                size_t key_len,
                                                char *value_buff,
                                                size_t value_buff_len) {
  ElementView key(key_buff, key_len);
  ElementView buff(value_buff, value_buff_len);
  Hash hash = hash_function_(key_buff, key_len);
  Owner o = hash % rank_size_;
  if (o == rank_) {
    return LocalGet(key, buff, hash);
  } else {
    if (consistency_ == RELAXED) {
      return RemoteGetRelaxed(key, buff, hash);
    } else {  // SEQUENTIAL
      std::pair<QueryStatus, size_t> el = remoteOperator_.Get(key, buff, hash);
      if (mode_ == READONLY) {
        if (el.first == QueryStatus::FOUND) {
          remote_cache_.put(key, hash, Element(buff.Value(), el.second));
        } else if (el.first == QueryStatus::DELETED ||
                   el.first == QueryStatus::NOT_FOUND) {
          remote_cache_.put(key, hash, el.first);
        }
      }
      return el;
    }
  }
}

FutureQueryInfo PaperlessKV::IGet(const char *key_buff,
                                                 size_t key_len,
                                                 char *value_buff,
                                                 size_t value_buff_len) {
  ElementView key(key_buff, key_len);
  ElementView buff(value_buff, value_buff_len);
  Hash hash = hash_function_(key_buff, key_len);
  Owner o = hash % rank_size_;
  if (o == rank_) {
    return LocalGet(key, buff, hash);
  } else {
    if (consistency_ == RELAXED) {
      return remoteOperator_.IGet(key, buff, hash);
    } else {  // SEQUENTIAL
      return remoteOperator_.IGet(key, buff, hash);
    }
  }
}


void PaperlessKV::DeleteKey(const char *key, size_t key_len) {
  DeleteKey(ElementView(key, key_len));
}

void PaperlessKV::Fence() {
  remote_cache_.clear();
  remote_.Flush();
  remoteOperator_.InitSync();
  MPI_Barrier(comm_);
}

void PaperlessKV::FenceAndChangeOptions(PaperlessKV::Consistency_t c,
                                        Mode_t mode) {

  if (c != RELAXED) {
    remote_cache_.clear();
  }
  remote_.Flush();
  remoteOperator_.InitSync();
  consistency_ = c;
  mode_ = mode;
  MPI_Barrier(comm_);
}

void PaperlessKV::FenceAndCheckPoint() {
  remote_cache_.clear();
  remote_.Flush();
  remoteOperator_.InitSync();
  local_.Flush();
  MPI_Barrier(comm_);
}

