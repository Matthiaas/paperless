#include "Responder.h"



Responder::Responder(PaperlessKV *kv, bool dispatch_data_in_chunks)
    : kv_(kv), dispatch_data_in_chunks_(dispatch_data_in_chunks) {

}


void Responder::SendQueryResult(
    const std::pair<QueryStatus, size_t>& val, int target, int tag) {

  Message m;
  QueryResultMessage* qm = m.ToQueryResultMessage();
  qm->queryStatus = val.first;
  qm->value_len = val.second;
  m.SendMessage(target, tag, comm_);
  if (val.first == QueryStatus::FOUND) {
    MPI_Send(big_buffer_, val.second, MPI_CHAR, target, tag, comm_);
  }
}

bool Responder::Respond() {
  MPI_Status status;
  Message m = Message::ReceiveMessage(
      MPI_ANY_SOURCE, PAPERLESS_MSG_TAG, comm_, &status);
  switch (m.type) {
    case Message::Type::PUT_REQUEST:
      HandlePut(m.ToPutMessage(), status.MPI_SOURCE);
      return true;
    case Message::Type::GET_REQUEST:
      HandleGet(m.ToGetMessage(), status.MPI_SOURCE);
      return true;
    case Message::Type::SYNC:
      HandleSync();
      return true;
    case Message::Type::KILL:
      return false;
  }
  throw "This is an error";
}

void Responder::HandlePut(PutMessage* msg, int src) {
  if(!dispatch_data_in_chunks_) {
    Element key(msg->key_len);
    Tomblement value(msg->value_len - 1);

    MPI_Recv(key.Value(), msg->key_len, MPI_CHAR, src, msg->tag, comm_, MPI_STATUS_IGNORE);
    MPI_Recv(key.Value(), msg->key_len, MPI_CHAR, src, msg->tag, comm_, MPI_STATUS_IGNORE);
    Owner o = msg->hash % rank_size_;
    kv_->local_.Put(std::move(key), std::move(value), msg->hash, o);
  }
}

void Responder::HandleGet(GetMessage* msg, int src) {
  MPI_Recv(big_buffer_, msg->key_len, MPI_CHAR, src, msg->tag, comm_, MPI_STATUS_IGNORE);
  std::pair<QueryStatus, size_t>  res =
      kv_->get(big_buffer_, msg->key_len, big_buffer_, MAX_ELEMENT_LEN);
  SendQueryResult(res, src, msg->tag);
}

void Responder::HandleSync() {
  std::lock_guard<std::mutex> lck(fence_mutex);
  fence_calls_received++;
  if (fence_calls_received == rank_size_ - 1) {
    fence_wait.notify_all();
  }
}

void Responder::WaitForSync() {
  std::unique_lock<std::mutex> lck(fence_mutex);
  if (fence_calls_received != rank_size_ - 1) {
    fence_wait.wait(lck);
  }
  fence_calls_received = 0;
}

