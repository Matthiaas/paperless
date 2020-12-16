#include "Responder.h"

Responder::Responder(PaperlessKV* kv, MPI_Comm comm,
                     bool dispatch_data_in_chunks)
    : kv_(kv), comm_(comm), dispatch_data_in_chunks_(dispatch_data_in_chunks) {
  MPI_Comm_size(comm_, &rank_size_);
  big_buffer_ = static_cast<char*>(PAPERLESS::malloc(10000));
}

Responder::~Responder() { free(big_buffer_); }


size_t getMsgLenA(int src, int tag, MPI_Comm comm) {
  MPI_Status status;
  MPI_Probe(src, tag, comm, &status);
  int count;
  MPI_Get_count(&status, MPI_CHAR, &count);
  return count;
}

void Responder::SendQueryResult(const std::pair<QueryStatus, size_t>& val,
                                int target, int tag) {
  Message m(Message::QUERY_RESULT);
  m.SetTag(tag);
  m.SetQueryStatus(val.first);
  m.SetValueLen(val.second);
  m.SendMessage(target, tag, comm_);
  if (val.first == QueryStatus::FOUND) {
    MPI_Send(big_buffer_, val.second, MPI_CHAR, target, tag, comm_);
  }
}

bool Responder::Respond() {
  MPI_Status status;
  Message m = Message::ReceiveMessage(MPI_ANY_SOURCE, PAPERLESS_MSG_TAG, comm_,
                                      &status);
  switch (m.GetType()) {
    case Message::Type::PUT_REQUEST:
      HandlePut(m, status.MPI_SOURCE);
      return true;
    case Message::Type::GET_REQUEST:
      HandleGet(m, status.MPI_SOURCE);
      return true;
    case Message::Type::SYNC:
      HandleSync();
      return true;
    case Message::Type::KILL:
      return false;
  }
  throw "This is an error";
}

void Responder::HandlePut(const Message& msg, int src) {
  if (!dispatch_data_in_chunks_) {
    Element key(msg.GetKeyLen());
    Tomblement value(msg.GetValueLen() - 1);

    if(getMsgLenA(src, msg.GetTag(), comm_) != msg.GetKeyLen()) {
      std::cerr << "This is an error1" << std::endl << std::flush;
    }
    MPI_Recv(key.Value(), msg.GetKeyLen(), MPI_CHAR, src, msg.GetTag(), comm_,
             MPI_STATUS_IGNORE);
    if(getMsgLenA(src, msg.GetTag(), comm_) != msg.GetValueLen()) {
      std::cerr << "This is an error2" << std::endl << std::flush;
    }
    MPI_Recv(value.GetBuffer(), msg.GetValueLen(), MPI_CHAR, src, msg.GetTag(),
             comm_, MPI_STATUS_IGNORE);
    Owner o = msg.GetHash() % rank_size_;
    kv_->local_.Put(std::move(key), std::move(value), msg.GetHash(), o);
  }
}

void Responder::HandleGet(const Message& msg, int src) {
  if(getMsgLenA(src, msg.GetTag(), comm_) != msg.GetKeyLen()) {
    std::cerr << "This is an error3" << std::endl << std::flush;
  }
  MPI_Recv(big_buffer_, msg.GetKeyLen(), MPI_CHAR, src, msg.GetTag(), comm_,
           MPI_STATUS_IGNORE);
  std::pair<QueryStatus, size_t> res =
      kv_->get(big_buffer_, msg.GetKeyLen(), big_buffer_, MAX_ELEMENT_LEN);
  SendQueryResult(res, src, msg.GetTag());
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
