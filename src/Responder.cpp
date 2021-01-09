#include "Responder.h"

#include <iostream>

Responder::Responder(PaperlessKV* kv, MPI_Comm comm,
                     bool dispatch_data_in_chunks)
    : kv_(kv), comm_(comm), dispatch_data_in_chunks_(dispatch_data_in_chunks) {
  MPI_Comm_size(comm_, &rank_size_);
  big_buffer_ = static_cast<char*>(PAPERLESS::malloc(MAX_ELEMENT_LEN));
}

Responder::~Responder() { free(big_buffer_); }

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
      HandlePut(m, status.MPI_SOURCE, true);
      return true;
    case Message::Type::PUT_REQUEST_NO_RESPOND:
      HandlePut(m, status.MPI_SOURCE, false);
      return true;
    case Message::Type::GET_REQUEST:
      HandleGet(m, status.MPI_SOURCE);
      return true;
    case Message::Type::SYNC:
      HandleSync(m, status.MPI_SOURCE);
      return true;
    case Message::Type::KILL:
      return false;
  }
  throw "This is an error";
}

void Responder::HandlePut(const Message& msg, int src, bool respond) {
  Element key(msg.GetKeyLen());
  Tomblement value(msg.GetValueLen() - 1);
  MPI_Recv(key.Value(), msg.GetKeyLen(), MPI_CHAR, src, msg.GetTag(), comm_,
            MPI_STATUS_IGNORE);
  MPI_Recv(value.GetBuffer(), msg.GetValueLen(), MPI_CHAR, src, msg.GetTag(),
            comm_, MPI_STATUS_IGNORE);
  Owner o = msg.GetHash() % rank_size_;
  kv_->local_.Put(std::move(key), std::move(value), msg.GetHash(), o);
  if(respond) {
    MPI_Send(nullptr, 0, MPI_CHAR, src, msg.GetTag(), comm_);
  }
}

void Responder::HandleGet(const Message& msg, int src) {
  MPI_Recv(big_buffer_, msg.GetKeyLen(), MPI_CHAR, src, msg.GetTag(), comm_,
           MPI_STATUS_IGNORE);
  std::pair<QueryStatus, size_t> res =
      kv_->get(big_buffer_, msg.GetKeyLen(), big_buffer_, MAX_ELEMENT_LEN);
  SendQueryResult(res, src, msg.GetTag());
}

void Responder::HandleSync(const Message& msg, int src) {
  // Send Ack.
  MPI_Send(nullptr, 0, MPI_CHAR, src, msg.GetTag(), comm_);
}
