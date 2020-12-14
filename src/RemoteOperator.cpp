//
// Created by matthias on 14.12.20.
//

#include "RemoteOperator.h"
#include "Message.h"

RemoteOperator::RemoteOperator(MPI_Comm comm, bool dispatch_data_in_chunks)
    : comm_(comm), dispatch_data_in_chunks_(dispatch_data_in_chunks),
      get_value_tagger(PAPERLESS_MSG_TAG + 1, PAPERLESS_MSG_TAG + 100000000) {
  MPI_Comm_rank(comm_, &rank_);
  MPI_Comm_size(comm_, &rank_size_);
}


QueryResult RemoteOperator::Get(const ElementView &key, Hash hash) {
  Owner o = hash % rank_size_;
  int tag = getTag();
  Message m;
  GetMessage* get_msg = m.ToGetMessage();
  get_msg->tag = tag;
  get_msg->key_len = key.Length();
  get_msg->hash = hash;
  m.SendMessage(o, PAPERLESS_MSG_TAG, comm_);

  m = Message::ReceiveMessage(o ,tag, comm_, MPI_STATUS_IGNORE);
  QueryResultMessage* qm = m.ToQueryResultMessage();
   if(qm->queryStatus == QueryStatus::FOUND) {
    Element res(qm->value_len);
    MPI_Recv(res.Value(), qm->value_len, MPI_CHAR,o, tag, comm_, MPI_STATUS_IGNORE);
    return res;
  } else {
    return static_cast<QueryStatus >(qm->queryStatus);
  }
}

std::pair<QueryStatus, size_t> RemoteOperator::Get(
    const ElementView &key, const ElementView &v_buff, Hash hash) {
  Owner o = hash % rank_size_;
  int tag = getTag();
  Message m;
  GetMessage* get_msg = m.ToGetMessage();
  get_msg->tag = tag;
  get_msg->key_len = key.Length();
  get_msg->hash = hash;
  m.SendMessage(o, PAPERLESS_MSG_TAG, comm_);

  m = Message::ReceiveMessage(o ,tag, comm_, MPI_STATUS_IGNORE);
  QueryResultMessage* qm = m.ToQueryResultMessage();
  if(qm->value_len > v_buff.Length()) {
    return {QueryStatus::BUFFER_TOO_SMALL, qm->value_len};
  } else if(qm->queryStatus == QueryStatus::FOUND) {
    MPI_Recv(v_buff.Value(), qm->value_len, MPI_CHAR,o, tag, comm_, MPI_STATUS_IGNORE);
    return {QueryStatus::FOUND,  qm->value_len};
  } else {
    return {static_cast<QueryStatus >(qm->queryStatus), 0};
  }
}


void RemoteOperator::PutSequential(const ElementView &key, Hash hash, const Tomblement &value) {
  Message m;
  int tag = getTag();
  Owner o = hash % rank_size_;
  PutMessage* put_msg = m.ToPutMessage();
  put_msg->tag = tag;
  put_msg->key_len = key.Length();
  put_msg->hash = hash;
  put_msg->value_len = value.GetBufferLen();
  m.SendMessage(o,PAPERLESS_MSG_TAG, comm_);
  MPI_Send(key.Value(), key.Length(), MPI_CHAR, o, tag, comm_);
  MPI_Send(value.GetBuffer(), value.GetBufferLen(), MPI_CHAR, o, tag, comm_);
}

void RemoteOperator::InitSync() {
  MPI_Barrier(comm_);
  for (int i = 0; i < rank_size_; i++) {
    if (i == rank_) continue;
    Message m;
    m.type = Message::Type::SYNC;
    m.SendMessage(i, PAPERLESS_MSG_TAG, comm_);
  }
}
int RemoteOperator::getTag() {
  return get_value_tagger.getNextTag();
}

