//
// Created by matthias on 14.12.20.
//

#include "RemoteOperator.h"

#include "FutureQueryInfo.h"
#include "Message.h"

#include <iostream>

RemoteOperator::RemoteOperator(MPI_Comm comm, bool dispatch_data_in_chunks)
    : comm_(comm), dispatch_data_in_chunks_(dispatch_data_in_chunks) {
  MPI_Comm_rank(comm_, &rank_);
  MPI_Comm_size(comm_, &rank_size_);

  min_get_key = (rank_ + rank_size_ )*10000 + 1;
  max_get_key = ((rank_ + rank_size_) + 1) * 10000;
}

Message RemoteOperator::InitGet(const ElementView &key, Hash hash) {
  Owner o = hash % rank_size_;
  int tag = getTag();
  Message m(Message::GET_REQUEST);
  m.SetTag(tag);
  m.SetKeyLen(key.Length());
  m.SetHash(hash);
  m.SendMessage(o, PAPERLESS_MSG_TAG, comm_);
  MPI_Send(key.Value(), key.Length(), MPI_CHAR, o, tag, comm_);
  return Message::ReceiveMessage(o, tag, comm_, MPI_STATUS_IGNORE);
}

// this is 10% slower than the other init.
Message RemoteOperator::InitGetAsync(const ElementView &key, Hash hash) {
  Owner o = hash % rank_size_;
  int tag = getTag();
  Message m(Message::GET_REQUEST);
  m.SetTag(tag);
  m.SetKeyLen(key.Length());
  m.SetHash(hash);

  MPI_Request requests[3];
  m.I_SendMessage(o, PAPERLESS_MSG_TAG, comm_, &requests[0]);
  MPI_Isend(key.Value(), key.Length(), MPI_CHAR, o, tag, comm_, &requests[1]);
  Message res;
  res.I_ReceiveMessage(o, tag, comm_, &requests[2]);
  MPI_Waitall(3, requests, MPI_STATUSES_IGNORE);
  return res;
}


QueryResult RemoteOperator::Get(const ElementView &key, Hash hash) {
  Owner o = hash % rank_size_;
  Message m2 = InitGet(key, hash);
  if (m2.GetQueryStatus() == QueryStatus::FOUND) {
    Element res(m2.GetValueLen());
    MPI_Recv(res.Value(), res.Length(), MPI_CHAR, o, m2.GetTag(), comm_,
             MPI_STATUS_IGNORE);
    return res;
  } else {
    return static_cast<QueryStatus>(m2.GetQueryStatus());
  }
}

FutureQueryInfo RemoteOperator::IGet(const ElementView &key,
                                                   const ElementView &v_buff,
                                                   Hash hash) {

  Owner o = hash % rank_size_;
  int tag = getTag();
  FutureQueryInfo res(tag, key, hash);
  res.InitRequest(o, v_buff, comm_);
  return res;
}

std::pair<QueryStatus, size_t> RemoteOperator::Get(const ElementView &key,
                                                   const ElementView &v_buff,
                                                   Hash hash) {

  Owner o = hash % rank_size_;
  Message m2 = InitGet(key, hash);
  if (m2.GetValueLen() > v_buff.Length() &&
      m2.GetQueryStatus() == QueryStatus::FOUND) {
    // TODO: Discard MPI message instead:
    Element res(m2.GetValueLen());
    MPI_Recv(res.Value(), res.Length(), MPI_CHAR, o, m2.GetTag(), comm_,
             MPI_STATUS_IGNORE);
    return {QueryStatus::BUFFER_TOO_SMALL, m2.GetValueLen()};
  } else if (static_cast<QueryStatus>(m2.GetQueryStatus()) == QueryStatus::FOUND) {
    MPI_Recv(v_buff.Value(), m2.GetValueLen(), MPI_CHAR, o, m2.GetTag(), comm_,
             MPI_STATUS_IGNORE);
    return {QueryStatus::FOUND, m2.GetValueLen()};
  } else {
    return {static_cast<QueryStatus>(m2.GetQueryStatus()), 0};
  }
}


void RemoteOperator::Put(const ElementView &key, Hash hash,
                         const Tomblement &value, Message::Type t) {
  Message m(t);
  int tag = getTag();
  Owner o = hash % rank_size_;
  m.SetTag(tag);
  m.SetKeyLen(key.Length());
  m.SetValueLen(value.GetBufferLen());
  m.SetHash(hash);
  m.SendMessage(o, PAPERLESS_MSG_TAG, comm_);
  MPI_Send(key.Value(), key.Length(), MPI_CHAR, o, tag, comm_);
  MPI_Send(value.GetBuffer(), value.GetBufferLen(), MPI_CHAR, o, tag, comm_);
  if( t == Message::PUT_REQUEST) {
    MPI_Recv(nullptr, 0, MPI_CHAR, o, tag, comm_, MPI_STATUS_IGNORE);
  }
}
void RemoteOperator::PutRelaxed(const ElementView &key, Hash hash,
                                const Tomblement &value) {
  Put(key, hash, value, Message::PUT_REQUEST_NO_RESPOND);
}


void RemoteOperator::PutSequential(const ElementView &key, Hash hash,
                                   const Tomblement &value) {
  Put(key, hash, value, Message::PUT_REQUEST);
}




Message RemoteOperator::IPut(const ElementView &key, Hash hash,
                                    const Tomblement &value, MPI_Request *rqs) {
  Message m(Message::PUT_REQUEST_NO_RESPOND);
  int tag = getTag();
  Owner o = hash % rank_size_;
  m.SetTag(tag);
  m.SetKeyLen(key.Length());
  m.SetValueLen(value.GetBufferLen());
  m.SetHash(hash);
  m.I_SendMessage(o, PAPERLESS_MSG_TAG, comm_, &rqs[0]);
  MPI_Isend(key.Value(), key.Length(), MPI_CHAR, o, tag, comm_, &rqs[1]);
  MPI_Isend(value.GetBuffer(), value.GetBufferLen(), MPI_CHAR, o, tag, comm_, &rqs[2]);
  return m;
}


void RemoteOperator::InitSync() {
  MPI_Barrier(comm_);
  int tag = getTag();
  Message m(Message::SYNC);
  MPI_Request* request = static_cast<MPI_Request*>(PAPERLESS::malloc((rank_size_-1) * 2 * sizeof(MPI_Request)));
  int pos = 0;
  for (int i = 0; i < rank_size_; i++) {
    if (i == rank_) continue;
    m.SetTag(tag);
    m.I_SendMessage(i, PAPERLESS_MSG_TAG, comm_, &request[pos]);
    MPI_Irecv(nullptr, 0, MPI_CHAR, i, tag, comm_, &request[pos+1]);
    pos += 2;
  }
  MPI_Waitall((rank_size_-1)*2, request, MPI_STATUSES_IGNORE);
  free(request);
}

int RemoteOperator::getTag() {
  return min_get_key + (cur_get_key++ % (max_get_key - min_get_key));
}

void RemoteOperator::Kill() {
  Message m(Message::KILL);
  m.SendMessage(rank_, PAPERLESS_MSG_TAG, comm_);
}

