//
// Created by matthias on 16.12.20.
//

#ifndef PAPERLESS_FUTUREQUERYINFO_H
#define PAPERLESS_FUTUREQUERYINFO_H

#include "Message.h"
#include "Status.h"
#include <mpi.h>

class FutureQueryInfo {
 public:
  FutureQueryInfo(size_t tag, ElementView key, Hash hash) :
      send_msg_(Message::GET_REQUEST),
      key_(key.Value(), key.Length())  {
    requests_ = static_cast<MPI_Request*>(malloc(4 * sizeof (MPI_Request)));
    has_value_ = false;

    send_msg_.SetTag(tag);
    send_msg_.SetKeyLen(key.Length());
    send_msg_.SetHash(hash);
  }
  FutureQueryInfo(std::pair<QueryStatus, size_t> result) : key_(nullptr, 0) {
    has_value_ = true;
    result_ = result;
    requests_ = nullptr;
  }

  FutureQueryInfo(const FutureQueryInfo&) = delete;
  FutureQueryInfo& operator=(const FutureQueryInfo&) = delete;

  FutureQueryInfo(FutureQueryInfo&& other) :
      requests_(other.requests_),
      status_(other.status_),
      msg_(std::move(other.msg_)),
      send_msg_(std::move(other.send_msg_)),
      key_(std::move(other.key_)),
      has_value_(other.has_value_),
    result_(std::move(other.result_)){
    other.requests_ = nullptr;
  }
  FutureQueryInfo& operator=(FutureQueryInfo&& other) {
    free(requests_);
    requests_ = other.requests_;
    other.requests_ = nullptr;
    status_ = other.status_;
    msg_ = std::move(other.msg_);
    send_msg_ = std::move(other.send_msg_);
    key_ = std::move(other.key_);
    has_value_ = other.has_value_;
    result_ = other.result_;


    return *this;
  }

  ~FutureQueryInfo() {
    Destruct();
  }

  bool Test() {
    if(has_value_) {
      return true;
    } else {
      MPI_Testall(3, requests_, &has_value_, MPI_STATUSES_IGNORE);
      if(has_value_ && msg_.GetQueryStatus() == QueryStatus::FOUND) {
        MPI_Test(requests_ + 3, &has_value_, MPI_STATUSES_IGNORE);
      } else {
        MPI_Request_free(requests_+3);
      }
      return has_value_;
    }
  }

  std::pair<QueryStatus, size_t> Get() {
    if(!has_value_) {
      MPI_Waitall(3, requests_, MPI_STATUSES_IGNORE);

      if(static_cast<QueryStatus>(msg_.GetQueryStatus())  == QueryStatus::FOUND) {
        MPI_Wait(&requests_[3], MPI_STATUSES_IGNORE);
      }else {
        MPI_Request_free(requests_+3);
      }
      has_value_ = true;
      result_ = {static_cast<QueryStatus>(msg_.GetQueryStatus()),
                 msg_.GetValueLen()};
    }
    return result_;
  }

 private:
  // 0 is MSG Send
  // 1 is KEY Send
  // 3 is MSG Recv
  // 4 is VALUE Recv
  MPI_Request* requests_;
  MPI_Status status_;
  Message msg_;
  Message send_msg_;

  Element key_;
  int has_value_;
  std::pair<QueryStatus, size_t> result_;

  void InitRequest(Owner o, const ElementView &v_buff, MPI_Comm comm) {
    send_msg_.I_SendMessage(o, PAPERLESS_MSG_TAG, comm, &requests_[0]);
    MPI_Isend(key_.Value(), key_.Length(), MPI_CHAR, o, send_msg_.GetTag(), comm, &requests_[1]);
    msg_.I_ReceiveMessage(o, send_msg_.GetTag(), comm, &requests_[2]);
    MPI_Irecv(v_buff.Value(), v_buff.Length(), MPI_CHAR, o, send_msg_.GetTag(), comm, &requests_[3]);
  }

  void Destruct() {
    free(requests_);
  }

  friend class RemoteOperator;
};


#endif  // PAPERLESS_FUTUREQUERYINFO_H
