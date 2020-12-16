//
// Created by matthias on 16.12.20.
//

#ifndef PAPERLESS_FUTUREQUERYINFO_H
#define PAPERLESS_FUTUREQUERYINFO_H

#include "Message.h"
#include "Status.h"

class FutureQueryInfo {
 public:
  FutureQueryInfo() {
    has_value_ = false;
    requests_ = static_cast<MPI_Request*>(malloc(4 * sizeof (MPI_Request)));
  }
  FutureQueryInfo(std::pair<QueryStatus, size_t> result) {
    has_value_ = true;
    result_ = result;
    requests_ = nullptr;
  }

  FutureQueryInfo(const FutureQueryInfo&) = delete;
  FutureQueryInfo& operator=(const FutureQueryInfo&) = delete;

  FutureQueryInfo(FutureQueryInfo&& other) {
    requests_ = other.requests_;
    other.requests_ = nullptr;
    status = other.status;
    msg_ = std::move(other.msg_);
    result_ = other.result_;
    has_value_ = other.has_value_;
  }
  FutureQueryInfo& operator=(FutureQueryInfo&& other) {
    requests_ = other.requests_;
    other.requests_ = nullptr;
    status = other.status;
    msg_ = std::move(other.msg_);
    result_ = other.result_;
    has_value_ = other.has_value_;
    return *this;
  }

  ~FutureQueryInfo() {
    Destruct();
  }

  bool Test() {
    if(has_value_) {
      return true;
    } else {
      MPI_Testall(4, requests_, &has_value_, MPI_STATUSES_IGNORE);
      return has_value_;
    }
  }

  std::pair<QueryStatus, size_t> Get() {
    if(!has_value_) {
      MPI_Waitall(4, requests_, MPI_STATUSES_IGNORE);
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
  MPI_Status status;
  Message msg_;
  int has_value_;
  std::pair<QueryStatus, size_t> result_;

  void Destruct() {
    free(requests_);
  }

  friend class RemoteOperator;
};


#endif  // PAPERLESS_FUTUREQUERYINFO_H
