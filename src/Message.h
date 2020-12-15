#ifndef PAPERLESS_MESSAGE_H
#define PAPERLESS_MESSAGE_H

#include <mpi.h>

#include "Common.h"
#include "Types.h"

#define PAPERLESS_MSG_TAG 0
#define MAX_ELEMENT_LEN (1l << 20)

/*

struct PutMessage {
  PutMessage() = delete;
  size_t type;
  size_t tag;
  size_t hash;
  size_t key_len;
  size_t value_len;

};

struct GetMessage {
  GetMessage() = delete;
  size_t type;
  size_t tag;
  size_t hash;
  size_t key_len;
};

struct QueryResultMessage {
  size_t queryStatus;
  size_t tag;
  size_t value_len;
};

*/

struct Message {
  enum Type {
    PUT_REQUEST = 0,
    GET_REQUEST = 1,
    SYNC = 3,
    KILL = 4,
    QUERY_RESULT = 5
  };

  Message(Type t) {
    buff_ = static_cast<size_t*>(std::malloc(buff_len_ * sizeof(size_t)));
    buff_[0] = t;
  }

  Message(const Message&) = delete;
  Message(Message&& m) {
    buff_ = m.buff_;
    m.buff_ = nullptr;
  }

  ~Message() { free(buff_); }

  static Message ReceiveMessage(int src, int tag, MPI_Comm comm,
                                MPI_Status* status) {
    Message m;
    MPI_Recv(m.buff_, buff_len_, MPI_UNSIGNED_LONG, src, tag, comm, status);
    return m;
  }
  void SendMessage(int dest, int tg, MPI_Comm comm) {
    MPI_Send(buff_, buff_len_, MPI_UNSIGNED_LONG, dest, tg, comm);
  }

  void SetTag(size_t t) { buff_[1] = t; }

  void SetKeyLen(size_t t) { buff_[2] = t; }

  void SetQueryStatus(size_t t) { buff_[2] = t; }

  void SetValueLen(size_t t) { buff_[3] = t; }

  void SetHash(size_t t) { buff_[4] = t; }

  size_t GetType() const { return buff_[0]; }

  size_t GetTag() const { return buff_[1]; }

  size_t GetKeyLen() const { return buff_[2]; }

  size_t GetQueryStatus() const { return buff_[2]; }

  size_t GetValueLen() const { return buff_[3]; }

  size_t GetHash() const { return buff_[4]; }

 private:
  Message() {
    buff_ = static_cast<size_t*>(PAPERLESS::malloc(buff_len_ * sizeof(size_t)));
  }
  static constexpr size_t buff_len_ = 10;
  size_t* buff_ = nullptr;
};

#endif  // PAPERLESS_MESSAGE_H
