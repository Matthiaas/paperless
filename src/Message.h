#ifndef PAPERLESS_MESSAGE_H
#define PAPERLESS_MESSAGE_H

#include <mpi.h>
#include "Types.h"

#define PAPERLESS_MSG_TAG 0
#define MAX_ELEMENT_LEN (1l<<20)

struct PutMessage {
  PutMessage() = delete;
  int type;
  int tag;
  Hash hash;
  size_t key_len;
  size_t value_len;

};

struct GetMessage {
  GetMessage() = delete;
  int type;
  int tag;
  Hash hash;
  size_t key_len;
};

struct QueryResultMessage {
  char queryStatus;
  size_t value_len;
};

struct Message {
  enum Type {
    PUT_REQUEST = 0, GET_REQUEST = 1, SYNC = 2, KILL = 3
  };
  int type;
  int tag;
  size_t buff[10];
  static Message ReceiveMessage(int src, int tag, MPI_Comm comm, MPI_Status* status) {
    Message m;
    MPI_Recv(&m, sizeof(m) *sizeof(char), MPI_CHAR, src, tag, comm, status);
    return m;
  }
  void SendMessage(int dest, int tag, MPI_Comm comm) {
    Message &m = *this;
    MPI_Send(&m, sizeof(m) *sizeof(char), MPI_CHAR, dest, tag, comm);
  }
  PutMessage* ToPutMessage() {
    type = Message::Type::PUT_REQUEST;
    return reinterpret_cast<PutMessage*>(this);
  }
  GetMessage* ToGetMessage() {
    type = Message::Type::GET_REQUEST;
    return reinterpret_cast<GetMessage*>(this);
  }
  QueryResultMessage* ToQueryResultMessage() {
    type = Message::Type::GET_REQUEST;
    return reinterpret_cast<QueryResultMessage*>(this);
  }
};


#endif  // PAPERLESS_MESSAGE_H
