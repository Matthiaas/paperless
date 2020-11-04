//
// Created by matthias on 04.11.20.
//

#ifndef PAPERLESS_CIRCULARNONBLOCKINGQUEUE_H
#define PAPERLESS_CIRCULARNONBLOCKINGQUEUE_H


#include <cstddef>

template <typename T>
class CircularNonBlockingQueue {
public:
  CircularNonBlockingQueue(size_t size);

  void push_back(const T&);
  void push_back(T&&);
  const T& front();
  void pop_front();
  bool empty();

};


#endif //PAPERLESS_CIRCULARNONBLOCKINGQUEUE_H
