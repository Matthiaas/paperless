//
// Created by matthias on 08.11.20.
//

#ifndef PAPERLESS_TYPES_H
#define PAPERLESS_TYPES_H


#include <cstdint>
#include <functional>

using Owner = int;
using Hash = uint64_t;
using HashFunction = std::function<Hash(const char*,const size_t)>;

#endif //PAPERLESS_TYPES_H
