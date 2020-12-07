#ifndef PAPERLESS_STATUS_H
#define PAPERLESS_STATUS_H

#include "Element.h"
#include "StatusOr.h"

enum QueryStatus {
  UNKNOWN, NOT_FOUND, FOUND, DELETED, BUFFER_TOO_SMALL, NOT_IN_CACHE
};

using QueryResult
  = StatusOr<Element, QueryStatus>;

#endif //PAPERLESS_STATUS_H
