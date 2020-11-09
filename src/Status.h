//
// Created by matthias on 08.11.20.
//

#ifndef PAPERLESS_STATUS_H
#define PAPERLESS_STATUS_H

#include "StatusOr.h"

enum QueryStatus {
  NOT_FOUND, DELETED
};

using QueryResult = StatusOr<Element, QueryStatus>;

#endif //PAPERLESS_STATUS_H
