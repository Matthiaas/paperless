//
// Created by matthias on 08.11.20.
//

#ifndef PAPERLESS_STATUS_H
#define PAPERLESS_STATUS_H

#include "Element.h"
#include "StatusOr.h"

enum QueryStatus {
  NOT_FOUND, FOUND, DELETED
};

using QueryResult
  = StatusOr<OwningElement, QueryStatus>;

#endif //PAPERLESS_STATUS_H
