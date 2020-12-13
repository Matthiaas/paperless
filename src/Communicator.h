//
// Created by matthias on 13.12.20.
//

#ifndef PAPERLESS_COMMUNICATOR_H
#define PAPERLESS_COMMUNICATOR_H

#include <mpi.h>

#include "Element.h"
#include "Status.h"
class Communicator {

 private:
  MPI_Comm comm_;
};

#endif  // PAPERLESS_COMMUNICATOR_H
