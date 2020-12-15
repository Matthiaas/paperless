//
// Created by matthias on 16.12.20.
//

#ifndef PAPERLESS_FUTUREQUERYRESULT_H
#define PAPERLESS_FUTUREQUERYRESULT_H



class FutureQueryResult {
 public:
  FutureQueryResult() {
    has_value_ = false;
  }
  FutureQueryResult(std::pair<QueryStatus, size_t> result) {
    has_value_ = true;
    result_ = result;
  }
 private:
  std::pair<QueryStatus, size_t> result_;
  bool has_value_;
};


#endif  // PAPERLESS_FUTUREQUERYRESULT_H
