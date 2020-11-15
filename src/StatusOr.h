//
// Created by matthias on 08.11.20.
//

#ifndef PAPERLESS_STATUSOR_H
#define PAPERLESS_STATUSOR_H

#include <new>

template <class T, class Status>
class StatusOr {
 public:
  StatusOr(const Status& s) { status_ = s; }

  StatusOr(Status&& s) {
    status_ = std::move(s);
    has_value_ = false;
  }

  template <typename U = T>
  StatusOr(U&& t) {
    build(std::forward<T>(t));
  }

  ~StatusOr() {
    if (hasValue()) Value().T::~T();
  }

  StatusOr(StatusOr<T, Status>&& t) {
    if(t.hasValue())
      build(std::move(t.value()));
  }

  StatusOr(const StatusOr<T, Status>& t) {
    if(t.hasValue() )
      build(t.value());
  }


  T& Value() & {
    return static_cast<T&>(*std::launder(reinterpret_cast<T*>(&storage_)));
  }
  T const& Value() const& {
    return static_cast<T const&>(
        *std::launder(reinterpret_cast<T const*>(&storage_)));
  }
  T&& Value() && noexcept {
    return static_cast<T&&>(*std::launder(reinterpret_cast<T*>(&storage_)));
  }
  T const&& Value() const&& {
    return static_cast<T const&&>(
        *std::launder(reinterpret_cast<T const*>(&storage_)));
  }

  bool hasValue() const { return has_value_; }

  explicit operator bool() /* cons */{ return hasValue(); }

  const T* operator->() const { return &Value(); }

  T* operator->() { return &Value(); }

  T const& operator*() const& { return Value(); }

  T& operator*() & { return Value(); }

  T&& operator*() && { return Value(); }


  bool operator==(const StatusOr<T, Status>& o) const {
    if(has_value_ && o.has_value_) {
      return Value() == o.value();
    } else if( !has_value_ && !o.has_value_) {
      return status_ == o.status_;
    }
    return false;
  }

  bool operator==(const Status& o) const {
    if(has_value_) {
      return false;
    }
    return status_== o;
  }

  bool operator!=(const Status& o) const {
    if(has_value_) {
      return true;
    }
    return status_!= o;
  }


private:
  template <typename... ArgsT>
  void build(ArgsT&&... Args) {
    new (reinterpret_cast<void*>(&storage_)) T(std::forward<ArgsT>(Args)...);
    has_value_ = true;
  }

  bool has_value_;
  std::aligned_storage_t<sizeof(T), alignof(T)> storage_;
  Status status_;
};

template <class T, class Status>
bool operator==(const StatusOr<T, Status>& lhs, const StatusOr<T, Status>& rhs) {

}

#endif  // PAPERLESS_STATUSOR_H
