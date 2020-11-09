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

  StatusOr(Status&& s) { status_ = std::move(s); }

  template <typename U = T>
  StatusOr(U&& t) {
    build(std::forward<T>(t));
  }

  ~StatusOr() {
    if (hasValue()) value().T::~T();
  }

  T& value() {
    return static_cast<T&>(*std::launder(reinterpret_cast<T*>(&storage_)));
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

  bool hasValue() { return has_value_; }

  explicit operator bool() const { return hasValue(); }

  const T* operator->() const { return &value(); }

  T* operator->() { return &value(); }

  T const& operator*() const& { return value(); }

  T& operator*() & { return value(); }

  T&& operator*() && { return value(); }

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

#endif  // PAPERLESS_STATUSOR_H
