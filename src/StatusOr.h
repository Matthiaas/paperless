//
// Created by matthias on 08.11.20.
//

#ifndef PAPERLESS_STATUSOR_H
#define PAPERLESS_STATUSOR_H

#include <new>

template <class T, class St>
class StatusOr {
 public:
  StatusOr(const St& s) {
    status_ = s;
    has_value_ = false;
  }

  StatusOr(St&& s) {
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

  StatusOr(StatusOr<T, St>&& t) {
    if(t.hasValue())
      build(std::move(t.Value()));
  }

  StatusOr(const StatusOr<T, St>& t) {
    if(t.hasValue() )
      build(t.Value());
  }


  T& Value() & {
    return static_cast<T&>(*std::launder(reinterpret_cast<T*>(&storage_)));
  }
  const T& Value() const& {
    return static_cast<T const&>(
        *std::launder(reinterpret_cast<const T*>(&storage_)));
  }
  T&& Value() && noexcept {
    return static_cast<T&&>(*std::launder(reinterpret_cast<T*>(&storage_)));
  }
  const T&& Value() const&& {
    return static_cast<const T&&>(
        *std::launder(reinterpret_cast<T const*>(&storage_)));
  }

  St& Status() & {
    return static_cast<St&>(status_);
  }

  const St&  Status() const& {
    return static_cast<const St&>(status_);
  }

  const T&& Status() const&& {
    return static_cast<St&&>(status_);
  }

  bool hasValue() const { return has_value_; }

  explicit operator bool()  const { return hasValue(); }

  const T* operator->() const { return &Value(); }

  T* operator->() { return &Value(); }

  T const& operator*() const& { return Value(); }

  T& operator*() & { return Value(); }

  T&& operator*() && { return Value(); }


  bool operator==(const StatusOr<T, St>& o) const {
    if(has_value_ && o.has_value_) {
      return Value() == o.Value();
    } else if( !has_value_ && !o.has_value_) {
      return status_ == o.status_;
    }
    return false;
  }

  bool operator!=(const StatusOr<T, St>& o) const {
    return !(*this == o);
  }

  bool operator==(const St& o) const {
    if(has_value_) {
      return false;
    }
    return status_== o;
  }

  bool operator!=(const St& o) const {
    if(has_value_) {
      return true;
    }
    return status_!= o;
  }

  bool operator==(const T& o) const {
    if(!has_value_) {
      return false;
    }
    return Value() == o;
  }

  bool operator!=(const T& o) const {
    if(!has_value_) {
      return true;
    }
    return Value() != o;
  }


private:
  template <typename... ArgsT>
  void build(ArgsT&&... Args) {
    new (reinterpret_cast<void*>(&storage_)) T(std::forward<ArgsT>(Args)...);
    has_value_ = true;
  }

  bool has_value_ = false;
  std::aligned_storage_t<sizeof(T), alignof(T)> storage_;
  St status_;
};

#endif  // PAPERLESS_STATUSOR_H
