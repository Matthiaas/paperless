//
// Created by fooris on 17.12.20.
//

#ifndef PAPERLESS_SRC_BENCHMARK_HELPER_H_
#define PAPERLESS_SRC_BENCHMARK_HELPER_H_

static void rand_str(size_t len, char* str) {
  if (len == 0) return;
  char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  int l = (int) (sizeof(charset) -1);
  for (size_t i = 0; i < len - 1; i++) {
    int key = rand() % l;
    str[i] = charset[key];
  }
  str[len - 1] = 0;
}

#endif  // PAPERLESS_SRC_BENCHMARK_HELPER_H_
