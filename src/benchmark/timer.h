//https://code.ornl.gov/eck/papyrus/-/blob/master/kv/apps/sc17/timer.h
#ifndef PAPYRUS_KV_APPS_SC17_TIMER_H
#define PAPYRUS_KV_APPS_SC17_TIMER_H

#include <sys/time.h>

static double _t[128];

static double _wtime() {
  static double base_sec = -1;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  if (base_sec < 0) base_sec = tv.tv_sec + 1.e-6 * tv.tv_usec;
  return tv.tv_sec + 1.e-6 * tv.tv_usec - base_sec;
}

static double _w(int i) {
  _t[i] = _wtime();
  return _t[i];
}

static double _ww(int i, int j) {
  return _t[j] - _t[i];
}

#endif /* PAPYRUS_KV_APPS_SC17_TIMER_H */
