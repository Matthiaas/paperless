#include "wtimer.h"

double _ta[128];
double _t0[128];

double _wtime() {
    static double base_sec = -1;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    if (base_sec < 0) base_sec = tv.tv_sec + 1.e-6 * tv.tv_usec;
    return tv.tv_sec + 1.e-6 * tv.tv_usec - base_sec;
}

void _wi() {
    int i = 0;
    for (i = 0; i < 128; i++) {
        _ta[i] = 0.0;
    }
}

void _w0(int i) {
    _t0[i] = _wtime();
}

void _w1(int i) {
    _ta[i] += _wtime() - _t0[i];
}

double _w(int i) {
    return _ta[i];
}

