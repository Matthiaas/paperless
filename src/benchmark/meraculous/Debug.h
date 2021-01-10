#ifndef PAPYRUS_KV_DEBUG_H
#define PAPYRUS_KV_DEBUG_H

#include <stdio.h>

//#define _TRACE_ENABLE
#define _CHECK_ENABLE
#define _DEBUG_ENABLE
#define _INFO_ENABLE
#define _ERROR_ENABLE
#define _TODO_ENABLE

#define _COLOR_DEBUG

#ifdef _COLOR_DEBUG
#define RED     "\033[22;31m"
#define GREEN   "\033[22;32m"
#define YELLOW  "\033[22;33m"
#define BLUE    "\033[22;34m"
#define PURPLE  "\033[22;35m"
#define CYAN    "\033[22;36m"
#define GRAY    "\033[22;37m"
#define _RED    "\033[22;41m"
#define _GREEN  "\033[22;42m"
#define _YELLOW "\033[22;43m"
#define _BLUE   "\033[22;44m"
#define _PURPLE "\033[22;45m"
#define _CYAN   "\033[22;46m"
#define _GRAY   "\033[22;47m"
#define RESET   "\e[m"
#else
#define RED
#define GREEN
#define YELLOW
#define BLUE
#define PURPLE
#define CYAN
#define GRAY
#define _RED
#define _GREEN
#define _YELLOW
#define _BLUE
#define _PURPLE
#define _CYAN
#define _GRAY
#define RESET
#endif

extern char nick_[];

#ifdef _TRACE_ENABLE
#define  _trace(fmt, ...) { printf( BLUE "[T] %s [%s:%d:%s] " fmt RESET "\n", nick_, __FILE__, __LINE__, __func__, __VA_ARGS__); fflush(stdout); }
#define __trace(fmt, ...) { printf(_BLUE "[T] %s [%s:%d:%s] " fmt RESET "\n", nick_, __FILE__, __LINE__, __func__, __VA_ARGS__); fflush(stdout); }
#else
#define  _trace(fmt, ...)
#define __trace(fmt, ...)
#endif

#ifdef _CHECK_ENABLE
#define  _check() { printf( PURPLE "[C] %s [%s:%d:%s]" RESET "\n", nick_, __FILE__, __LINE__, __func__); fflush(stdout); }
#define __check() { printf(_PURPLE "[C] %s [%s:%d:%s]" RESET "\n", nick_, __FILE__, __LINE__, __func__); fflush(stdout); }
#else
#define  _check()
#define __check()
#endif

#ifdef _DEBUG_ENABLE
#define  _debug(fmt, ...) { printf( CYAN "[D] %s [%s:%d:%s] " fmt RESET "\n", nick_, __FILE__, __LINE__, __func__, __VA_ARGS__); fflush(stdout); }
#define __debug(fmt, ...) { printf(_CYAN "[D] %s [%s:%d:%s] " fmt RESET "\n", nick_, __FILE__, __LINE__, __func__, __VA_ARGS__); fflush(stdout); }
#else
#define  _debug(fmt, ...)
#define __debug(fmt, ...)
#endif

#ifdef _INFO_ENABLE
#define  _info(fmt, ...) { printf( YELLOW "[I] %s [%s:%d:%s] " fmt RESET "\n", nick_, __FILE__, __LINE__, __func__, __VA_ARGS__); fflush(stdout); }
#define __info(fmt, ...) { printf(_YELLOW "[I] %s [%s:%d:%s] " fmt RESET "\n", nick_, __FILE__, __LINE__, __func__, __VA_ARGS__); fflush(stdout); }
#else
#define  _info(fmt, ...)
#define __info(fmt, ...)
#endif

#ifdef _ERROR_ENABLE
#define  _error(fmt, ...) { printf( RED "[E] %s [%s:%d:%s] " fmt RESET "\n", nick_, __FILE__, __LINE__, __func__, __VA_ARGS__); fflush(stdout); }
#define __error(fmt, ...) { printf(_RED "[E] %s [%s:%d:%s] " fmt RESET "\n", nick_, __FILE__, __LINE__, __func__, __VA_ARGS__); fflush(stdout); }
#else
#define  _error(fmt, ...)
#define __error(fmt, ...)
#endif

#ifdef _TODO_ENABLE
#define  _todo(fmt, ...) { printf( GREEN "[TODO] %s [%s:%d:%s] " fmt RESET "\n", nick_, __FILE__, __LINE__, __func__, __VA_ARGS__); fflush(stdout); }
#define __todo(fmt, ...) { printf(_GREEN "[TODO] %s [%s:%d:%s] " fmt RESET "\n", nick_, __FILE__, __LINE__, __func__, __VA_ARGS__); fflush(stdout); }
#else
#define  _todo(fmt, ...)
#define __todo(fmt, ...)
#endif

#endif /* PAPYRUS_KV_DEBUG_H */
