add_library(smhasher STATIC MurmurHash3.cpp MurmurHash3.h)
set_property(TARGET smhasher PROPERTY POSITION_INDEPENDENT_CODE ON)

