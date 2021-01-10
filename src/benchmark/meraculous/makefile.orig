#######################
# general configuration

# kmer length for all datasets used in the benchmark
KMER_LENGTH = 51

# base CFLAGS for UPC
CFLAGS = -DNDEBUG -O 
#CFLAGS = -O0 -g

# base CXXFLAGS
CXXFLAGS = -DNDEBUG -O3 -std=c++11 -Wshadow -Wreturn-type
#CXXFLAGS = -O0 -std=c++11 -Wshadow -Wreturn-type -g

# C++ compiler
#  note: must support c++11
CXX = CC

# UPC compiler type
#  determines which compiler-specific configuration is used below
# included out of the box: Berkeley UPC (bupc), Cray UPC (cray)
UPC_COMPILER = bupc

################################
# UPC compiler-specific settings

ifeq ($(UPC_COMPILER), bupc)
  $(info Using Berkeley UPC)
  UPC_TYPE=-DUSE_BUPC -shared-heap=1GB
  UPCC=upcc
  # autodetect if upcc can use the more reliable -cupc2c translation option
  CAN_USE_UPC2C := $(shell $(UPCC) -cupc2c hello_world.c -o hello_world >/dev/null 2>/dev/null ; echo $$?)
  ifeq ($(CAN_USE_UPC2C), 0)
    $(info using -cupc2c to translate UPC code)
    USE_UPC2C = -cupc2c
  endif
  UPC_CXX_LINK=-link-with=$(CXX) -lstdc++
endif

ifeq ($(UPC_COMPILER), cray)
  $(info Using CRAY UPC)
  UPC_TYPE=-DUSE_CRAY_UPC
  UPCC=cc -h upc
endif

## >> add additional UPC compiler configs here << ##

###################################
# checks, derived CPP defines, etc.

ifeq ($(UPCC),)
  $(error Unrecognized UPC compiler: $(UPC_COMPILER))
endif

OBJECTS = Kmer.o KmerIterator.o hash.o KmerMiddle.o

KMER_PACKED_LENGTH = $(shell echo $$((($(KMER_LENGTH)+3)/4)))

MAX_KMER_SIZE = $(shell echo $$((32*(($(KMER_LENGTH)+31)/32))))

KMERFLAG = -DMAX_KMER_SIZE=$(MAX_KMER_SIZE) -DKMERlength=$(KMER_LENGTH)

CXXFLAGS += ${KMERFLAG}

all : meraculous-$(KMER_LENGTH)

test-mode : meraculous-$(KMER_LENGTH)-testing

Kmer.o: Kmer.hpp Kmer.cpp

KmerIterator.o: KmerIterator.hpp KmerIterator.cpp

hash.o: hash.hpp hash.cpp

KmerMiddle.o: KmerMiddle.hpp KmerMiddle.cpp

INCLUDES = meraculous.h kmer_hash.h kmer_handling.h readufx.h buildUFXhashBinary.h UU_traversal_final.h packingDNAseq.h

PROFILE = -DPROFILE -DIO_TIME_PROFILE

meraculous-%-testing : meraculous.c readufx.o $(OBJECTS) $(INCLUDES)
	$(UPCC) $(USE_UPC2C) $(UPC_OPTS) $(UPC_TYPE) meraculous.c \
		-DMERACULOUS \
		-DSYNC_PROTOCOL \
		$(PROFILE) \
		-DKMER_LENGTH=$(KMER_LENGTH) \
		-DKMER_PACKED_LENGTH=$(KMER_PACKED_LENGTH) \
		$(CFLAGS) \
		readufx.o ${OBJECTS} \
		$(UPC_CXX_LINK) \
		-o $@

meraculous-% : meraculous.c readufx.o $(OBJECTS) $(INCLUDES)
	$(UPCC) $(USE_UPC2C) $(UPC_OPTS) $(UPC_TYPE) meraculous.c \
		-DMERACULOUS \
		-DDISABLE_SAVE_RESULTS \
		-DSYNC_PROTOCOL \
		$(PROFILE) \
		-DKMER_LENGTH=$(KMER_LENGTH) \
		-DKMER_PACKED_LENGTH=$(KMER_PACKED_LENGTH) \
		$(CFLAGS) \
		readufx.o ${OBJECTS} \
		$(UPC_CXX_LINK) \
		-o $@

.PHONY : clean
clean :
	rm -f *.o
	rm -f benchmark-constr benchmark-trav
	rm -rf meraculous-[1-9]* hello_world
