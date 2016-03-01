.SUFFIXES:
.SUFFIXES: .c .h .o

PKG_CFLAGS := $(shell pkg-config mercury --cflags)
PKG_LDLIBS := $(shell pkg-config mercury --libs)

USE_GOOGLEPROF ?= no
USE_TCMALLOC   ?= no

ifeq ($(USE_GOOGLEPROF),yes)
PKG_CFLAGS += $(shell pkg-config libprofiler --cflags)
PKG_LDLIBS += $(shell pkg-config libprofiler --libs)
endif

ifeq ($(USE_TCMALLOC),yes)
PKG_CFLAGS += $(shell pkg-config libtcmalloc --cflags)
PKG_LDLIBS += $(shell pkg-config libtcmalloc --libs)
endif

USE_DUMMY_PTHREAD ?= no

DUMMY_PTHREAD :=
ifeq ($(USE_DUMMY_PTHREAD),yes)
DUMMY_PTHREAD := pthread-noop.o
endif

override CFLAGS += -Wall -Wextra -std=gnu99 $(PKG_CFLAGS)
override LDLIBS += $(PKG_LDLIBS)

EXES := hg-ctest1 hg-ctest2 hg-ctest3 hg-ctest4

UTILS := hg-ctest-util.o
HEADERS := hg-ctest-util.h

all: $(EXES)

$(EXES): $(UTILS) $(DUMMY_PTHREAD) $(HEADERS)

hg-ctest-util.o: hg-ctest-util.h

clean:
	rm -f $(EXES) $(UTILS)
