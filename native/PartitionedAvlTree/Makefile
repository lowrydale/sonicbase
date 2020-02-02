#
#  There exist several targets which are by default empty and which can be 
#  used for execution of your targets. These targets are usually executed 
#  before and after some main targets. They are: 
#
#     .build-pre:              called before 'build' target
#     .build-post:             called after 'build' target
#     .clean-pre:              called before 'clean' target
#     .clean-post:             called after 'clean' target
#     .clobber-pre:            called before 'clobber' target
#     .clobber-post:           called after 'clobber' target
#     .all-pre:                called before 'all' target
#     .all-post:               called after 'all' target
#     .help-pre:               called before 'help' target
#     .help-post:              called after 'help' target
#
#  Targets beginning with '.' are not intended to be called on their own.
#
#  Main targets can be executed directly, and they are:
#  
#     build                    build a specific configuration
#     clean                    remove built files from a configuration
#     clobber                  remove all built files
#     all                      build all configurations
#     help                     print help mesage
#  
#  Targets .build-impl, .clean-impl, .clobber-impl, .all-impl, and
#  .help-impl are implemented in nbproject/makefile-impl.mk.
#
ARCH      := INTEL
DEBUGGING := -DNDEBUG

#-Ilzo-2.10/include -Ilzo_mac/include -Ilzo_mac
ifeq ($(ARCH),INTEL)
#CC          := gcc
CC          := g++
CFLAGS      := -fPIC -O -O2 -O3 -pthread -std=c++0x  -I. -I./skiplistmap -I/usr/lib/jvm/java-8-openjdk-amd64/include/linux/ -I/usr/lib/jvm/java-8-openjdk-amd64/include/  -I/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/include -I/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/include/darwin
#CFLAGS	    := -Wall -g  -Izlib -I/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/include -I/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/include/darwin
LDFLAGS     := #-L
endif

#/usr/lib/x86_64-linux-gnu/libc.a

UNAME := $(shell uname)

TARGETS    := $(GC_HARNESS_TARGETS)

ifeq ($(UNAME), Linux)
TARGETS    += linux/SonicBase.so linux/SonicBase
OUT_DIR	:= linux
LDFLAGS := $(LDFLAGS) -z muldefs
endif

ifeq ($(UNAME), Darwin)
TARGETS    += mac/SonicBase.so mac/SonicBase
OUT_DIR	:= mac
endif

CFLAGS      += $(DEBUGGING)
COMMON_DEPS += Makefile $(wildcard *.h)


all: $(TARGETS)

clean:
	rm -f $(TARGETS) replay *~ core *.o *.a


csrc = $(wildcard lzo-2.10/src/*.c) \
 		$(wildcard ./*.c) \
       $(wildcard ./*.cpp) \
 		$(wildcard ./skiplistmap/*.c) \
       $(wildcard ./skiplistmap/*.cpp)
obj = $(csrc:.c=.o)

$(OUT_DIR)/SonicBase.so: $(obj)
	echo $(OUT_DIR)
	@mkdir -p $(@D)
	$(CC) -shared $(CFLAGS) -o $@ $^ $(LDFLAGS)

$(OUT_DIR)/SonicBase: $(obj)
	echo $(OUT_DIR)
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) -v
