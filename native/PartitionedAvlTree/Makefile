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

ifeq ($(ARCH),INTEL)
#CC          := gcc
CC          := g++
CFLAGS      := -fPIC -std=c++0x -I/opt/jdk1.8.0_141/include/linux/ -I/opt/jdk1.8.0_161/include/ -I/Users/lowryda/Dropbox/git/sonicbase/db/src/main/java/com/sonicbase/index -I/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/include -I/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/include/darwin
#CFLAGS	    := -Wall -g  -I/Users/lowryda/Dropbox/git/sonicbase/db/src/main/java/com/sonicbase/index -I/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/include -I/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/include/darwin
LDFLAGS     := -Wl#-L /Users/lowryda/tools/boost_1_66_0/bin.v2/libs/system/build/darwin-gnu-4.2.1/release/link-static/threadapi-pthread/threading-multi #-lpthread
endif

UNAME := $(shell uname)

TARGETS    := $(GC_HARNESS_TARGETS)

ifeq ($(UNAME), Linux)
TARGETS    += linux/SonicBase.so
OUT_DIR	:= linux
endif

ifeq ($(UNAME), Darwin)
TARGETS    += mac/SonicBase.so
OUT_DIR	:= mac
endif

CFLAGS      += $(DEBUGGING)
COMMON_DEPS += Makefile $(wildcard *.h)


all: $(TARGETS)

clean:
	rm -f $(TARGETS) replay *~ core *.o *.a

$(OUT_DIR)/SonicBase.so: PartitionedAvlTree.cpp
	echo $(OUT_DIR)
	@mkdir -p $(@D)
	$(CC) -shared $(CFLAGS) PartitionedAvlTree.cpp -o $(OUT_DIR)/SonicBase.so
