.SUFFIXES: .cpp .o

CC=g++

SRCDIR=src/
INC=include/
LIBS=lib/

# SRCS:=$(wildcard src/*.cpp)
# OBJS:=$(SRCS:.cpp=.o)

# main source file
TARGET_SRC:=$(SRCDIR)main.cpp
TARGET_OBJ:=$(SRCDIR)main.o
STATIC_LIB:=$(LIBS)libbpt.a

# Include more files if you write another source file.
SRCS_FOR_LIB:= \
	$(SRCDIR)db.cpp \
	$(SRCDIR)index_layer.cpp \
	$(SRCDIR)buffer_layer.cpp \
	$(SRCDIR)disk_space_layer.cpp \
	$(SRCDIR)lock_manager.cpp \
	$(SRCDIR)trx_manager.cpp

OBJS_FOR_LIB:=$(SRCS_FOR_LIB:.cpp=.o)

CFLAGS+= -g -fPIC -I $(INC)

TARGET=main

all: $(TARGET)

$(TARGET): $(TARGET_OBJ) $(STATIC_LIB)
	$(CC) $(CFLAGS) $< -o $@ -L $(LIBS) -lbpt -lpthread

%.o: %.cpp
	$(CC) $(CFLAGS) $^ -c -o $@ -lpthread

clean:
	rm $(TARGET) $(TARGET_OBJ) $(OBJS_FOR_LIB) $(LIBS)*

$(STATIC_LIB): $(OBJS_FOR_LIB)
	ar cr $@ $^