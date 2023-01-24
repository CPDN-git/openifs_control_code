#
#  Makefile to generate the openifs wrapper.
#
#  Creates both production & debug versions.
#
#       Glenn 

VERSION = 1.08
TARGET  = openifs_$(VERSION)_x86_64-pc-linux-gnu
DEBUG   = openifs_$(VERSION)_x86_64-pc-linux-gnu-debug
SRC     = openifs.cpp

CC       = g++
CFLAGS   = -g -static -pthread -std=c++17 -Wall
# Address Sanitizer (ASan) can't use -static
CDEBUG   = -fsanitize=address -ggdb3 -pthread -std=c++17 -Wall
INCLUDES    = -I../boinc-install/include
LIBDIR    = -L../boinc-install/lib
LIBS      = -lboinc_api -lboinc_zip -lboinc


all: $(TARGET) $(DEBUG)

$(TARGET): $(SRC)
	$(CC) $(SRC) $(CFLAGS) $(INCLUDES) $(LIBDIR) $(LIBS) -o $(TARGET)

$(DEBUG): $(SRC)
	$(CC) $(SRC) $(CDEBUG) $(INCLUDES) $(LIBDIR) $(LIBS) -o $(DEBUG)

clean:
	$(RM) *.o $(TARGET) $(DEBUG)
