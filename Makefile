#
#  Makefile to generate the openifs wrapper
#
#    A standalone version of the control code links against
#    boinc client libraries with 'standalone' set in api/boinc_api.cpp
#
#       Glenn 

VERSION = 1.08
TARGET  = openifs_$(VERSION)_x86_64-pc-linux-gnu
STANDALONE = openifs_$(VERSION)_x86_64-pc-linux-gnu-standalone
SRC     = openifs.cpp

CC       = g++
CFLAGS   = -g -static -pthread -std=c++17 -Wall
CDEBUG   = -ggdb3
INCLUDES    = -I../boinc-install/include
INCLUDES_ST = -I../boinc-install-standalone/include
LIBDIR    = -L../boinc-install/lib
LIBDIR_ST = -L../boinc-install-standalone/lib
LIBS      = -lboinc_api -lboinc_zip -lboinc


all: $(TARGET) $(STANDALONE)

$(TARGET): $(SRC)
	$(CC) $(SRC) $(CFLAGS) $(INCLUDES) $(LIBDIR) $(LIBS) -o $(TARGET)

$(STANDALONE): $(SRC)
	$(CC) $(SRC) $(CFLAGS) $(CDEBUG) $(INCLUDES_ST) $(LIBDIR_ST) $(LIBS) -o $(STANDALONE)
                
clean:
	$(RM) *.o $(TARGET) $(STANDALONE)
