#
#  Makefile to generate the openifs wrapper

VERSION = 1.00
TARGET  = openifs_$(VERSION)_x86_64-pc-linux-gnu
SRC     = openifs.cpp

CC       = g++
CFLAGS   = -g -pthread -std=c++17 -Wall
INCLUDES = -I../boinc-install/include
LIBS     = -L../boinc-install/lib -lboinc_api -lboinc_zip -lboinc


all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(SRC) $(CFLAGS) $(INCLUDES) $(LIBS) -o $(TARGET)
                
clean:
	$(RM) *.o $(TARGET)
