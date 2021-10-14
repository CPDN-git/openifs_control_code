# ECMWF OpenIFS BOINC control code

This respository contains the instructions and code for building the controlling application used for controlling the ECMWF OpenIFS code in the climateprediction.net project.

To compile the controlling code you will need to download and build the BOINC code (this is available from: https://github.com/BOINC/boinc). For instructions on building this code see: https://boinc.berkeley.edu/trac/wiki/CompileClient. This code needs to be in the same directory as the OpenIFS controller code.

To compile the controller code on a Linux machine:

First ensure that libzip is installed using (on an Ubuntu machine): sudo apt-get install libzip-dev

g++ openifs.cpp -I./boinc -I./boinc/lib -L./boinc/api -L./boinc/lib -L./boinc/zip -lboinc_api -lboinc -lboinc_zip -static -pthread -std=c++11 -o openifs_0.1_x86_64-pc-linux-gnu

To compile the controller code on a Mac machine:

First ensure libzip is installed: brew install libzip

Build the BOINC libraries using Xcode. Then build the controller code:

clang++ openifs.cpp -I./boinc -I./boinc/lib -L./boinc/api -L./boinc/lib -L./boinc/zip -lboinc_api -lboinc -lboinc_zip -pthread -std=c++11 -o openifs_0.1_x86_64-apple-darwin

This will create an executable that is the app imported into the BOINC environment alongside the OpenIFS executable. Now to run this the OpenIFS ancillary files along with the OpenIFS executable will need to be alongside, the command to run this in standalone mode is (40r1):

./openifs_0.1_x86_64-pc-linux-gnu 2000010100 gw3a 0001 1 00001 1 openifs 1.1

Or for macOS:

./openifs_0.1_x86_64-apple-darwin 2000010100 gw3a 0001 1 00001 1 openifs 1.1

The command line parameters: [1] compiled executable, [2] start date YYYYMMDDHH, [3] experiment id, [4] unique member id, [5] batch id, [6] workunit id, [7] FCLEN, [8] app name, [9]  app version id.

The current version of OpenIFS this supports is: oifs40r1. The OpenIFS code is compiled separately and is installed alongside the OpenIFS controller in BOINC. To upgrade the controller code in the future to later versions of OpenIFS consideration will need to be made whether there are any changes to the command line parameters the compiled version of OpenIFS takes in, and whether there are changes to the structure and content of the supporting ancillary files.

Currently in the controller code the following variables are fixed (this will change with further development):

OIFS_DUMMY_ACTION=abort    : Action to take if a dummy (blank) subroutine is entered (quiet/verbose/abort)

OMP_NUM_THREADS=1          : Number of OpenMP threads to use.

OMP_SCHEDULE=STATIC        : OpenMP thread scheduling to use. STATIC usually gives the best performance.

DR_HOOK=1                  : DrHook is OpenIFS's tracing facility. Set to '1' to enable.

DR_HOOK_HEAPCHECK=no       : Enable/disable DrHook heap checking. Usually 'no' unless debugging.

DR_HOOK_STACKCHECK=no      : Enable/disable DrHook stack checks. Usually 'no' unless debugging.

DR_HOOK_NOT_MPI=true       : If set true, DrHook will not make calls to MPI (OpenIFS does not use MPI in CPDN).

OMP_STACKSIZE=128M         : Set OpenMP stack size per thread. Default is usually too low for OpenIFS.

NTHREADS=1                 : Default number of OPENMP threads.

OIFS_RUN=1                 : Run number

NAMELIST=fort.4            : NAMELIST file
