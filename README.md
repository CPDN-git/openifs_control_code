# Control code for ECMWF OpenIFS (40r1 and 43r3) running in climateprediction.net (CPDN)

This respository contains the instructions and code for building the controlling application used for controlling the ECMWF OpenIFS code in the climateprediction.net project.

To compile the controlling code you will need to download and build the BOINC code (this is available from: https://github.com/BOINC/boinc). For instructions on building this code see: https://boinc.berkeley.edu/trac/wiki/CompileClient.

As only the libraries are required, the boinc client and manager can be disabled (reduces system packages required).

In short:
    git clone https://github.com/BOINC/boinc.git
    cd boinc
    ./_autosetup
    ./configure --disable-server --disable-fcgi --disable-manager --disable-client  \
                --enable-libraries --enable-boinczip  \
                --prefix=../boinc-install             \
                CXXFLAGS='-O3'
    make install

This installs the boinc libraries and include files into the parent directory to the git source (change the --prefix argument to install into a different directory). It's preferable not to install into the same directory as the source. To specify the location of the include files, use the -I argument on the compile line, to specify the location of the libraries, use the -L argument.


To compile the controller code on a Linux machine:

First ensure that libzip is installed using (on an Ubuntu machine): sudo apt-get install libzip-dev

Then we need to obtain the RapidXml header for parsing XML files. This is downloaded from the site: http://rapidxml.sourceforge.net/
We only need the file: 'rapidxml.hpp'. Download this file and put in the same folder as openifs.cpp.

    g++ openifs.cpp -I../boinc/include -L../boinc/lib  -lboinc_api -lboinc -lboinc_zip -static -pthread -std=c++11 -o oifs_43r3_1.00_x86_64-pc-linux-gnu

(assuming boinc libraries are located in ../boinc relative to this repository)

And to build on an ARM architecture machine:

    g++ openifs.cpp -D_ARM -I../boinc/include -L../boinc/lib -lboinc_api -lboinc -lboinc_zip -static -pthread -lstdc++ -lm -std=c++11 -o oifs_43r3_1.00_aarch64-poky-linux

To compile the controller code on a Mac machine:

First ensure libzip is installed: brew install libzip

And that we have obtained the RapidXml header.

Build the BOINC libraries using Xcode. Then build the controller code:

    clang++ openifs.cpp -I../boinc/include -L../boinc/lib  -lboinc_api -lboinc -lboinc_zip -pthread -std=c++11 -o oifs_43r3_1.00_x86_64-apple-darwin

This will create an executable that is the app imported into the BOINC environment alongside the OpenIFS executable. Now to run this the OpenIFS ancillary files along with the OpenIFS executable will need to be alongside, the command to run this in standalone mode is (40r1):

    ./oifs_43r3_1.00_x86_64-pc-linux-gnu 2000010100 gw3a 0001 1 00001 1 oifs_43r3 1

Or for macOS:

    ./oifs_43r3_1.00_x86_64-apple-darwin 2000010100 gw3a 0001 1 00001 1 oifs_43r3 1

The command line parameters: [0] compiled executable, [1] start date in YYYYMMDDHH format, [2] experiment id, [3] unique member id, [4] batch id, [5] workunit id, [6] FCLEN, [7] app name, [8]  nthreads, [9] app version id.

Note, [9] is only used in standalone mode.

The current version of OpenIFS this supports is: oifs40r1 and oifs43r3. The OpenIFS code is compiled separately and is installed alongside the OpenIFS controller in BOINC. To upgrade the controller code in the future to later versions of OpenIFS consideration will need to be made whether there are any changes to the command line parameters the compiled version of OpenIFS takes in, and whether there are changes to the structure and content of the supporting ancillary files.

Currently in the controller code the following variables are fixed (this will change with further development):

OIFS_DUMMY_ACTION=abort    : Action to take if a dummy (blank) subroutine is entered (quiet/verbose/abort)

OMP_SCHEDULE=STATIC        : OpenMP thread scheduling to use. STATIC usually gives the best performance.

OMP_STACKSIZE=128M         : Set OpenMP stack size per thread. Default is usually too low for OpenIFS.

OMP_NUM_THREADS=1          : Number of threads (cores). Defaults to 1, can be changed by argument.

OIFS_RUN=1                 : Run number

DR_HOOK=1                  : DrHook is OpenIFS's tracing facility. Set to '1' to enable.

DR_HOOK_HEAPCHECK=no       : Enable/disable DrHook heap checking. Usually 'no' unless debugging.

DR_HOOK_STACKCHECK=no      : Enable/disable DrHook stack checks. Usually 'no' unless debugging.

DR_HOOK_NOT_MPI=true       : If set true, DrHook will not make calls to MPI (OpenIFS does not use MPI in CPDN).

EC_MEMINFO=0               : Disable EC_MEMINFO messages in stdout.

NAMELIST=fort.4            : NAMELIST file
