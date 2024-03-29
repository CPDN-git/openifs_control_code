//
// Control code for the OpenIFS application in the climateprediction.net project
//
// Written by Andy Bowery (Oxford eResearch Centre, Oxford University) November 2022
// Contributions from Glenn Carver (ex-ECMWF), 2022->
//

#include <string>
#include <chrono>
#include <thread>
#include <fstream>
#include <sstream>
#include <iostream>
#include <filesystem>  // required by file_is_empty
#include <exception>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <dirent.h> 
#include <regex.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include "boinc/boinc_api.h"
#include "boinc/boinc_zip.h"
#include "boinc/util.h"
#include "rapidxml.hpp"
#include <algorithm>

int check_child_status(long, int);
int check_boinc_status(long, int);
long launch_process(const std::string, const char*, const char*, const std::string);
std::string get_tag(const std::string &str);
void process_trickle(double, const std::string, const std::string, const std::string, int);
bool file_exists(const std::string &str);
bool file_is_empty(std::string &str);
double cpu_time(long);
double model_frac_done(double, double, int);
std::string get_second_part(const std::string, const std::string);
bool check_stoi(std::string& cin);
bool oifs_parse_stat(std::string&, std::string&, int);
bool oifs_get_stat(std::ifstream&, std::string&);
bool oifs_valid_step(std::string&,int);
int  print_last_lines(std::string filename, int nlines);

using namespace std;
using namespace std::chrono;
using namespace std::this_thread;
using namespace rapidxml;

int main(int argc, char** argv) {
    std::string ifsdata_file, ic_ancil_file, climate_data_file, horiz_resolution, vert_resolution, grid_type;
    std::string project_path, wu_name, version, tmpstr1, tmpstr2, tmpstr3;
    std::string ifs_line="", iter="0", ifs_word="", second_part, upload_file_name, last_line="";
    std::string upfile(""), resolved_name, upload_file, result_base_name;
    int upload_interval, timestep_interval, ICM_file_interval, retval=0, i, j;
    int process_status=1, restart_interval, current_iter=0, count=0, trickle_upload_count;
    char *pathvar=NULL;
    long handleProcess;
    double tv_sec, tv_usec, fraction_done, current_cpu_time=0, total_nsteps = 0;
    struct dirent *dir;
    struct rusage usage;
    regex_t regex;
    DIR *dirp=NULL;
    ZipFileList zfl;
    std::ifstream ifs_stat_file;
	

    // Set defaults for input arguments
    std::string OIFS_EXPID;           // model experiment id, must match string in filenames
    std::string namelist="fort.4";    // namelist file, this name is fixed

    // Initialise BOINC
    boinc_init();
    boinc_parse_init_data_file();

    // Get BOINC user preferences
    APP_INIT_DATA dataBOINC;
    boinc_get_init_data(dataBOINC);

    // Set BOINC optional values
    BOINC_OPTIONS options;
    boinc_options_defaults(options);
    options.main_program = true;
    options.multi_process = true;
    options.check_heartbeat = true;
    options.handle_process_control = true;  // the control code will handle all suspend/quit/resume
    options.direct_process_action = false;  // the control won't get suspended/killed by BOINC
    options.send_status_msgs = false;

    retval = boinc_init_options(&options);
    if (retval) {
       cerr << "..BOINC init options failed" << '\n';
       return retval;
    }

    cerr << "(argv0) " << argv[0] << '\n';
    cerr << "(argv1) start_date: " << argv[1] << '\n';
    cerr << "(argv2) exptid: " << argv[2] << '\n';
    cerr << "(argv3) unique_member_id: " << argv[3] << '\n';
    cerr << "(argv4) batchid: " << argv[4] << '\n';
    cerr << "(argv5) wuid: " << argv[5] << '\n';
    cerr << "(argv6) fclen: " << argv[6] << '\n';
    cerr << "(argv7) app_name: " << argv[7] << '\n';
    cerr << "(argv8) nthreads: " << argv[8] << std::endl;

    // Read the exptid, batchid, version, wuid from the command line
    std::string start_date = argv[1]; // simulation start date
    std::string exptid = argv[2];     // OpenIFS experiment id
    std::string unique_member_id = argv[3];  // umid
    std::string batchid = argv[4];    // batch id
    std::string wuid = argv[5];       // workunit id
    std::string fclen = argv[6];      // number of simulation days
    std::string app_name = argv[7];   // CPDN app name
    std::string nthreads = argv[8];   // number of OPENMP threads
	
    OIFS_EXPID = exptid;
    wu_name = dataBOINC.wu_name;

    double num_days = atof(fclen.c_str()); // number of simulation days
    int num_days_trunc = (int) num_days; // number of simulation days truncated to an integer
	
    // Get the slots path (the current working path)
    std::string slot_path = std::filesystem::current_path();
    if (slot_path.empty()) {
      cerr << "..current_path() returned empty" << std::endl;
    }
    else {
      cerr << "Working directory is: "<< slot_path << '\n';      
    }

    if (!boinc_is_standalone()) {

      // Get the project path
      project_path = dataBOINC.project_dir + std::string("/");
      cerr << "Project directory is: " << project_path << '\n';
	    
      // Get the app version and re-parse to add a dot
      version = std::to_string(dataBOINC.app_version);
      if (version.length()==2) {
         version = version.insert(0,".");
         //cerr << "version: " << version << '\n';
      }
      else if (version.length()==3) {
         version = version.insert(1,".");
         //cerr << "version: " << version << '\n';
      }
      else if (version.length()==4) {
         version = version.insert(2,".");
         //cerr << "version: " << version << '\n';
      }
      else {
         cerr << "..Error with the length of app_version, length is: " << version.length() << '\n';
         return 1;
      }
	    
      cerr << "app name: " << app_name << '\n';
      cerr << "version: " << version << '\n';
    }
    // Running in standalone
    else {
      cerr << "Running in standalone mode" << '\n';
      // Set the project path
      project_path = slot_path + std::string("/../projects/");
      cerr << "Project directory is: " << project_path << '\n';
	    
      // In standalone get the app version from the command line
      version = argv[9];
      cerr << "app name: " << app_name << '\n'; 
      cerr << "(argv9) app_version: " << argv[9] << '\n'; 
    }

    boinc_begin_critical_section();

    // Create temporary folder for moving the results to and uploading the results from
    // BOINC measures the disk usage on the slots directory so we must move all results out of this folder
    std::string temp_path = project_path + app_name + std::string("_") + wuid;
    cerr << "Location of temp folder: " << temp_path << '\n';
    if (mkdir(temp_path.c_str(),S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH) != 0) cerr << "..mkdir for temp folder for results failed" << std::endl;

    // macOS
    #if defined (__APPLE__)
       std::string app_file = app_name + std::string("_app_") + version + std::string("_x86_64-apple-darwin.zip");
    // ARM
    #elif defined (_ARM) 
       std::string app_file = app_name + std::string("_app_") + version + std::string("_aarch64-poky-linux.zip");
    // Linux
    #else
       std::string app_file = app_name + std::string("_app_") + version + std::string("_x86_64-pc-linux-gnu.zip");
    #endif

    // Copy the app file to the working directory
    std::string app_source = project_path + app_file;
    std::string app_destination = slot_path + std::string("/") + app_file;
    cerr << "Copying: " << app_source << " to: " << app_destination << '\n';
    retval = boinc_copy(app_source.c_str(), app_destination.c_str());
    if (retval) {
       cerr << "..Copying the app file to the working directory failed: error " << retval << std::endl;
       return retval;
    }

    // Unzip the app zip file
    std::string app_zip = slot_path + std::string("/") + app_file;
    cerr << "Unzipping the app zip file: " << app_zip << '\n';
    retval = boinc_zip(UNZIP_IT, app_zip.c_str(), slot_path);

    if (retval) {
       cerr << "..Unzipping the app file failed" << std::endl;
       return retval;
    }
    // Remove the zip file
    else {
       std::remove(app_zip.c_str());       
    }

	
    // Process the Namelist/workunit file:
    std::string namelist_zip = slot_path + std::string("/") + app_name + std::string("_") + unique_member_id + std::string("_") + start_date +\
                      std::string("_") + std::to_string(num_days_trunc) + std::string("_") + batchid + std::string("_") + wuid + std::string(".zip");
		
    // Get the name of the 'jf_' filename from a link within the namelist file
    std::string wu_source = get_tag(namelist_zip);

    // Copy the namelist files to the working directory
    std::string wu_destination = namelist_zip;
    cerr << "Copying the namelist files from: " << wu_source << " to: " << wu_destination << '\n';
    retval = boinc_copy(wu_source.c_str(), wu_destination.c_str());
    if (retval) {
       cerr << "..Copying the namelist files to the working directory failed" << std::endl;
       return retval;
    }

    // Unzip the namelist zip file
    cerr << "Unzipping the namelist zip file: " << namelist_zip << '\n';
    retval = boinc_zip(UNZIP_IT, namelist_zip.c_str(), slot_path);
    if (retval) {
       cerr << "..Unzipping the namelist file failed" << std::endl;
       return retval;
    }
    // Remove the zip file
    else {
       std::remove(namelist_zip.c_str());
    }

	
    // Parse the fort.4 namelist for the filenames and variables
    std::string namelist_file = slot_path + std::string("/") + namelist;
    std::string namelist_line="", delimiter="=";
    std::ifstream namelist_filestream;

   // Check for the existence of the namelist
   if( !file_exists(namelist_file) ) {
      cerr << "..The namelist file does not exist: " << namelist_file << std::endl;
      return 1;        // should terminate, the model won't run.
    }

    // Open the namelist file
    if(!(namelist_filestream.is_open())) {
       namelist_filestream.open(namelist_file);
    }

    // Read the namelist file
    while(std::getline(namelist_filestream, namelist_line)) { //get 1 row as a string
       std::istringstream nss(namelist_line);   //put line into stringstream

       if (nss.str().find("IFSDATA_FILE") != std::string::npos) {
          ifsdata_file = nss.str().substr(nss.str().find(delimiter)+1, nss.str().length()-1);
          // Remove any whitespace
          ifsdata_file.erase(std::remove(ifsdata_file.begin(), ifsdata_file.end(), ' '), ifsdata_file.end());
          cerr << "ifsdata_file: " << ifsdata_file << '\n';
       }
       else if (nss.str().find("IC_ANCIL_FILE") != std::string::npos) {
          ic_ancil_file = nss.str().substr(nss.str().find(delimiter)+1, nss.str().length()-1);
          // Remove any whitespace
          ic_ancil_file.erase(std::remove(ic_ancil_file.begin(), ic_ancil_file.end(), ' '), ic_ancil_file.end());
          cerr << "ic_ancil_file: " << ic_ancil_file << '\n'; 
       }
       else if (nss.str().find("CLIMATE_DATA_FILE") != std::string::npos) {
          climate_data_file = nss.str().substr(nss.str().find(delimiter)+1, nss.str().length()-1);
          // Remove any whitespace
          climate_data_file.erase(std::remove(climate_data_file.begin(),climate_data_file.end(),' '), climate_data_file.end());
          cerr << "climate_data_file: " << climate_data_file << '\n';
       }
       else if (nss.str().find("HORIZ_RESOLUTION") != std::string::npos) {
          horiz_resolution = nss.str().substr(nss.str().find(delimiter)+1, nss.str().length()-1);
          // Remove any whitespace
          horiz_resolution.erase(std::remove(horiz_resolution.begin(),horiz_resolution.end(),' '), horiz_resolution.end());
          cerr << "horiz_resolution: " << horiz_resolution << '\n';
       }
       else if (nss.str().find("VERT_RESOLUTION") != std::string::npos) {
          vert_resolution = nss.str().substr(nss.str().find(delimiter)+1, nss.str().length()-1);
          // Remove any whitespace
          vert_resolution.erase(std::remove(vert_resolution.begin(), vert_resolution.end(), ' '), vert_resolution.end());
          cerr << "vert_resolution: " << vert_resolution << '\n';
       }
       else if (nss.str().find("GRID_TYPE") != std::string::npos) {
          grid_type = nss.str().substr(nss.str().find(delimiter)+1, nss.str().length()-1);
          // Remove any whitespace
          grid_type.erase(std::remove(grid_type.begin(), grid_type.end(),' '), grid_type.end());
          cerr << "grid_type: " << grid_type << '\n';
       }
       else if (nss.str().find("UPLOAD_INTERVAL") != std::string::npos) {
          tmpstr1 = nss.str().substr(nss.str().find(delimiter)+1, nss.str().length()-1);
          // Remove any whitespace
          tmpstr1.erase(std::remove(tmpstr1.begin(), tmpstr1.end(),' '), tmpstr1.end());
          upload_interval=std::stoi(tmpstr1);
          cerr << "upload_interval: " << upload_interval << '\n';
       }
       else if (nss.str().find("UTSTEP") != std::string::npos) {
          tmpstr2 = nss.str().substr(nss.str().find(delimiter)+1, nss.str().length()-1);
          // Remove any whitespace
	  tmpstr2.erase(std::remove(tmpstr2.begin(), tmpstr2.end(),','), tmpstr2.end());
          tmpstr2.erase(std::remove(tmpstr2.begin(), tmpstr2.end(),' '), tmpstr2.end());
          timestep_interval = std::stoi(tmpstr2);
          cerr << "utstep: " << timestep_interval << '\n';
       }
       else if (nss.str().find("!NFRPOS") != std::string::npos) {
          tmpstr3 = nss.str().substr(nss.str().find(delimiter)+1, nss.str().length()-1);
          // Remove any whitespace and commas
          tmpstr3.erase(std::remove(tmpstr3.begin(), tmpstr3.end(),','), tmpstr3.end());
          tmpstr3.erase(std::remove(tmpstr3.begin(), tmpstr3.end(),' '), tmpstr3.end());
          ICM_file_interval = std::stoi(tmpstr3);
          cerr << "nfrpos: " << ICM_file_interval << '\n';
       }
       else if (nss.str().find("NFRRES") != std::string::npos) {     // frequency of model output: +ve steps, -ve in hours.
          tmpstr3 = nss.str().substr(nss.str().find(delimiter)+1, nss.str().length()-1);
          // Remove any whitespace and commas
          tmpstr3.erase(std::remove(tmpstr3.begin(), tmpstr3.end(),','), tmpstr3.end());
          tmpstr3.erase(std::remove(tmpstr3.begin(), tmpstr3.end(),' '), tmpstr3.end());
          if ( check_stoi(tmpstr3) ) {
            restart_interval = stoi(tmpstr3);
          } else {
            cerr << "..Warning, unable to read restart interval, setting to zero, got string: " << tmpstr3 << std::endl;
            restart_interval = 0;
          }
       }
    }
    namelist_filestream.close();

    // restart frequency might be in units of hrs, convert to model steps
    if ( restart_interval < 0 )   restart_interval = abs(restart_interval)*3600 / timestep_interval;
    cerr << "nfrres: restart dump frequency (steps) " << restart_interval << '\n';

    // this should match CUSTEP in fort.4. If it doesn't we have a problem
    total_nsteps = (num_days * 86400.0) / (double) timestep_interval;


    // Process the ic_ancil_file:
    std::string ic_ancil_zip = slot_path + std::string("/") + ic_ancil_file + std::string(".zip");
	
    // For transfer downloading, BOINC renames download files to jf_HEXADECIMAL-NUMBER, these files
    // need to be renamed back to the original name
    // Get the name of the 'jf_' filename from a link within the ic_ancil_file
    std::string ic_ancil_source = get_tag(ic_ancil_zip);

    // Copy the IC ancils to working directory
    std::string ic_ancil_destination = ic_ancil_zip;
    cerr << "Copying IC ancils from: " << ic_ancil_source << " to: " << ic_ancil_destination << '\n';
    retval = boinc_copy(ic_ancil_source.c_str(), ic_ancil_destination.c_str());
    if (retval) {
       cerr << "..Copying the IC ancils to the working directory failed" << std::endl;
       return retval;
    }

    // Unzip the IC ancils zip file
    cerr << "Unzipping the IC ancils zip file: " << ic_ancil_zip << '\n';
    retval = boinc_zip(UNZIP_IT, ic_ancil_zip.c_str(), slot_path);
    if (retval) {
       cerr << "..Unzipping the IC ancils file failed" << std::endl;
       return retval;
    }
    // Remove the zip file
    else {
       std::remove(ic_ancil_zip.c_str());
    }


    // Process the ifsdata_file:
    // Make the ifsdata directory
    std::string ifsdata_folder = slot_path + std::string("/ifsdata");
    if (mkdir(ifsdata_folder.c_str(),S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH) != 0) cerr << "..mkdir for ifsdata folder failed" << '\n';

    // Get the name of the 'jf_' filename from a link within the ifsdata_file
    std::string ifsdata_source = get_tag(slot_path + std::string("/") + ifsdata_file + std::string(".zip"));

    // Copy the ifsdata_file to the working directory
    std::string ifsdata_destination = ifsdata_folder + std::string("/") + ifsdata_file + std::string(".zip");
    cerr << "Copying the ifsdata_file from: " << ifsdata_source << " to: " << ifsdata_destination << '\n';
    retval = boinc_copy(ifsdata_source.c_str(), ifsdata_destination.c_str());
    if (retval) {
       cerr << "..Copying the ifsdata file to the working directory failed" << std::endl;
       return retval;
    }

    // Unzip the ifsdata_file zip file
    std::string ifsdata_zip = ifsdata_folder + std::string("/") + ifsdata_file + std::string(".zip");
    cerr << "Unzipping the ifsdata_zip file: " << ifsdata_zip << '\n';
    retval = boinc_zip(UNZIP_IT, ifsdata_zip.c_str(), ifsdata_folder + std::string("/"));
    if (retval) {
       cerr << "..Unzipping the ifsdata_zip file failed" << std::endl;
       return retval;
    }
    // Remove the zip file
    else {
       std::remove(ifsdata_zip.c_str());
    }


    // Process the climate_data_file:
    // Make the climate data directory
    std::string climate_data_path = slot_path + std::string("/") + horiz_resolution + grid_type;
    if (mkdir(climate_data_path.c_str(),S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH) != 0) \
                       cerr << "..mkdir for the climate data folder failed" << std::endl;

    // Get the name of the 'jf_' filename from a link within the climate_data_file
    std::string climate_data_source = get_tag(slot_path + std::string("/") + climate_data_file + std::string(".zip"));

    // Copy the climate data file to working directory
    std::string climate_data_destination = climate_data_path + std::string("/") + climate_data_file + std::string(".zip");
    cerr << "Copying the climate data file from: " << climate_data_source << " to: " << climate_data_destination << '\n';
    retval = boinc_copy(climate_data_source.c_str(), climate_data_destination.c_str());
    if (retval) {
       cerr << "..Copying the climate data file to the working directory failed" << std::endl;
       return retval;
    }	

    // Unzip the climate data zip file
    std::string climate_zip = climate_data_destination;
    cerr << "Unzipping the climate data zip file: " << climate_zip << '\n';
    retval = boinc_zip(UNZIP_IT, climate_zip.c_str(), climate_data_path);
    if (retval) {
       cerr << "..Unzipping the climate data file failed" << std::endl;
       return retval;
    }
    // Remove the zip file
    else {
       std::remove(climate_zip.c_str());
    }

	
    // Set the environmental variables:
    // Set the OIFS_DUMMY_ACTION environmental variable, this controls what OpenIFS does if it goes into a dummy subroutine
    // Possible values are: 'quiet', 'verbose' or 'abort'
    std::string OIFS_var("OIFS_DUMMY_ACTION=abort");
    if (putenv((char *)OIFS_var.c_str())) {
      cerr << "..Setting the OIFS_DUMMY_ACTION environmental variable failed" << std::endl;
      return 1;
    }
    pathvar = getenv("OIFS_DUMMY_ACTION");
    //cerr << "The OIFS_DUMMY_ACTION environmental variable is: " << pathvar << '\n';

    // Set the OMP_NUM_THREADS environmental variable, the number of threads
    std::string OMP_NUM_var = std::string("OMP_NUM_THREADS=") + nthreads;
    if (putenv((char *)OMP_NUM_var.c_str())) {
      cerr << "..Setting the OMP_NUM_THREADS environmental variable failed" << std::endl;
      return 1;
    }
    pathvar = getenv("OMP_NUM_THREADS");
    //cerr << "The OMP_NUM_THREADS environmental variable is: " << pathvar << '\n';

    // Set the OMP_SCHEDULE environmental variable, this enforces static thread scheduling
    std::string OMP_SCHED_var("OMP_SCHEDULE=STATIC");
    if (putenv((char *)OMP_SCHED_var.c_str())) {
      cerr << "..Setting the OMP_SCHEDULE environmental variable failed" << std::endl;
      return 1;
    }
    pathvar = getenv("OMP_SCHEDULE");
    //cerr << "The OMP_SCHEDULE environmental variable is: " << pathvar << '\n';

    // Set the DR_HOOK environmental variable, this controls the tracing facility in OpenIFS, off=0 and on=1
    std::string DR_HOOK_var("DR_HOOK=1");
    if (putenv((char *)DR_HOOK_var.c_str())) {
      cerr << "..Setting the DR_HOOK environmental variable failed" << std::endl;
      return 1;
    }
    pathvar = getenv("DR_HOOK");
    //cerr << "The DR_HOOK environmental variable is: " << pathvar << '\n';

    // Set the DR_HOOK_HEAPCHECK environmental variable, this ensures the heap size statistics are reported
    std::string DR_HOOK_HEAP_var("DR_HOOK_HEAPCHECK=no");
    if (putenv((char *)DR_HOOK_HEAP_var.c_str())) {
      cerr << "..Setting the DR_HOOK_HEAPCHECK environmental variable failed" << std::endl;
      return 1;
    }
    pathvar = getenv("DR_HOOK_HEAPCHECK");
    //cerr << "The DR_HOOK_HEAPCHECK environmental variable is: " << pathvar << '\n';

    // Set the DR_HOOK_STACKCHECK environmental variable, this ensures the stack size statistics are reported
    std::string DR_HOOK_STACK_var("DR_HOOK_STACKCHECK=no");
    if (putenv((char *)DR_HOOK_STACK_var.c_str())) {
      cerr << "..Setting the DR_HOOK_STACKCHECK environmental variable failed" << std::endl;
      return 1;
    }
    pathvar = getenv("DR_HOOK_STACKCHECK");
    //cerr << "The DR_HOOK_STACKCHECK environmental variable is: " << pathvar << '\n';
	
    // Set the EC_MEMINFO environment variable, only applies to OpenIFS 43r3.
    // Disable EC_MEMINFO to remove the useless EC_MEMINFO messages to the stdout file to reduce filesize.
    std::string EC_MEMINFO("EC_MEMINFO=0");
    if (putenv((char *)EC_MEMINFO.c_str())) {
       cerr << "..Setting the EC_MEMINFO environment variable failed" << std::endl;
       return 1;
    }
    pathvar = getenv("EC_MEMINFO");
    //cerr << "The EC_MEMINFO environment variable is: " << pathvar << '\n';

    // Disable Heap memory stats at end of run; does not work for CPDN version of OpenIFS
    std::string EC_PROFILE_HEAP("EC_PROFILE_HEAP=0");
    if (putenv((char *)EC_PROFILE_HEAP.c_str())) {
       cerr << "..Setting the EC_PROFILE_HEAP environment variable failed" << std::endl;
       return 1;
    }
    pathvar = getenv("EC_PROFILE_HEAP");
    //cerr << "The EC_PROFILE_HEAP environment variable is: " << pathvar << '\n';

    // Disable all memory stats at end of run; does not work for CPDN version of OpenIFS
    std::string EC_PROFILE_MEM("EC_PROFILE_MEM=0");
    if (putenv((char *)EC_PROFILE_MEM.c_str())) {
       cerr << "..Setting the EC_PROFILE_MEM environment variable failed" << std::endl;
       return 1;
    }
    pathvar = getenv("EC_PROFILE_MEM");
    //cerr << "The EC_PROFILE_MEM environment variable is: " << pathvar << '\n';

    // Set the OMP_STACKSIZE environmental variable, OpenIFS needs more stack memory per process
    std::string OMP_STACK_var("OMP_STACKSIZE=128M");
    if (putenv((char *)OMP_STACK_var.c_str())) {
      cerr << "..Setting the OMP_STACKSIZE environmental variable failed" << std::endl;
      return 1;
    }
    pathvar = getenv("OMP_STACKSIZE");
    //cerr << "The OMP_STACKSIZE environmental variable is: " << pathvar << '\n';


    // Set the core dump size to 0
    struct rlimit core_limits;
    core_limits.rlim_cur = core_limits.rlim_max = 0;
    if (setrlimit(RLIMIT_CORE, &core_limits) != 0) cerr << "..Setting the core dump size to 0 failed" << std::endl;

    // Set the stack limit to be unlimited
    struct rlimit stack_limits;
    // In macOS we cannot set the stack size limit to infinity
    #ifndef __APPLE__ // Linux
       stack_limits.rlim_cur = stack_limits.rlim_max = RLIM_INFINITY;
       if (setrlimit(RLIMIT_STACK, &stack_limits) != 0) cerr << "..Setting the stack limit to unlimited failed" << std::endl;
    #endif

    int last_cpu_time, restart_cpu_time = 0, upload_file_number, last_upload, model_completed, restart_iter;
    std::string last_iter = "0";

    // last_upload is the time of the last upload file (in seconds)

    // Define the name and location of the progress file
    std::string progress_file = slot_path+std::string("/progress_file_")+wuid+std::string(".xml");
	
    // Model progress is held in the progress file
    // First check if a file is not already present from an unscheduled shutdown
    cerr << "Checking for progress XML file: " << progress_file << '\n';

    if ( file_exists(progress_file) && !file_is_empty(progress_file) ) {
       std::ifstream progress_file_in(progress_file);
       std::stringstream progress_file_buffer;
       xml_document<> doc;

       // If present parse file and extract values
       progress_file_in.open(progress_file);
       cerr << "Opened progress file ok : " << progress_file << '\n';
       progress_file_buffer << progress_file_in.rdbuf();
       progress_file_in.close();
	    
       // Parse XML progress file
       // RapidXML needs careful memory management. Use string to preserve memory for later xml_node calls.
       // Passing &progress_file_buffer.str()[0] caused new str on heap & memory error.
       std::string prog_contents = progress_file_buffer.str();       // could use vector<char> here

       doc.parse<0>(&prog_contents[0]);
       xml_node<> *root_node = doc.first_node("running_values");
       xml_node<> *last_cpu_time_node = root_node->first_node("last_cpu_time");
       xml_node<> *upload_file_number_node = root_node->first_node("upload_file_number");
       xml_node<> *last_iter_node = root_node->first_node("last_iter");
       xml_node<> *last_upload_node = root_node->first_node("last_upload");
       xml_node<> *model_completed_node = root_node->first_node("model_completed");

       // Set the values from the XML
       last_cpu_time = std::stoi(last_cpu_time_node->value());
       upload_file_number = std::stoi(upload_file_number_node->value());
       last_iter = last_iter_node->value();
       last_upload = std::stoi(last_upload_node->value());
       model_completed = std::stoi(model_completed_node->value());

       // Adjust last_iter to the step of the previous model restart dump step.
       // This is always a multiple of the restart frequency

       cerr << "-- Model is restarting --\n";
       cerr << "Adjusting last_iter, " << last_iter << ", to previous model restart step.\n";
       restart_iter = stoi(last_iter);
       restart_iter = restart_iter - ((restart_iter % restart_interval) - 1);   // -1 because the model will continue from restart_iter.
       last_iter = to_string(restart_iter); 
    }
    else {
       // Set the initial values for start of model run
       last_cpu_time = 0;
       upload_file_number = 0;
       last_iter = "0";
       last_upload = 0;
       model_completed = 0;
    }
	    
    // Write out the new progress file. Note this truncates progress_file to zero bytes if it already exists (as in a model restart)
    std::ofstream progress_file_out(progress_file);
    cerr << "Creating progress file: " << progress_file << '\n';

    progress_file_out.open(progress_file);
    progress_file_out <<"<?xml version=\"1.0\" encoding=\"utf-8\"?>"<< '\n';
    progress_file_out <<"<running_values>"<< '\n';
    progress_file_out <<"  <last_cpu_time>"<<std::to_string(last_cpu_time)<<"</last_cpu_time>"<< '\n';
    progress_file_out <<"  <upload_file_number>"<<std::to_string(upload_file_number)<<"</upload_file_number>"<< '\n';
    progress_file_out <<"  <last_iter>"<<last_iter<<"</last_iter>"<< '\n';
    progress_file_out <<"  <last_upload>"<<std::to_string(last_upload)<<"</last_upload>"<< '\n';
    progress_file_out <<"  <model_completed>"<<std::to_string(model_completed)<<"</model_completed>"<< '\n';
    progress_file_out <<"</running_values>"<< std::endl;
    progress_file_out.close();

    cerr << "last_cpu_time: " << last_cpu_time << '\n';
    cerr << "upload_file_number: " << upload_file_number << '\n';
    cerr << "last_iter: " << last_iter << '\n';
    cerr << "last_upload: " << last_upload << '\n';
    cerr << "model_completed: " << model_completed << '\n';


    fraction_done = 0;
    trickle_upload_count = 0;

    // seconds between upload files: upload_interval
    // seconds between ICM files: ICM_file_interval * timestep_interval
    // upload interval in steps = upload_interval / timestep_interval
    //cerr "upload_interval: "<< upload_interval << ", timestep_interval: " << timestep_interval << '\n';

    // Check if upload_interval x timestep_interval equal to zero
    if (upload_interval * timestep_interval == 0) {
       cerr << "..upload_interval x timestep_interval equals zero" << std::endl;
       return 1;
    }

    int total_length_of_simulation = (int) (num_days * 86400);
    cerr << "total_length_of_simulation: " << total_length_of_simulation << '\n';

    // Get result_base_name to construct upload file names using 
    // the first upload as an example and then stripping off '_0.zip'

    if (!boinc_is_standalone()) {
       retval = boinc_resolve_filename_s("upload_file_0.zip", resolved_name);
       if (retval) {
          cerr << "..boinc_resolve_filename failed" << std::endl;
	  return 1;
       }

       result_base_name = std::filesystem::path(resolved_name).stem();     // returns filename without path nor '.zip'
       if ( result_base_name.length() > 2 ){
          result_base_name.erase(result_base_name.length()-2);                   // removes the '_0'
       }

       cerr << "result_base_name: " << result_base_name << '\n';
       if (result_base_name.compare("upload_file") == 0) {
          cerr << "..Failed to get result name" << std::endl;
          return 1;
       }
    }

    // Check for the existence of a Unix script file to override the environment variables
    // Script file should be in projects folder
    std::string override_env_vars = project_path + std::string("override_env_variables");
    if(file_exists(override_env_vars)) {
       // If exists then run file
       FILE* pipe = popen(override_env_vars.c_str(), "r");
       if (!pipe) {
          cerr << "..Failed to open environment variables override file" << std::endl;
          return 1;
       }
       pclose(pipe);
    }	
	

    // Start the OpenIFS job
    std::string strCmd = slot_path + std::string("/oifs_43r3_model.exe");
    handleProcess = launch_process(slot_path, strCmd.c_str(), exptid.c_str(), app_name);
    if (handleProcess > 0) process_status = 0;

    boinc_end_critical_section();


    // process_status = 0 running
    // process_status = 1 stopped normally
    // process_status = 2 stopped with quit request from BOINC
    // process_status = 3 stopped with child process being killed
    // process_status = 4 stopped with child process being stopped
    // process_status = 5 child process not found by waitpid()


    // Main loop:	
    // Periodically check the process status and the BOINC client status
    std::string stat_lastline = "";

    while (process_status == 0 && model_completed == 0) {
       sleep_until(system_clock::now() + seconds(1));

       count++;

       // Check every 10 seconds whether an upload point has been reached
       if(count==10) {
         
          iter = last_iter;
          if( file_exists(slot_path + std::string("/ifs.stat")) ) {

             //  To reduce I/O, open file once only and use oifs_parse_ifsstat() to parse the step count
             if( !(ifs_stat_file.is_open()) ) {
                ifs_stat_file.open(slot_path + std::string("/ifs.stat"));
             } 
             if( ifs_stat_file.is_open() ) {

                // Read completed step from last line of ifs.stat file.
                // Note the first line from the model has a step count of '....  CNT3      -999 ....'
                // When the iteration number changes in the ifs.stat file, OpenIFS has completed writing
                // to the output files for that iteration, those files can now be moved and uploaded.

                oifs_get_stat(ifs_stat_file, stat_lastline);
                if ( oifs_parse_stat(stat_lastline, iter, 4) ) {     // iter updates
                   if ( !oifs_valid_step(iter,total_nsteps) ) {
                     iter = last_iter;
                   }
                }
             }
          } 

          if (std::stoi(iter) != std::stoi(last_iter)) {
             // Construct file name of the ICM result file
             second_part = get_second_part(last_iter, exptid);

             // Move the ICMGG result file to the temporary folder in the project directory
             if(file_exists(slot_path + std::string("/ICMGG") + second_part)) {
                cerr << "Moving to projects directory: " << (slot_path + std::string("/ICMGG") + second_part) << '\n';
                retval = boinc_copy((slot_path + std::string("/ICMGG") + second_part).c_str() , \
                                    (temp_path + std::string("/ICMGG") + second_part).c_str());
                if (retval) {
                   cerr << "..Copying ICMGG result file to the temp folder in the projects directory failed" << std::endl;
                   return retval;
                }
                // If result file has been successfully copied over, remove it from slots directory
                else {
                   std::remove((slot_path + std::string("/ICMGG") + second_part).c_str());
                }
             }

             // Move the ICMSH result file to the temporary folder in the project directory
             if(file_exists(slot_path+std::string("/ICMSH") + second_part)) {
                cerr << "Moving to projects directory: " << (slot_path + std::string("/ICMSH") + second_part) << '\n';
                retval = boinc_copy((slot_path + std::string("/ICMSH") + second_part).c_str() , \
                                    (temp_path + std::string("/ICMSH") + second_part).c_str());
                if (retval) {
                   cerr << "..Copying ICMSH result file to the temp folder in the projects directory failed" << std::endl;
                   return retval;
                }
                // If result file has been successfully copied over, remove it from slots directory
                else {
                   std::remove((slot_path + std::string("/ICMSH") + second_part).c_str());
                }
             }

             // Move the ICMUA result file to the temporary folder in the project directory (this is for 43r3 and above only)
             if(file_exists(slot_path+std::string("/ICMUA") + second_part)) {
                cerr << "Moving to projects directory: " << (slot_path + std::string("/ICMUA") + second_part) << '\n';
                retval = boinc_copy((slot_path+std::string("/ICMUA") + second_part).c_str() , \
                                    (temp_path+std::string("/ICMUA") + second_part).c_str());
                if (retval) {
                   cerr << "..Copying ICMUA result file to the temp folder in the projects directory failed" << std::endl;
                   return retval;
                }
                // If result file has been successfully copied over, remove it from slots directory
                else {
                   std::remove((slot_path+std::string("/ICMUA") + second_part).c_str());
                }
             }
		  
             // Convert iteration number to seconds
             current_iter = (std::stoi(last_iter)) * timestep_interval;

             //cerr << "Current iteration of model: " << last_iter << '\n';
             //cerr << "timestep_interval: " << timestep_interval << '\n';
             //cerr << "current_iter: " << current_iter << '\n';
             //cerr << "last_upload: " << last_upload << '\n';

             // Upload a new upload file if the end of an upload_interval has been reached
             if((( current_iter - last_upload ) >= (upload_interval * timestep_interval)) && (current_iter < total_length_of_simulation)) {
                // Create an intermediate results zip file using BOINC zip
                zfl.clear();

                boinc_begin_critical_section();

                // Cycle through all the steps from the last upload to the current upload
                for (i = (last_upload / timestep_interval); i < (current_iter / timestep_interval); i++) {
                   //cerr << "last_upload/timestep_interval: " << (last_upload/timestep_interval) << '\n';
                   //cerr << "current_iter/timestep_interval: " << (current_iter/timestep_interval) << '\n';
                   //cerr << "i: " << (std::to_string(i)) << '\n';

                   // Construct file name of the ICM result file
                   second_part = get_second_part(std::to_string(i), exptid);

                   // Add ICMGG result files to zip to be uploaded
                   if(file_exists(temp_path + std::string("/ICMGG") + second_part)) {
                      cerr << "Adding to the zip: " << (temp_path + std::string("/ICMGG")+second_part) << '\n';
                      zfl.push_back(temp_path + std::string("/ICMGG") + second_part);
                      // Delete the file that has been added to the zip
                      // std::remove((temp_path+std::string("/ICMGG")+second_part).c_str());
                   }

                   // Add ICMSH result files to zip to be uploaded
                   if(file_exists(temp_path + std::string("/ICMSH") + second_part)) {
                      cerr << "Adding to the zip: " << (temp_path + std::string("/ICMSH")+second_part) << '\n';
                      zfl.push_back(temp_path + std::string("/ICMSH") + second_part);
                      // Delete the file that has been added to the zip
                      // std::remove((temp_path+std::string("/ICMSH")+second_part).c_str());
                   }
		
                   // Add ICMUA result files to zip to be uploaded
                   if(file_exists(temp_path + std::string("/ICMUA") + second_part)) {
                      cerr << "Adding to the zip: " << (temp_path + std::string("/ICMUA")+second_part) << '\n';
                      zfl.push_back(temp_path + std::string("/ICMUA") + second_part);
                      // Delete the file that has been added to the zip
                      // std::remove((temp_path+std::string("/ICMUA")+second_part).c_str());
                   }
                }

                // If running under a BOINC client
                if (!boinc_is_standalone()) {

                   if (zfl.size() > 0){

                      // Create the zipped upload file from the list of files added to zfl
                      upload_file = project_path + result_base_name + "_" + std::to_string(upload_file_number) + ".zip";

                      cerr << "Zipping up the intermediate file: " << upload_file << '\n';
                      //upfile = string(upload_file);
                      upfile = upload_file;
                      retval = boinc_zip(ZIP_IT, upfile, &zfl);  // n.b. pass std::string to avoid copy-on-call
                      upfile.clear();

                      if (retval) {
                         cerr << "..Zipping up the intermediate file failed" << std::endl;
                         boinc_end_critical_section();
                         return retval;
                      }
                      else {
                         // Files have been successfully zipped, they can now be deleted
                         for (j = 0; j < (int) zfl.size(); ++j) {
                            // Delete the zipped file
                            std::remove(zfl[j].c_str());
                         }
                      }
                   
                      // Upload the file. In BOINC the upload file is the logical name, not the physical name
                      upload_file_name = std::string("upload_file_") + std::to_string(upload_file_number) + std::string(".zip");
                      cerr << "Uploading the intermediate file: " << upload_file_name << '\n';
                      sleep_until(system_clock::now() + seconds(20));
                      boinc_upload_file(upload_file_name);
                      retval = boinc_upload_status(upload_file_name);
                      if (!retval) {
                         cerr << "Finished the upload of the intermediate file: " << upload_file_name << '\n';
                      }
		      
                      trickle_upload_count++;
                      if (trickle_upload_count == 10) {
                        // Produce trickle
                        process_trickle(current_cpu_time,wu_name,result_base_name,slot_path,current_iter);
                        trickle_upload_count = 0;
                      }
                   }
                   last_upload = current_iter; 
                }

                // Else running in standalone
                else {
                   upload_file_name = app_name + std::string("_") + unique_member_id + std::string("_") + start_date + std::string("_") + \
                               std::to_string(num_days_trunc) + std::string("_") + batchid + std::string("_") + wuid + std::string("_") + \
                               std::to_string(upload_file_number) + std::string(".zip");
                   cerr << "The current upload_file_name is: " << upload_file_name << '\n';

                   // Create the zipped upload file from the list of files added to zfl
                   upload_file = project_path + upload_file_name;

                   if (zfl.size() > 0){
                      //upfile = string(upload_file);
                      upfile = upload_file;
                      retval = boinc_zip(ZIP_IT,upfile,&zfl);
                      upfile.clear();

                      if (retval) {
                         cerr << "..Creating the zipped upload file failed" << std::endl;
                         boinc_end_critical_section();
                         return retval;
                      }
                      else {
                         // Files have been successfully zipped, they can now be deleted
                         for (j = 0; j < (int) zfl.size(); ++j) {
                            // Delete the zipped file
                            std::remove(zfl[j].c_str());
                         }
                      }
                   }
                   last_upload = current_iter;
		
                   trickle_upload_count++;
                   if (trickle_upload_count == 10) {
                      // Produce trickle
                      process_trickle(current_cpu_time,wu_name,result_base_name,slot_path,current_iter);
                      trickle_upload_count = 0;
                   }

                }
                boinc_end_critical_section();
                upload_file_number++;
             }
          }
          last_iter = iter;
          count = 0;
	       
          // Update the progress file	
          progress_file_out.open(progress_file);
          progress_file_out <<"<?xml version=\"1.0\" encoding=\"utf-8\"?>"<< '\n';
          progress_file_out <<"<running_values>"<< '\n';
          progress_file_out <<"  <last_cpu_time>"<<std::to_string(current_cpu_time)<<"</last_cpu_time>"<< '\n';
          progress_file_out <<"  <upload_file_number>"<<std::to_string(upload_file_number)<<"</upload_file_number>"<< '\n';
          progress_file_out <<"  <last_iter>"<<last_iter<<"</last_iter>"<< '\n';
          progress_file_out <<"  <last_upload>"<<std::to_string(last_upload)<<"</last_upload>"<< '\n';
          progress_file_out <<"  <model_completed>"<<std::to_string(model_completed)<<"</model_completed>"<< '\n';
          progress_file_out <<"</running_values>"<< std::endl;
          progress_file_out.close();
       }
	    
       // Calculate current_cpu_time, only update if cpu_time returns a value
       if (cpu_time(handleProcess)) {
          current_cpu_time = last_cpu_time + cpu_time(handleProcess);
          //fprintf(stderr,"current_cpu_time: %1.5f\n",current_cpu_time);
       }
	       

      // Calculate the fraction done
      fraction_done = model_frac_done( atof(iter.c_str()), total_nsteps, atoi(nthreads.c_str()) );
      //fprintf(stderr,"fraction done: %.6f\n", fraction_done);
     

      if (!boinc_is_standalone()) {
	     // If the current iteration is at a restart iteration     
	     if (!(std::stoi(iter)%restart_interval)) restart_cpu_time = current_cpu_time;
	      
         // Provide the current cpu_time to the BOINC server (note: this is deprecated in BOINC)
         boinc_report_app_status(current_cpu_time,restart_cpu_time,fraction_done);

         // Provide the fraction done to the BOINC client, 
         // this is necessary for the percentage bar on the client
         boinc_fraction_done(fraction_done);
	  
         // Check the status of the client if not in standalone mode     
         process_status = check_boinc_status(handleProcess,process_status);
      }
	
      // Check the status of the child process    
      process_status = check_child_status(handleProcess,process_status);
    }


    // Time delay to ensure model files are all flushed to disk
    sleep_until(system_clock::now() + seconds(60));

    // Print content of key model files to help with diagnosing problems
    print_last_lines("NODE.001_01", 70);    //  main model output log	

    // To check whether model completed successfully, look for 'CNT0' in 3rd column of ifs.stat
    // This will always be the last line of a successful model forecast.
    if(file_exists(slot_path + std::string("/ifs.stat"))) {
       ifs_word="";
       oifs_get_stat(ifs_stat_file, last_line);
       oifs_parse_stat(last_line, ifs_word, 3);
       if (ifs_word!="CNT0") {
         cerr << "CNT0 not found; string returned was: " << "'" << ifs_word << "'" << '\n';
         // print extra files to help diagnose fail
         print_last_lines("ifs.stat",8);
         print_last_lines("rcf",11);              // openifs restart control
         print_last_lines("waminfo",17);          // wave model restart control
         print_last_lines(progress_file,8);
         cerr << "..Failed, model did not complete successfully" << std::endl;
         return 1;
       }
    }
    // ifs.stat has not been produced, then model did not start
    else {
       cerr << "..Failed, model did not start" << std::endl;
       return 1;	    
    }
	
	
    // Update model_completed
    model_completed = 1;

    // We need to handle the last ICM files
    // Construct final file name of the ICM result file
    second_part = get_second_part(last_iter, exptid);

    // Move the ICMGG result file to the temporary folder in the project directory
    if(file_exists(slot_path+std::string("/ICMGG") + second_part)) {
       cerr << "Moving to projects directory: " << (slot_path+std::string("/ICMGG") + second_part) << '\n';
       retval = boinc_copy((slot_path + std::string("/ICMGG") + second_part).c_str() , \
                           (temp_path + std::string("/ICMGG") + second_part).c_str());
       if (retval) {
          cerr << "..Copying ICMGG result file to the temp folder in the projects directory failed" << std::endl;
          return retval;
       }
       // If result file has been successfully copied over, remove it from slots directory
       else {
          std::remove((slot_path + std::string("/ICMGG") + second_part).c_str());
       }
    }

    // Move the ICMSH result file to the temporary folder in the project directory
    if(file_exists(slot_path+std::string("/ICMSH") + second_part)) {
       cerr << "Moving to projects directory: " << (slot_path+std::string("/ICMSH") + second_part) << '\n';
       retval = boinc_copy((slot_path + std::string("/ICMSH") + second_part).c_str() , \
                           (temp_path + std::string("/ICMSH") + second_part).c_str());
       if (retval) {
          cerr << "..Copying ICMSH result file to the temp folder in the projects directory failed" << std::endl;
          return retval;
       }
       // If result file has been successfully copied over, remove it from slots directory
       else {
          std::remove((slot_path+std::string("/ICMSH")+second_part).c_str());
       }
    }

    // Move the ICMUA result file to the temporary folder in the project directory (this is for 43r3 and above only)
    if(file_exists(slot_path + std::string("/ICMUA") + second_part)) {
       cerr << "Moving to projects directory: " << (slot_path+std::string("/ICMUA") + second_part) << '\n';
       retval = boinc_copy((slot_path + std::string("/ICMUA") + second_part).c_str() , \
                           (temp_path + std::string("/ICMUA") + second_part).c_str());
       if (retval) {
          cerr << "..Copying ICMUA result file to the temp folder in the projects directory failed" << std::endl;
	  return retval;
       }
       // If result file has been successfully copied over, remove it from slots directory
       else {
          std::remove((slot_path + std::string("/ICMUA") + second_part).c_str());
       }
    }
    
	    
    boinc_begin_critical_section();

    // Create the final results zip file

    zfl.clear();
    std::string node_file = slot_path + std::string("/NODE.001_01");
    zfl.push_back(node_file);
    std::string ifsstat_file = slot_path + std::string("/ifs.stat");
    zfl.push_back(ifsstat_file);
    cerr << "Adding to the zip: " << node_file << '\n';
    cerr << "Adding to the zip: " << ifsstat_file << '\n';

    // Read the remaining list of files from the slots directory and add the matching files to the list of files for the zip
    dirp = opendir(temp_path.c_str());
    if (dirp) {
        regcomp(&regex,"\\+",0);
        while ((dir = readdir(dirp)) != NULL) {
          //cerr << "In temp folder: "<< dir->d_name << '\n';

          if (!regexec(&regex,dir->d_name,(size_t) 0,NULL,0)) {
            zfl.push_back(temp_path + std::string("/") + dir->d_name);
            cerr << "Adding to the zip: " << (temp_path+std::string("/") + dir->d_name) << '\n';
          }
        }
        regfree(&regex);
        closedir(dirp);
    }

    // If running under a BOINC client
    if (!boinc_is_standalone()) {
       if (zfl.size() > 0){

          // Create the zipped upload file from the list of files added to zfl
          upload_file = project_path + result_base_name + "_" + std::to_string(upload_file_number) + ".zip";

          cerr << "Zipping up the final file: " << upload_file << '\n';
          upfile = upload_file;
          retval = boinc_zip(ZIP_IT, upfile, &zfl);
          upfile.clear();

          if (retval) {
             cerr << "..Zipping up the final file failed" << std::endl;
             boinc_end_critical_section();
             return retval;
          }
          else {
             // Files have been successfully zipped, they can now be deleted
             for (j = 0; j < (int) zfl.size(); ++j) {
                // Delete the zipped file
                std::remove(zfl[j].c_str());
             }
          }

          // Upload the file. In BOINC the upload file is the logical name, not the physical name
          upload_file_name = std::string("upload_file_") + std::to_string(upload_file_number) + std::string(".zip");
          cerr << "Uploading the final file: " << upload_file_name << '\n';
          sleep_until(system_clock::now() + seconds(20));
          boinc_upload_file(upload_file_name);
          retval = boinc_upload_status(upload_file_name);
          if (!retval) {
             cerr << "Finished the upload of the final file" << '\n';
          }
	       
	  // Produce trickle
          process_trickle(current_cpu_time,wu_name,result_base_name,slot_path,current_iter);
       }
       boinc_end_critical_section();
    }
    // Else running in standalone
    else {
       upload_file_name = app_name + std::string("_") + unique_member_id + std::string("_") + start_date + std::string("_") + \
                   std::to_string(num_days_trunc) + std::string("_") + batchid + std::string("_") + wuid + std::string("_") + \
                   std::to_string(upload_file_number) + std::string(".zip");
       cerr << "The final upload_file_name is: " << upload_file_name << '\n';

       // Create the zipped upload file from the list of files added to zfl
       upload_file = project_path + upload_file_name;

       if (zfl.size() > 0){
          upfile = upload_file;
          retval = boinc_zip(ZIP_IT,upfile,&zfl);
          upfile.clear();
          if (retval) {
             cerr << "..Creating the zipped upload file failed" << std::endl;
             boinc_end_critical_section();
             return retval;
          }
          else {
             // Files have been successfully zipped, they can now be deleted
             for (j = 0; j < (int) zfl.size(); ++j) {
                // Delete the zipped file
                std::remove(zfl[j].c_str());
             }
          }
        }
	// Produce trickle
        process_trickle(current_cpu_time,wu_name,result_base_name,slot_path,current_iter);     
    }

    // Now task has finished, remove the temp folder
    std::remove(temp_path.c_str());

    sleep_until(system_clock::now() + seconds(120));

    // if finished normally
    if (process_status == 1){
      boinc_end_critical_section();
      boinc_finish(0);
      cerr << "Task finished" << std::endl;
      return 0;
    }
    else if (process_status == 2){
      boinc_end_critical_section();
      boinc_finish(0);
      cerr << "Task finished" << std::endl;
      return 0;
    }
    else {
      boinc_end_critical_section();
      boinc_finish(1);
      cerr << "Task finished" << std::endl;
      return 1;
    }	
}




int check_child_status(long handleProcess, int process_status) {
    int stat,pid;

    // Check whether child processed has exited
    // waitpid will return process id of zombie (finished) process; zero if still running
    if ( (pid=waitpid(handleProcess,&stat,WNOHANG)) > 0 ) {
       process_status = 1;
       // Child exited normally but model might still have failed
       if (WIFEXITED(stat)) {
          process_status = 1;
          cerr << "..The child process terminated with status: " << WEXITSTATUS(stat) << std::endl;
       }
       // Child process has exited due to signal that was not caught
       // n.b. OpenIFS has its own signal handler.
       else if (WIFSIGNALED(stat)) {
          process_status = 3;
          cerr << "..The child process has been killed with signal: " << WTERMSIG(stat) << std::endl;
       }
       // Child is stopped
       else if (WIFSTOPPED(stat)) {
          process_status = 4;
          cerr << "..The child process has stopped with signal: " << WSTOPSIG(stat) << std::endl;
       }
    }
    else if ( pid == -1) {
      // should not get here, it means the child could not be found
      process_status = 5;
      cerr << "..Unable to retrieve status of child process " << std::endl;
      perror("waitpid() error");
    }
    return process_status;
}


int check_boinc_status(long handleProcess, int process_status) {
    BOINC_STATUS status;
    boinc_get_status(&status);

    // If a quit, abort or no heartbeat has been received from the BOINC client, end child process
    if (status.quit_request) {
       cerr << "Quit request received from BOINC client, ending the child process" << std::endl;
       kill(handleProcess, SIGKILL);
       process_status = 2;
       return process_status;
    }
    else if (status.abort_request) {
       cerr << "Abort request received from BOINC client, ending the child process" << std::endl;
       kill(handleProcess, SIGKILL);
       process_status = 1;
       return process_status;
    }
    else if (status.no_heartbeat) {
       cerr << "No heartbeat received from BOINC client, ending the child process" << std::endl;
       kill(handleProcess, SIGKILL);
       process_status = 1;
       return process_status;
    }
    // Else if BOINC client is suspended, suspend child process and periodically check BOINC client status
    else {
       if (status.suspended) {
          cerr << "Suspend request received from the BOINC client, suspending the child process" << std::endl;
          kill(handleProcess, SIGSTOP);

          while (status.suspended) {
             boinc_get_status(&status);
             if (status.quit_request) {
                cerr << "Quit request received from the BOINC client, ending the child process" << std::endl;
                kill(handleProcess, SIGKILL);
                process_status = 2;
                return process_status;
             }
             else if (status.abort_request) {
                cerr << "Abort request received from the BOINC client, ending the child process" << std::endl;
                kill(handleProcess, SIGKILL);
                process_status = 1;
                return process_status;
             }
             else if (status.no_heartbeat) {
                cerr << "No heartbeat received from the BOINC client, ending the child process" << std::endl;
                kill(handleProcess, SIGKILL);
                process_status = 1;
                return process_status;
             }
             sleep_until(system_clock::now() + seconds(1));
          }
          // Resume child process
          cerr << "Resuming the child process" << std::endl;
          kill(handleProcess, SIGCONT);
          process_status = 0;
       }
       return process_status;
    }
}


long launch_process(const std::string slot_path,const char* strCmd,const char* exptid, const std::string app_name) {
    int retval = 0;
    long handleProcess;

    //cerr << "slot_path: " << slot_path << '\n';
    //cerr << "strCmd: " << strCmd << '\n';
    //cerr << "exptid: " << exptid << '\n';

    switch((handleProcess=fork())) {
       case -1: {
          cerr << "..Unable to start a new child process" << std::endl;
          exit(0);
          break;
       }
       case 0: { //The child process
          char *pathvar=NULL;
          // Set the GRIB_SAMPLES_PATH environmental variable
          std::string GRIB_SAMPLES_var = std::string("GRIB_SAMPLES_PATH=") + slot_path + \
                                         std::string("/eccodes/ifs_samples/grib1_mlgrib2");
          if (putenv((char *)GRIB_SAMPLES_var.c_str())) {
            cerr << "..Setting the GRIB_SAMPLES_PATH failed" << std::endl;
          }
          pathvar = getenv("GRIB_SAMPLES_PATH");
          cerr << "The GRIB_SAMPLES_PATH environmental variable is: " << pathvar << '\n';

          // Set the GRIB_DEFINITION_PATH environmental variable
          std::string GRIB_DEF_var = std::string("GRIB_DEFINITION_PATH=") + slot_path + \
                                     std::string("/eccodes/definitions");
          if (putenv((char *)GRIB_DEF_var.c_str())) {
            cerr << "..Setting the GRIB_DEFINITION_PATH failed" << std::endl;
          }
          pathvar = getenv("GRIB_DEFINITION_PATH");
          cerr << "The GRIB_DEFINITION_PATH environmental variable is: " << pathvar << '\n';

          if((app_name=="openifs") || (app_name=="oifs_40r1")) { // OpenIFS 40r1
            cerr << "Executing the command: " << strCmd << " -e " << exptid << '\n';
            retval = execl(strCmd,strCmd,"-e",exptid,NULL);
          }
          else {  // OpenIFS 43r3 and above
            cerr << "Executing the command: " << strCmd << '\n';
            retval = execl(strCmd,strCmd,NULL,NULL,NULL);
          }

          // If execl returns then there was an error
          cerr << "..The execl() command failed slot_path=" << slot_path << ",strCmd=" << strCmd << ",exptid=" << exptid << std::endl;
          exit(retval);
          break;
       }
       default: 
          cerr << "The child process has been launched with process id: " << handleProcess << '\n';
    }
    return handleProcess;
}


// Open a file and return the string contained between the arrow tags
std::string get_tag(const std::string &filename) {
    std::ifstream file(filename);
    if (file.is_open()) {
       std::string line;
       while (getline(file, line)) {
          std::string::size_type start = line.find('>');
          if (start != line.npos) {
             std::string::size_type end = line.find('<', start + 1);
             if (end != line.npos) {
                ++start;
                std::string::size_type count_size = end - start;
                return line.substr(start, count_size);
             }
          }
          return "";
       }
       file.close();
    }
    return "";
}


// Produce the trickle and either upload to the project server or as a physical file
void process_trickle(double current_cpu_time, std::string wu_name, std::string result_base_name, std::string slot_path, int timestep) {
    std::string trickle, trickle_location;
    int rsize;

    //cerr << "current_cpu_time: " << current_cpu_time << '\n';
    //cerr << "wu_name: " << wu_name << '\n';
    //cerr << "result_base_name: " << result_base_name << '\n';
    //cerr << "slot_path: " << slot_path << '\n';
    //cerr << "timestep: " << timestep << '\n';

    std::stringstream trickle_buffer;
    trickle_buffer << "<wu>" << wu_name << "</wu>\n<result>" << result_base_name << "</result>\n<ph></ph>\n<ts>" \
                   << timestep << "</ts>\n<cp>" << current_cpu_time << "</cp>\n<vr></vr>\n";
    trickle = trickle_buffer.str();
    cerr << "Contents of trickle:\n" << trickle << '\n';
      
    // Upload the trickle if not in standalone mode
    if (!boinc_is_standalone()) {
       std::string variety("orig");
       cerr << "Uploading trickle at timestep: " << timestep << '\n';
       boinc_send_trickle_up(variety.data(), trickle.data());
    }

    // Write out the trickle in standalone mode
    else {
       std::stringstream trickle_location_buffer;
       trickle_location_buffer << slot_path << "/trickle_" << time(NULL) << ".xml";
       trickle_location = trickle_location_buffer.str();
       cerr << "Writing trickle to: " << trickle_location << '\n';
       FILE* trickle_file = boinc_fopen(trickle_location.c_str(), "w");
       if (trickle_file) {
          fwrite(trickle.c_str(), 1, trickle.length(), trickle_file);
          fclose(trickle_file);
       }
    }
}


// Check whether a file exists
bool file_exists(const std::string& filename)
{
    std::ifstream infile(filename.c_str());
    return infile.good();
}

// Check whether file is zero bytes long
// from: https://stackoverflow.com/questions/2390912/checking-for-an-empty-file-in-c
// returns True if file is zero bytes, otherwise False.
bool file_is_empty(std::string& fpath) {
   return (std::filesystem::file_size(fpath) == 0);
}

// Calculate the cpu_time
double cpu_time(long handleProcess) {
    #ifdef __APPLE_CC__
       double x;
       int retval = boinc_calling_thread_cpu_time(x);
       return x;
    // Placeholder for Windows
    //#elif defined(_WIN32) || defined(_WIN64)
    //   double x;
    //   int retval = boinc_process_cpu_time(GetCurrentProcess(), x);
    //   return x;
    #else
       //getrusage(RUSAGE_SELF,&usage); //Return resource usage measurement
       //tv_sec = usage.ru_utime.tv_sec; //Time spent executing in user mode (seconds)
       //tv_usec = usage.ru_utime.tv_usec; //Time spent executing in user mode (microseconds)
       //return tv_sec+(tv_usec/1000000); //Convert to seconds
       //fprintf(stderr,"tv_sec: %.5f\n",tv_sec);
       //fprintf(stderr,"tv_usec: %.5f\n",(tv_usec/1000000));
       return linux_cpu_time(handleProcess);
    #endif
}


// returns fraction completed of model run
// (candidate for moving into OpenIFS specific src file)
double model_frac_done(double step, double total_steps, int nthreads ) {
   static int     stepm1 = -1;
   static double  heartbeat = 0.0;
   static bool    debug = false;
   double         frac_done, frac_per_step;
   double         heartbeat_inc;

   frac_done     = step / total_steps;	// this increments slowly, as a model step is ~30sec->2mins cpu
   frac_per_step = 1.0 / total_steps;
   
   if (debug) {
      fprintf( stderr,"get_frac_done: step = %.0f\n", step);
      fprintf( stderr,"        total_steps = %.0f\n", total_steps );
      fprintf( stderr,"      frac_per_step = %f\n",   frac_per_step );
   }
   
   // Constant below represents estimate of how many times around the mainloop
   // before the model completes it's next step. This varies alot depending on model
   // resolution, computer speed, etc. Tune it looking at varied runtimes & resolutions!
   // Higher is better than lower to underestimate.
   // 
   // Impact of speedup due to multiple threads is accounted by below.
   //
   // If we want more accuracy could use the ratio of the model timestep to 1h (T159 tstep) to 
   // provide a 'slowdown' factor for higher resolutions.
   heartbeat_inc = (frac_per_step / (70.0 / (double)nthreads) );

   if ( (int) step > stepm1 ) {
      heartbeat = 0.0;
      stepm1 = (int) step;
   } else {
      heartbeat = heartbeat + heartbeat_inc;
      if ( heartbeat > frac_per_step )  heartbeat = frac_per_step - 0.001;  // slightly less than the next step
      frac_done = frac_done + heartbeat;
   } 

   if (frac_done < 0.0)  frac_done = 0.0;
   if (frac_done > 1.0)  frac_done = 0.9999; // never 100% until wrapper finishes
   if (debug){
      fprintf(stderr, "    heartbeat_inc = %.8f\n", heartbeat_inc);
      fprintf(stderr, "    heartbeat     = %.8f\n", heartbeat );
      double percent = frac_done * 100.0;
      fprintf(stderr, "     percent done = %.3f\n", percent);
   }

   return frac_done;
}

// Construct the second part of the file to be uploaded
std::string get_second_part(string last_iter, string exptid) {
   std::string second_part="";

   if (last_iter.length() == 1) {
      second_part = exptid +"+"+ "00000" + last_iter;
   }
   else if (last_iter.length() == 2) {
      second_part = exptid + "+" + "0000" + last_iter;
   }
   else if (last_iter.length() == 3) {
      second_part = exptid + "+" + "000" + last_iter;
   }
   else if (last_iter.length() == 4) {
      second_part = exptid + "+" + "00" + last_iter;
   }
   else if (last_iter.length() == 5) {
      second_part = exptid + "+" + "0" + last_iter;
   }
   else if (last_iter.length() == 6) {
      second_part = exptid + "+" + last_iter;
   }

   return second_part;
}


bool check_stoi(std::string& cin) {
    //  check input string is convertable to an integer by checking for any letters
    //  nb. stoi() will convert leading digits if alphanumeric but we know step must be all digits.
    //  Returns true on success, false if non-numeric data in input string.
    //  Glenn Carver

    int step;

    if (std::any_of(cin.begin(), cin.end(), ::isalpha)) {
        cerr << "..Invalid characters in stoi string: " << cin << std::endl;
        return false;
    }

    //  check stoi standard exceptions
    //  n.b. still need to check step <= max_step
    try {
        step = std::stoi(cin);
        //cerr << "step converted is : " << step << '\n';
        return true;
    }
    catch (const std::invalid_argument &excep) {
        cerr << "..Invalid input argument for stoi : " << excep.what() << std::endl;
        return false;
    }
    catch (const std::out_of_range &excep) {
        cerr << "..Out of range value for stoi : " << excep.what() << std::endl;
        return false;
    }
}


bool oifs_parse_stat(std::string& logline, std::string& stat_column, int index) {
   //   Parse a line of the OpenIFS ifs.stat log file, previously obtained from oifs_get_statline
   //      logline  : incoming ifs.stat logfile line to be parsed
   //      stat_col : returned string given by position 'index'
   //  Returns false if string is empty.

   istringstream tokens;
   std::string statstr="";

   //  split input, get token specified by 'column' unless file is corrupted
   tokens.str(logline);
   for (int i=0; i<index; ++i)
      tokens >> statstr;

   if ( statstr.empty() ){
      return false;
   } else {
      stat_column = statstr;
      //cerr << "oifs_parse_ifsstat: parsed string  = " << stat_column << " index " << index << '\n';
      return true;
   }
}


bool oifs_get_stat(std::ifstream& ifs_stat, std::string& logline) {
   // Parse content of ifs.stat and always return last non-zero line read from log file.
   //
   // Updates stream offset between calls to prevent completely re-reading the file,
   // to reduce file I/O on the volunteer's machine.
   //
   //    ifs_stat : name of logfile (ifs.stat for current generation of OpenIFS models)
   //    logline  : last line read from ifs.stat. Preserved between calls to this fn.
   //    NOTE!  The file MUST already be open. This fn does not close it.
   //
   // Returns: False if file not open, otherwise true.
   //
   // TODO: ideally this should be part of a small class that
   // inherits from ifstream to manage & read ifs.stat, as it relies on trust the
   // callee has not opened & closed this file inbetween calls.
   //
   //     Glenn

    string             statline = "";         // default: 4th element of ifs.stat file lines
    static string      current_line = "";
    static streamoff   p = 0;             // stream offset position

    if ( !ifs_stat.is_open() ) {
        cerr << "oifs_get_stat: error, ifs.stat file is not open" << std::endl;
        p = 0;
        current_line = "";
        return false;
    }

    ifs_stat.seekg(p);
    while ( std::getline(ifs_stat, statline) ) {
      current_line = statline;

      if ( ifs_stat.tellg() == -1 )     // set p to eof for next call to this fn
         p = p + statline.size();
      else
         p = ifs_stat.tellg();
    }
    ifs_stat.clear();           // must clear stream error before attempting to read again as file remains open

    logline = current_line;

    return true;
}


bool oifs_valid_step(std::string& step, int nsteps) {
   //  checks for a valid step count in arg 'step'
   //  Returns :   true if step is valid, otherwise false
   //      Glenn

   // make sure step is valid integer
   if (!check_stoi(step)) {
      cerr << "oifs_valid_step: Invalid characters in stoi string, unable to convert step to int: " << step << '\n';
      return false;
   } else {
      // check step is in valid range: 0 -> total no. of steps
      if (stoi(step)<0) {
         return false;
      } else if (stoi(step) > nsteps) {
         return false;
      }
   }
   return true;
}


int print_last_lines(string filename, int maxlines) {
   // Opens a file if exists and uses circular buffer to read & print last lines of file to stderr.
   // Returns: zero : either can't open file or file is empty
   //          > 0  : no. of lines in file (may be less than maxlines)
   //  Glenn

   int     count = 0;
   int     start, end;
   string  lines[maxlines];
   ifstream filein(filename);

   if ( filein.is_open() ) {
      while ( getline(filein, lines[count%maxlines]) )
         count++;
   }

   if ( count > 0 ) {
      // find the oldest lines first in the buffer, will not be at start if count > maxlines
      start = count > maxlines ? (count%maxlines) : 0;
      end   = min(maxlines,count);

      cerr << ">>> Printing last " << end << " lines from file: " << filename << '\n';
      for ( int i=0; i<end; i++ ) {
         cerr << lines[ (start+i)%maxlines ] << '\n';
      }
      cerr << "------------------------------------------------" << std::endl;
   }

   return count;
}
