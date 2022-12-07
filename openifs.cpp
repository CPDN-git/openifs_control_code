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
#include "boinc/api/boinc_api.h"
#include "boinc/zip/boinc_zip.h"
#include "boinc/lib/util.h"
#include "rapidxml.hpp"
#include <algorithm>

#ifndef _MAX_PATH
   #define _MAX_PATH 512
#endif

const char* strip_path(const char* path);
int check_child_status(long,int);
int check_boinc_status(long,int);
long launch_process(const char*,const char*,const char*,const std::string);
std::string get_tag(const std::string &str);
void process_trickle(double,const char*,const char*,const char*,int);
bool file_exists(const std::string &str);
double cpu_time(long);
double model_frac_done(double,double,int);
std::string get_second_part(const std::string, const std::string);
bool check_stoi(std::string& cin);

using namespace std;
using namespace std::chrono;
using namespace std::this_thread;
using namespace rapidxml;

int main(int argc, char** argv) {
    std::string ifsdata_file, ic_ancil_file, climate_data_file, horiz_resolution, vert_resolution, grid_type;
    std::string project_path, result_name, wu_name, version, tmpstr1, tmpstr2, tmpstr3;
    std::string ifs_line="", iter="-1", ifs_word="", second_part, upload_file_name, last_line="";
    int upload_interval, timestep_interval, ICM_file_interval, process_status, retval=0, i, j;
    int current_iter=0, count=0;	
    char strTmp[_MAX_PATH], upload_file[_MAX_PATH], result_base_name[64];
    char *pathvar;
    long handleProcess;
    double tv_sec, tv_usec, fraction_done, current_cpu_time=0, total_nsteps = 0;
    struct dirent *dir;
    struct rusage usage;
    regex_t regex;
    DIR *dirp;
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
       fprintf(stderr,"..BOINC init options failed\n");
       return retval;
    }

    fprintf(stderr,"(argv0) %s\n",argv[0]);
    fprintf(stderr,"(argv1) start_date: %s\n",argv[1]);
    fprintf(stderr,"(argv2) exptid: %s\n",argv[2]);
    fprintf(stderr,"(argv3) unique_member_id: %s\n",argv[3]);
    fprintf(stderr,"(argv4) batchid: %s\n",argv[4]);
    fprintf(stderr,"(argv5) wuid: %s\n",argv[5]);
    fprintf(stderr,"(argv6) fclen: %s\n",argv[6]);
    fprintf(stderr,"(argv7) app_name: %s\n",argv[7]);
    fprintf(stderr,"(argv8) nthreads: %s\n",argv[8]);

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
    char slot_path[_MAX_PATH];
    if (getcwd(slot_path, sizeof(slot_path)) == NULL)
      fprintf(stderr,"..getcwd returned an error\n");
    else
      fprintf(stderr,"Working directory is: %s\n",slot_path);

    if (!boinc_is_standalone()) {

      // Get the project path
      project_path = dataBOINC.project_dir + std::string("/");
      fprintf(stderr,"Project directory is: %s\n",project_path.c_str());
	    
      // Get the app version and re-parse to add a dot
      version = std::to_string(dataBOINC.app_version);
      if (version.length()==2) {
         version = version.insert(0,".");
         //fprintf(stderr,"version: %s\n",version.c_str());
      }
      else if (version.length()==3) {
         version = version.insert(1,".");
         //fprintf(stderr,"version: %s\n",version.c_str());
      }
      else if (version.length()==4) {
         version = version.insert(2,".");
         //fprintf(stderr,"version: %s\n",version.c_str());
      }
      else {
         fprintf(stderr,"..Error with the length of app_version, length is: %lu\n",version.length());
         return 1;
      }
	    
      fprintf(stderr,"app name: %s\n",app_name.c_str());
      fprintf(stderr,"version: %s\n",version.c_str());
    }
    // Running in standalone
    else {
      fprintf(stderr,"Running in standalone mode\n");
      // Set the project path
      project_path = slot_path + std::string("/../projects/");
      fprintf(stderr,"Project directory is: %s\n",project_path.c_str());
	    
      // In standalone get the app version from the command line
      version = argv[9];
      fprintf(stderr,"app name: %s\n",app_name.c_str());
      fprintf(stderr,"(argv9) app_version: %s\n",argv[9]);
    }

    boinc_begin_critical_section();

    // Create temporary folder for moving the results to and uploading the results from
    // BOINC measures the disk usage on the slots directory so we must move all results out of this folder
    std::string temp_path = project_path + app_name + std::string("_") + wuid;
    fprintf(stderr,"Location of temp folder: %s\n",temp_path.c_str());
    if (mkdir(temp_path.c_str(),S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH) != 0) fprintf(stderr,"..mkdir for temp folder for results failed\n");

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
    fprintf(stderr,"Copying: %s to: %s\n",app_source.c_str(),app_destination.c_str());
    retval = boinc_copy(app_source.c_str(),app_destination.c_str());
    if (retval) {
       fprintf(stderr,"..Copying the app file to the working directory failed: error %i\n",retval);
       return retval;
    }

    // Unzip the app zip file
    std::string app_zip = slot_path + std::string("/") + app_file;
    fprintf(stderr,"Unzipping the app zip file: %s\n",app_zip.c_str());

    retval = boinc_zip(UNZIP_IT,app_zip.c_str(),slot_path);

    if (retval) {
       fprintf(stderr,"..Unzipping the app file failed\n");
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
    fprintf(stderr,"Copying the namelist files from: %s to: %s\n",wu_source.c_str(),wu_destination.c_str());
    retval = boinc_copy(wu_source.c_str(),wu_destination.c_str());
    if (retval) {
       fprintf(stderr,"..Copying the namelist files to the working directory failed\n");
       return retval;
    }

    // Unzip the namelist zip file
    fprintf(stderr,"Unzipping the namelist zip file: %s\n",namelist_zip.c_str());
    retval = boinc_zip(UNZIP_IT,namelist_zip.c_str(),slot_path);
    if (retval) {
       fprintf(stderr,"..Unzipping the namelist file failed\n");
       return retval;
    }
    // Remove the zip file
    else {
       std::remove(namelist_zip.c_str());
    }

	
    // Parse the fort.4 namelist for the filenames and variables
    std::string namelist_file = slot_path + std::string("/") + namelist;
    std::string namelist_line="",nss="",delimiter="=";
    std::ifstream namelist_filestream;

   // Check for the existence of the namelist
   if( !file_exists(namelist_file) ) {
      fprintf(stderr,"..The namelist file %s does not exist\n",namelist.c_str());
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
          ifsdata_file.erase(std::remove(ifsdata_file.begin(),ifsdata_file.end(),' '),ifsdata_file.end());
          fprintf(stderr,"ifsdata_file: %s\n",ifsdata_file.c_str());
       }
       else if (nss.str().find("IC_ANCIL_FILE") != std::string::npos) {
          ic_ancil_file = nss.str().substr(nss.str().find(delimiter)+1, nss.str().length()-1);
          // Remove any whitespace
          ic_ancil_file.erase(std::remove(ic_ancil_file.begin(),ic_ancil_file.end(),' '),ic_ancil_file.end());
          fprintf(stderr,"ic_ancil_file: %s\n",ic_ancil_file.c_str());
       }
       else if (nss.str().find("CLIMATE_DATA_FILE") != std::string::npos) {
          climate_data_file = nss.str().substr(nss.str().find(delimiter)+1, nss.str().length()-1);
          // Remove any whitespace
          climate_data_file.erase(std::remove(climate_data_file.begin(),climate_data_file.end(),' '),climate_data_file.end());
          fprintf(stderr,"climate_data_file: %s\n",climate_data_file.c_str());
       }
       else if (nss.str().find("HORIZ_RESOLUTION") != std::string::npos) {
          horiz_resolution = nss.str().substr(nss.str().find(delimiter)+1, nss.str().length()-1);
          // Remove any whitespace
          horiz_resolution.erase(std::remove(horiz_resolution.begin(),horiz_resolution.end(),' '),horiz_resolution.end());
          fprintf(stderr,"horiz_resolution: %s\n",horiz_resolution.c_str());
       }
       else if (nss.str().find("VERT_RESOLUTION") != std::string::npos) {
          vert_resolution = nss.str().substr(nss.str().find(delimiter)+1, nss.str().length()-1);
          // Remove any whitespace
          vert_resolution.erase(std::remove(vert_resolution.begin(),vert_resolution.end(),' '),vert_resolution.end());
          fprintf(stderr,"vert_resolution: %s\n",vert_resolution.c_str());
       }
       else if (nss.str().find("GRID_TYPE") != std::string::npos) {
          grid_type = nss.str().substr(nss.str().find(delimiter)+1, nss.str().length()-1);
          // Remove any whitespace
          grid_type.erase(std::remove(grid_type.begin(),grid_type.end(),' '),grid_type.end());
          fprintf(stderr,"grid_type: %s\n",grid_type.c_str());
       }
       else if (nss.str().find("UPLOAD_INTERVAL") != std::string::npos) {
          tmpstr1 = nss.str().substr(nss.str().find(delimiter)+1, nss.str().length()-1);
          // Remove any whitespace
          tmpstr1.erase(std::remove(tmpstr1.begin(),tmpstr1.end(),' '),tmpstr1.end());
          upload_interval=std::stoi(tmpstr1);
          fprintf(stderr,"upload_interval: %i\n",upload_interval);
       }
       else if (nss.str().find("UTSTEP") != std::string::npos) {
          tmpstr2 = nss.str().substr(nss.str().find(delimiter)+1, nss.str().length()-1);
          // Remove any whitespace
	  tmpstr2.erase(std::remove(tmpstr2.begin(),tmpstr2.end(),','),tmpstr2.end());
          tmpstr2.erase(std::remove(tmpstr2.begin(),tmpstr2.end(),' '),tmpstr2.end());
          timestep_interval = std::stoi(tmpstr2);
          fprintf(stderr,"utstep: %i\n",timestep_interval);
       }
       else if (nss.str().find("!NFRPOS") != std::string::npos) {
          tmpstr3 = nss.str().substr(nss.str().find(delimiter)+1, nss.str().length()-1);
          // Remove any whitespace and commas
          tmpstr3.erase(std::remove(tmpstr3.begin(),tmpstr3.end(),','),tmpstr3.end());
          tmpstr3.erase(std::remove(tmpstr3.begin(),tmpstr3.end(),' '),tmpstr3.end());
          ICM_file_interval = std::stoi(tmpstr3);
          fprintf(stderr,"nfrpos: %i\n",ICM_file_interval);
       }
    }
    namelist_filestream.close();


    // Process the ic_ancil_file:
    std::string ic_ancil_zip = slot_path + std::string("/") + ic_ancil_file + std::string(".zip");
	
    // For transfer downloading, BOINC renames download files to jf_HEXADECIMAL-NUMBER, these files
    // need to be renamed back to the original name
    // Get the name of the 'jf_' filename from a link within the ic_ancil_file
    std::string ic_ancil_source = get_tag(ic_ancil_zip);

    // Copy the IC ancils to working directory
    std::string ic_ancil_destination = ic_ancil_zip;
    fprintf(stderr,"Copying IC ancils from: %s to: %s\n",ic_ancil_source.c_str(),ic_ancil_destination.c_str());
    retval = boinc_copy(ic_ancil_source.c_str(),ic_ancil_destination.c_str());
    if (retval) {
       fprintf(stderr,"..Copying the IC ancils to the working directory failed\n");
       return retval;
    }

    // Unzip the IC ancils zip file
    fprintf(stderr,"Unzipping the IC ancils zip file: %s\n",ic_ancil_zip.c_str());
    retval = boinc_zip(UNZIP_IT,ic_ancil_zip.c_str(),slot_path);
    if (retval) {
       fprintf(stderr,"..Unzipping the IC ancils file failed\n");
       return retval;
    }
    // Remove the zip file
    else {
       std::remove(ic_ancil_zip.c_str());
    }


    // Process the ifsdata_file:
    // Make the ifsdata directory
    std::string ifsdata_folder = slot_path + std::string("/ifsdata");
    if (mkdir(ifsdata_folder.c_str(),S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH) != 0) fprintf(stderr,"..mkdir for ifsdata folder failed\n");

    // Get the name of the 'jf_' filename from a link within the ifsdata_file
    std::string ifsdata_source = get_tag(slot_path + std::string("/") + ifsdata_file + std::string(".zip"));

    // Copy the ifsdata_file to the working directory
    std::string ifsdata_destination = ifsdata_folder + std::string("/") + ifsdata_file + std::string(".zip");
    fprintf(stderr,"Copying the ifsdata_file from: %s to: %s\n",ifsdata_source.c_str(),ifsdata_destination.c_str());
    retval = boinc_copy(ifsdata_source.c_str(),ifsdata_destination.c_str());
    if (retval) {
       fprintf(stderr,"..Copying the ifsdata file to the working directory failed\n");
       return retval;
    }

    // Unzip the ifsdata_file zip file
    std::string ifsdata_zip = ifsdata_folder + std::string("/") + ifsdata_file + std::string(".zip");
    fprintf(stderr,"Unzipping the ifsdata_zip file: %s\n", ifsdata_zip.c_str());
    retval = boinc_zip(UNZIP_IT,ifsdata_zip.c_str(),ifsdata_folder + std::string("/"));
    if (retval) {
       fprintf(stderr,"..Unzipping the ifsdata_zip file failed\n");
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
                       fprintf(stderr,"..mkdir for the climate data folder failed\n");

    // Get the name of the 'jf_' filename from a link within the climate_data_file
    std::string climate_data_source = get_tag(slot_path + std::string("/") + climate_data_file + std::string(".zip"));

    // Copy the climate data file to working directory
    std::string climate_data_destination = climate_data_path + std::string("/") + climate_data_file + std::string(".zip");
    fprintf(stderr,"Copying the climate data file from: %s to: %s\n",climate_data_source.c_str(),climate_data_destination.c_str());
    retval = boinc_copy(climate_data_source.c_str(),climate_data_destination.c_str());
    if (retval) {
       fprintf(stderr,"..Copying the climate data file to the working directory failed\n");
       return retval;
    }	

    // Unzip the climate data zip file
    std::string climate_zip = climate_data_destination;
    fprintf(stderr,"Unzipping the climate data zip file: %s\n",climate_zip.c_str());
    retval = boinc_zip(UNZIP_IT,climate_zip.c_str(),climate_data_path);
    if (retval) {
       fprintf(stderr,"..Unzipping the climate data file failed\n");
       return retval;
    }
    // Remove the zip file
    else {
       std::remove(climate_zip.c_str());
    }

	
    // Set the environmental variables:
    // Set the OIFS_DUMMY_ACTION environmental variable, this controls what OpenIFS does if it goes into a dummy subroutine
    // Possible values are: 'quiet', 'verbose' or 'abort'
    std::string OIFS_var = std::string("OIFS_DUMMY_ACTION=abort");
    if (putenv((char *)OIFS_var.c_str())) {
      fprintf(stderr,"..Setting the OIFS_DUMMY_ACTION environmental variable failed\n");
      return 1;
    }
    pathvar = getenv("OIFS_DUMMY_ACTION");
    //fprintf(stderr,"The OIFS_DUMMY_ACTION environmental variable is: %s\n",pathvar);

    // Set the OMP_NUM_THREADS environmental variable, the number of threads
    std::string OMP_NUM_var = std::string("OMP_NUM_THREADS=") + nthreads;
    if (putenv((char *)OMP_NUM_var.c_str())) {
      fprintf(stderr,"..Setting the OMP_NUM_THREADS environmental variable failed\n");
      return 1;
    }
    pathvar = getenv("OMP_NUM_THREADS");
    //fprintf(stderr,"The OMP_NUM_THREADS environmental variable is: %s\n",pathvar);

    // Set the OMP_SCHEDULE environmental variable, this enforces static thread scheduling
    std::string OMP_SCHED_var = std::string("OMP_SCHEDULE=STATIC");
    if (putenv((char *)OMP_SCHED_var.c_str())) {
      fprintf(stderr,"..Setting the OMP_SCHEDULE environmental variable failed\n");
      return 1;
    }
    pathvar = getenv("OMP_SCHEDULE");
    //fprintf(stderr,"The OMP_SCHEDULE environmental variable is: %s\n",pathvar);

    // Set the DR_HOOK environmental variable, this controls the tracing facility in OpenIFS, off=0 and on=1
    std::string DR_HOOK_var = std::string("DR_HOOK=1");
    if (putenv((char *)DR_HOOK_var.c_str())) {
      fprintf(stderr,"..Setting the DR_HOOK environmental variable failed\n");
      return 1;
    }
    pathvar = getenv("DR_HOOK");
    //fprintf(stderr,"The DR_HOOK environmental variable is: %s\n",pathvar);

    // Set the DR_HOOK_HEAPCHECK environmental variable, this ensures the heap size statistics are reported
    std::string DR_HOOK_HEAP_var = std::string("DR_HOOK_HEAPCHECK=no");
    if (putenv((char *)DR_HOOK_HEAP_var.c_str())) {
      fprintf(stderr,"..Setting the DR_HOOK_HEAPCHECK environmental variable failed\n");
      return 1;
    }
    pathvar = getenv("DR_HOOK_HEAPCHECK");
    //fprintf(stderr,"The DR_HOOK_HEAPCHECK environmental variable is: %s\n",pathvar);

    // Set the DR_HOOK_STACKCHECK environmental variable, this ensures the stack size statistics are reported
    std::string DR_HOOK_STACK_var = std::string("DR_HOOK_STACKCHECK=no");
    if (putenv((char *)DR_HOOK_STACK_var.c_str())) {
      fprintf(stderr,"..Setting the DR_HOOK_STACKCHECK environmental variable failed\n");
      return 1;
    }
    pathvar = getenv("DR_HOOK_STACKCHECK");
    //fprintf(stderr, "The DR_HOOK_STACKCHECK environmental variable is: %s\n",pathvar);
	
    // Set the EC_MEMINFO environment variable, only applies to OpenIFS 43r3.
    // Disable EC_MEMINFO to remove the useless EC_MEMINFO messages to the stdout file to reduce filesize.
    std::string EC_MEMINFO = std::string("EC_MEMINFO=0");
    if (putenv((char *)EC_MEMINFO.c_str())) {
       fprintf(stderr,"..Setting the EC_MEMINFO environment variable failed\n");
       return 1;
    }
    pathvar = getenv("EC_MEMINFO");
    //fprintf(stderr, "The EC_MEMINFO environment variable is: %s\n, pathvar);

    // Set the OMP_STACKSIZE environmental variable, OpenIFS needs more stack memory per process
    std::string OMP_STACK_var = std::string("OMP_STACKSIZE=128M");
    if (putenv((char *)OMP_STACK_var.c_str())) {
      fprintf(stderr,"..Setting the OMP_STACKSIZE environmental variable failed\n");
      return 1;
    }
    pathvar = getenv("OMP_STACKSIZE");
    //fprintf(stderr,"The OMP_STACKSIZE environmental variable is: %s\n",pathvar);


    // Set the core dump size to 0
    struct rlimit core_limits;
    core_limits.rlim_cur = core_limits.rlim_max = 0;
    if (setrlimit(RLIMIT_CORE, &core_limits) != 0) fprintf(stderr,"..Setting the core dump size to 0 failed\n");

    // Set the stack limit to be unlimited
    struct rlimit stack_limits;
    // In macOS we cannot set the stack size limit to infinity
    #ifndef __APPLE__ // Linux
       stack_limits.rlim_cur = stack_limits.rlim_max = RLIM_INFINITY;
       if (setrlimit(RLIMIT_STACK, &stack_limits) != 0) fprintf(stderr,"..Setting the stack limit to unlimited failed\n");
    #endif

    int last_cpu_time, upload_file_number, last_upload, model_completed;
    std::string last_iter;

    // last_upload is the time of the last upload file (in seconds)

    // Define the name and location of the progress file
    std::string progress_file = temp_path+std::string("/progress_file_")+wuid+std::string(".xml");
    std::ofstream progress_file_out(progress_file);
    std::ifstream progress_file_in(progress_file);
    std::stringstream progress_file_buffer;
    xml_document<> doc;
	
    // Model progress is held in the progress file
    // First check if a file is not already present from an unscheduled shutdown
    if(file_exists(progress_file) && progress_file_in.tellg() > 0) {
       fprintf(stderr,"Located progress_file\n");
       // If present parse file and extract values
       progress_file_in.open(progress_file);
       progress_file_buffer << progress_file_in.rdbuf();
       progress_file_in.close();
	    
       // Parse XML progress file
       doc.parse<0>(&progress_file_buffer.str()[0]);
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

       fprintf(stderr,"last_cpu_time: %i\n",last_cpu_time);
       fprintf(stderr,"upload_file_number: %i\n",upload_file_number);
       fprintf(stderr,"last_iter: %s\n",last_iter.c_str());
       fprintf(stderr,"last_upload: %i\n",last_upload);
       fprintf(stderr,"model_completed: %i\n",model_completed);
    }
    else {
       fprintf(stderr,"progress_file not present, creating progress file\n");
       // Progress file not present, so create a progress file
       // Set the initial values
       last_cpu_time = 0;
       upload_file_number = 0;
       last_iter = "0";
       last_upload = 0;
       model_completed = 0;
	    
       // Write out the initial progress file	
       progress_file_out.open(progress_file);
       progress_file_out <<"<?xml version=\"1.0\" encoding=\"utf-8\"?>"<< std::endl;
       progress_file_out <<"<running_values>"<< std::endl;
       progress_file_out <<"  <last_cpu_time>"<<std::to_string(last_cpu_time)<<"</last_cpu_time>"<< std::endl;
       progress_file_out <<"  <upload_file_number>"<<std::to_string(upload_file_number)<<"</upload_file_number>"<< std::endl;
       progress_file_out <<"  <last_iter>"<<last_iter<<"</last_iter>"<< std::endl;
       progress_file_out <<"  <last_upload>"<<std::to_string(last_upload)<<"</last_upload>"<< std::endl;
       progress_file_out <<"  <model_completed>"<<std::to_string(model_completed)<<"</model_completed>"<< std::endl;
       progress_file_out <<"</running_values>"<< std::endl;
       progress_file_out.close();
    }
	
    fraction_done = 0;
    memset(result_base_name, 0x00, sizeof(char) * 64);

    // seconds between upload files: upload_interval
    // seconds between ICM files: ICM_file_interval * timestep_interval
    // upload interval in steps = upload_interval / timestep_interval
    //fprintf(stderr, "upload_interval, timestep_interval: %i, %i\n",upload_interval,timestep_interval);

    // Check if upload_interval x timestep_interval equal to zero
    if (upload_interval * timestep_interval == 0) {
       fprintf(stderr,"..upload_interval x timestep_interval equals zero\n");
       return 1;
    }

    int total_length_of_simulation = (int) (num_days * 86400);
    fprintf(stderr,"total_length_of_simulation: %i\n",total_length_of_simulation);

    // Get result_base_name to construct upload file names using 
    // the first upload as an example and then stripping off '_0.zip'
    if (!boinc_is_standalone()) {
       memset(strTmp,0x00,_MAX_PATH);
       retval = boinc_resolve_filename("upload_file_0.zip",strTmp,_MAX_PATH);
       //fprintf(stderr,"strTmp: %s\n",strTmp);
       strncpy(result_base_name, strip_path(strTmp), strlen(strip_path(strTmp))-6);
       fprintf(stderr,"result_base_name: %s\n",result_base_name);
       if (strcmp(result_base_name,"upload_file")==0) {
          fprintf(stderr,"..Failed to get result name\n");
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
          fprintf(stderr,"..Failed to open environment variables override file\n");
          return 1;
       }
       pclose(pipe);
    }	
	

    // Start the OpenIFS job
    std::string strCmd = slot_path + std::string("/oifs_43r3_model.exe");
    handleProcess = launch_process(slot_path,strCmd.c_str(),exptid.c_str(),app_name);
    if (handleProcess > 0) process_status = 0;

    boinc_end_critical_section();


    // process_status = 0 running
    // process_status = 1 stopped normally
    // process_status = 2 stopped with quit request from BOINC
    // process_status = 3 stopped with child process being killed
    // process_status = 4 stopped with child process being stopped


    // Main loop:	
    // Periodically check the process status and the BOINC client status
    while (process_status == 0 && model_completed == 0) {
       sleep_until(system_clock::now() + seconds(1));

       count++;

       // Check every 10 seconds whether an upload point has been reached
       if(count==10) {   
          if(!(ifs_stat_file.is_open())) {
             //fprintf(stderr,"Opening ifs.stat file\n");
             ifs_stat_file.open(slot_path + std::string("/ifs.stat"));
          }

          // Read last completed ICM file from ifs.stat file
          while(std::getline(ifs_stat_file, ifs_line)) {  //get 1 row as a string
             //fprintf(stderr,"Reading ifs.stat file\n");

             std::istringstream iss(ifs_line);  //put line into stringstream
             int ifs_word_count=0;
             // Read fourth column from file
             while(iss >> ifs_word) {  //read word by word
                ifs_word_count++;
                if (ifs_word_count==4) iter = ifs_word;
                //fprintf(stderr,"count: %i\n",ifs_word_count);
                //fprintf(stderr,"iter: %s\n",iter.c_str());
             }
          }

          // When the iteration number changes in the ifs.stat file, OpenIFS has completed writing
          // to the files for that iteration, those files can now be moved and uploaded
          //fprintf(stderr,"iter: %i\n",std::stoi(iter));
          //fprintf(stderr,"last_iter: %i\n",std::stoi(last_iter));

          // GC: Check for garbage in the retrieved string first, otherwise stoi will kill this process.
          if (!check_stoi(iter)) {
            fprintf(stderr,"Unable to update iter, resetting to last_iter.\n");
            iter = last_iter;
          }

          if (std::stoi(iter) != std::stoi(last_iter)) {
             // Construct file name of the ICM result file
             second_part = get_second_part(last_iter, exptid);

             // Move the ICMGG result file to the temporary folder in the project directory
             if(file_exists(slot_path+std::string("/ICMGG")+second_part)) {
                fprintf(stderr,"Moving to projects directory: %s\n",(slot_path+std::string("/ICMGG")+second_part).c_str());
                retval = boinc_copy((slot_path+std::string("/ICMGG")+second_part).c_str() , \
                                    (temp_path+std::string("/ICMGG")+second_part).c_str());
                if (retval) {
                   fprintf(stderr,"..Copying ICMGG result file to the temp folder in the projects directory failed\n");
                   return retval;
                }
                // If result file has been successfully copied over, remove it from slots directory
                else {
                   std::remove((slot_path+std::string("/ICMGG")+second_part).c_str());
                }
             }

             // Move the ICMSH result file to the temporary folder in the project directory
             if(file_exists(slot_path+std::string("/ICMSH")+second_part)) {
                fprintf(stderr,"Moving to projects directory: %s\n",(slot_path+std::string("/ICMSH")+second_part).c_str());
                retval = boinc_copy((slot_path+std::string("/ICMSH")+second_part).c_str() , \
                                    (temp_path+std::string("/ICMSH")+second_part).c_str());
                if (retval) {
                   fprintf(stderr,"..Copying ICMSH result file to the temp folder in the projects directory failed\n");
                   return retval;
                }
                // If result file has been successfully copied over, remove it from slots directory
                else {
                   std::remove((slot_path+std::string("/ICMSH")+second_part).c_str());
                }
             }

             // Move the ICMUA result file to the temporary folder in the project directory (this is for 43r3 and above only)
             if(file_exists(slot_path+std::string("/ICMUA")+second_part)) {
                fprintf(stderr,"Moving to projects directory: %s\n",(slot_path+std::string("/ICMUA")+second_part).c_str());
                retval = boinc_copy((slot_path+std::string("/ICMUA")+second_part).c_str() , \
                                    (temp_path+std::string("/ICMUA")+second_part).c_str());
                if (retval) {
                   fprintf(stderr,"..Copying ICMUA result file to the temp folder in the projects directory failed\n");
                   return retval;
                }
                // If result file has been successfully copied over, remove it from slots directory
                else {
                   std::remove((slot_path+std::string("/ICMUA")+second_part).c_str());
                }
             }
		  
             // Convert iteration number to seconds
             current_iter = (std::stoi(last_iter)) * timestep_interval;

             //fprintf(stderr,"Current iteration of model: %s\n",last_iter.c_str());
             //fprintf(stderr,"timestep_interval: %i\n",timestep_interval);
             //fprintf(stderr,"current_iter: %i\n",current_iter);
             //fprintf(stderr,"last_upload: %i\n",last_upload);

             // Upload a new upload file if the end of an upload_interval has been reached
             if((( current_iter - last_upload ) >= (upload_interval * timestep_interval)) && (current_iter < total_length_of_simulation)) {
                // Create an intermediate results zip file using BOINC zip
                zfl.clear();

                boinc_begin_critical_section();

                // Cycle through all the steps from the last upload to the current upload
                for (i = (last_upload / timestep_interval); i < (current_iter / timestep_interval); i++) {
                   //fprintf(stderr,"last_upload/timestep_interval: %i\n",(last_upload/timestep_interval));
                   //fprintf(stderr,"current_iter/timestep_interval: %i\n",(current_iter/timestep_interval));
                   //fprintf(stderr,"i: %s\n",(std::to_string(i)).c_str());

                   // Construct file name of the ICM result file
                   second_part = get_second_part(std::to_string(i), exptid);

                   // Add ICMGG result files to zip to be uploaded
                   if(file_exists(temp_path+std::string("/ICMGG")+second_part)) {
                      fprintf(stderr,"Adding to the zip: %s\n",(temp_path+std::string("/ICMGG")+second_part).c_str());
                      zfl.push_back(temp_path+std::string("/ICMGG")+second_part);
                      // Delete the file that has been added to the zip
                      // std::remove((temp_path+std::string("/ICMGG")+second_part).c_str());
                   }

                   // Add ICMSH result files to zip to be uploaded
                   if(file_exists(temp_path+std::string("/ICMSH")+second_part)) {
                      fprintf(stderr,"Adding to the zip: %s\n",(temp_path+std::string("/ICMSH")+second_part).c_str());
                      zfl.push_back(temp_path+std::string("/ICMSH")+second_part);
                      // Delete the file that has been added to the zip
                      // std::remove((temp_path+std::string("/ICMSH")+second_part).c_str());
                   }
		
                   // Add ICMUA result files to zip to be uploaded
                   if(file_exists(temp_path+std::string("/ICMUA")+second_part)) {
                      fprintf(stderr,"Adding to the zip: %s\n",(temp_path+std::string("/ICMUA")+second_part).c_str());
                      zfl.push_back(temp_path+std::string("/ICMUA")+second_part);
                      // Delete the file that has been added to the zip
                      // std::remove((temp_path+std::string("/ICMUA")+second_part).c_str());
                   }
                }

                // If running under a BOINC client
                if (!boinc_is_standalone()) {

                   if (zfl.size() > 0){

                      // Create the zipped upload file from the list of files added to zfl
                      memset(upload_file, 0x00, sizeof(upload_file));
                      std::sprintf(upload_file,"%s%s_%d.zip",project_path.c_str(),result_base_name,upload_file_number);

                      fprintf(stderr,"Zipping up the intermediate file: %s\n",upload_file);
                      retval = boinc_zip(ZIP_IT,upload_file,&zfl);

                      if (retval) {
                         fprintf(stderr,"..Zipping up the intermediate file failed\n");
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
                      fprintf(stderr,"Uploading the intermediate file: %s\n",upload_file_name.c_str());
                      sleep_until(system_clock::now() + seconds(20));
                      boinc_upload_file(upload_file_name);
                      retval = boinc_upload_status(upload_file_name);
                      if (!retval) {
                         fprintf(stderr,"Finished the upload of the intermediate file: %s\n",upload_file_name.c_str());
                      }
			
                      // Produce trickle
                      process_trickle(current_cpu_time,wu_name.c_str(),result_base_name,slot_path,current_iter);
                   }
                   last_upload = current_iter; 
                }

                // Else running in standalone
                else {
                   upload_file_name = app_name + std::string("_") + unique_member_id + std::string("_") + start_date + std::string("_") + \
                              std::to_string(num_days_trunc) + std::string("_") + batchid + std::string("_") + wuid + std::string("_") + \
                              std::to_string(upload_file_number) + std::string(".zip");
                   fprintf(stderr,"The current upload_file_name is: %s\n",upload_file_name.c_str());

                   // Create the zipped upload file from the list of files added to zfl
                   memset(upload_file, 0x00, sizeof(upload_file));
                   std::sprintf(upload_file,"%s%s",project_path.c_str(),upload_file_name.c_str());
                   if (zfl.size() > 0){
                      retval = boinc_zip(ZIP_IT,upload_file,&zfl);

                      if (retval) {
                         fprintf(stderr,"..Creating the zipped upload file failed\n");
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
		     
	           // Produce trickle
                   process_trickle(current_cpu_time,wu_name.c_str(),result_base_name,slot_path,current_iter);
                }
                boinc_end_critical_section();
                upload_file_number++;
             }
          }
          last_iter = iter;
          count = 0;
          // Closing ifs.stat file access
          ifs_stat_file.close();     
	       
          // Update the progress file	
          progress_file_out.open(progress_file);
          progress_file_out <<"<?xml version=\"1.0\" encoding=\"utf-8\"?>"<< std::endl;
          progress_file_out <<"<running_values>"<< std::endl;
          progress_file_out <<"  <last_cpu_time>"<<std::to_string(current_cpu_time)<<"</last_cpu_time>"<< std::endl;
          progress_file_out <<"  <upload_file_number>"<<std::to_string(upload_file_number)<<"</upload_file_number>"<< std::endl;
          progress_file_out <<"  <last_iter>"<<last_iter<<"</last_iter>"<< std::endl;
          progress_file_out <<"  <last_upload>"<<std::to_string(last_upload)<<"</last_upload>"<< std::endl;
          progress_file_out <<"  <model_completed>"<<std::to_string(model_completed)<<"</model_completed>"<< std::endl;
          progress_file_out <<"</running_values>"<< std::endl;
          progress_file_out.close();
       }
	    
       // Calculate current_cpu_time, only update if cpu_time returns a value
       if (cpu_time(handleProcess)) {
          current_cpu_time = last_cpu_time + cpu_time(handleProcess);
          //fprintf(stderr,"current_cpu_time: %1.5f\n",current_cpu_time);
       }
	       

      // GC: Calculate the fraction done
      total_nsteps = (num_days * 86400.0) / (double) timestep_interval;    // GC: this should match CUSTEP in fort.4. If it doesn't we have a problem
      fraction_done = model_frac_done( atof(iter.c_str()), total_nsteps, atoi(nthreads.c_str()) );
      //fprintf(stderr,"fraction done: %.6f\n", fraction_done);
     

      if (!boinc_is_standalone()) {
         // Provide the current cpu_time to the BOINC server (note: this is deprecated in BOINC)
         boinc_report_app_status(current_cpu_time,current_cpu_time,fraction_done);

         // Provide the fraction done to the BOINC client, 
         // this is necessary for the percentage bar on the client
         boinc_fraction_done(fraction_done);
	  
         // Check the status of the client if not in standalone mode     
         process_status = check_boinc_status(handleProcess,process_status);
       }
	
       // Check the status of the child process    
       process_status = check_child_status(handleProcess,process_status);
    }


    // Time delay to ensure final ICM are complete
    sleep_until(system_clock::now() + seconds(90));	

	
    // Check whether model completed successfully
    if(file_exists(slot_path + std::string("/ifs.stat"))) {
       if(!(ifs_stat_file.is_open())) {
          //fprintf(stderr,"Opening ifs.stat file\n");
          ifs_stat_file.open(slot_path + std::string("/ifs.stat"));
       }

       // Read last line from ifs.stat file
       while(std::getline(ifs_stat_file, ifs_line)) {  //get 1 row as a string
          //fprintf(stderr,"Reading ifs.stat file\n");

          std::istringstream iss2(ifs_line);  //put line into stringstream
          int ifs_word_count=0;
          // Read fourth column from file
          while(iss2 >> ifs_word) {  //read word by word
             ifs_word_count++;
             if (ifs_word_count==3) last_line = ifs_word;
             //fprintf(stderr,"count: %i\n",ifs_word_count);
             //fprintf(stderr,"last_line: %s\n",last_line.c_str());
          }
       }
       if (last_line!="CNT0") {
          fprintf(stderr,"..Failed, model did not complete successfully\n");
          return 1;
       }
    }
    // ifs.stat has not been produced, then model did not start
    else {
       fprintf(stderr,"..Failed, model did not start\n");
       return 1;	    
    }
	
	
    // Update model_completed
    model_completed = 1;

    // We need to handle the last ICM files
    // Construct final file name of the ICM result file
    second_part = get_second_part(last_iter, exptid);

    // Move the ICMGG result file to the temporary folder in the project directory
    if(file_exists(slot_path+std::string("/ICMGG")+second_part)) {
       fprintf(stderr,"Moving to projects directory: %s\n",(slot_path+std::string("/ICMGG")+second_part).c_str());
       retval = boinc_copy((slot_path+std::string("/ICMGG")+second_part).c_str() , \
                           (temp_path+std::string("/ICMGG")+second_part).c_str());
       if (retval) {
          fprintf(stderr,"..Copying ICMGG result file to the temp folder in the projects directory failed\n");
          return retval;
       }
       // If result file has been successfully copied over, remove it from slots directory
       else {
          std::remove((slot_path+std::string("/ICMGG")+second_part).c_str());
       }
    }

    // Move the ICMSH result file to the temporary folder in the project directory
    if(file_exists(slot_path+std::string("/ICMSH")+second_part)) {
       fprintf(stderr,"Moving to projects directory: %s\n",(slot_path+std::string("/ICMSH")+second_part).c_str());
       retval = boinc_copy((slot_path+std::string("/ICMSH")+second_part).c_str() , \
                           (temp_path+std::string("/ICMSH")+second_part).c_str());
       if (retval) {
          fprintf(stderr,"..Copying ICMSH result file to the temp folder in the projects directory failed\n");
          return retval;
       }
       // If result file has been successfully copied over, remove it from slots directory
       else {
          std::remove((slot_path+std::string("/ICMSH")+second_part).c_str());
       }
    }

    // Move the ICMUA result file to the temporary folder in the project directory (this is for 43r3 and above only)
    if(file_exists(slot_path+std::string("/ICMUA")+second_part)) {
       fprintf(stderr,"Moving to projects directory: %s\n",(slot_path+std::string("/ICMUA")+second_part).c_str());
       retval = boinc_copy((slot_path+std::string("/ICMUA")+second_part).c_str() , \
                           (temp_path+std::string("/ICMUA")+second_part).c_str());
       if (retval) {
          fprintf(stderr,"..Copying ICMUA result file to the temp folder in the projects directory failed\n");
	  return retval;
       }
       // If result file has been successfully copied over, remove it from slots directory
       else {
          std::remove((slot_path+std::string("/ICMUA")+second_part).c_str());
       }
    }
    
	    
    boinc_begin_critical_section();

    // Create the final results zip file

    zfl.clear();
    std::string node_file = slot_path + std::string("/NODE.001_01");
    zfl.push_back(node_file);
    std::string ifsstat_file = slot_path + std::string("/ifs.stat");
    zfl.push_back(ifsstat_file);

    // Read the remaining list of files from the slots directory and add the matching files to the list of files for the zip
    dirp = opendir(temp_path.c_str());
    if (dirp) {
        while ((dir = readdir(dirp)) != NULL) {
          //fprintf(stderr,"In temp folder: %s\n",dir->d_name);
          regcomp(&regex,"^[ICM+]",0);
          regcomp(&regex,"\\+",0);

          if (!regexec(&regex,dir->d_name,(size_t) 0,NULL,0)) {
            zfl.push_back(temp_path+std::string("/")+dir->d_name);
            fprintf(stderr,"Adding to the zip: %s\n",(temp_path+std::string("/")+dir->d_name).c_str());
          }
        }
        closedir(dirp);
    }

    // If running under a BOINC client
    if (!boinc_is_standalone()) {
       if (zfl.size() > 0){

          // Create the zipped upload file from the list of files added to zfl
          memset(upload_file, 0x00, sizeof(upload_file));
          std::sprintf(upload_file,"%s%s_%d.zip",project_path.c_str(),result_base_name,upload_file_number);

          fprintf(stderr,"Zipping up the final file: %s\n",upload_file);
          retval = boinc_zip(ZIP_IT,upload_file,&zfl);

          if (retval) {
             fprintf(stderr,"..Zipping up the final file failed\n");
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
          fprintf(stderr,"Uploading the final file: %s\n",upload_file_name.c_str());
          sleep_until(system_clock::now() + seconds(20));
          boinc_upload_file(upload_file_name);
          retval = boinc_upload_status(upload_file_name);
          if (!retval) {
             fprintf(stderr,"Finished the upload of the final file\n");
          }
	       
	  // Produce trickle
          process_trickle(current_cpu_time,wu_name.c_str(),result_base_name,slot_path,current_iter);
       }
       boinc_end_critical_section();
    }
    // Else running in standalone
    else {
       upload_file_name = app_name + std::string("_") + unique_member_id + std::string("_") + start_date + std::string("_") + \
                          std::to_string(num_days_trunc) + std::string("_") + batchid + std::string("_") + wuid + std::string("_") + \
                          std::to_string(upload_file_number) + std::string(".zip");
       fprintf(stderr,"The final upload_file_name is: %s\n",upload_file_name.c_str());

       // Create the zipped upload file from the list of files added to zfl
       memset(upload_file, 0x00, sizeof(upload_file));
       std::sprintf(upload_file,"%s%s",project_path.c_str(),upload_file_name.c_str());
       if (zfl.size() > 0){
          retval = boinc_zip(ZIP_IT,upload_file,&zfl);
          if (retval) {
             fprintf(stderr,"..Creating the zipped upload file failed\n");
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
        process_trickle(current_cpu_time,wu_name.c_str(),result_base_name,slot_path,current_iter);     
    }

    // Now task has finished, remove the progress file and temp folder
    std::remove(progress_file.c_str());
    std::remove(temp_path.c_str());

    sleep_until(system_clock::now() + seconds(120));

    // if finished normally
    if (process_status == 1){
      boinc_end_critical_section();
      boinc_finish(0);
      #ifdef __APPLE_CC__
         _exit(0);
      #else
         exit(0);
      #endif 
      return 0;
    }
    else if (process_status == 2){
      boinc_end_critical_section();
      boinc_finish(0);
      #ifdef __APPLE_CC__
         _exit(0);
      #else
         exit(0);
      #endif 
      return 0;
    }
    else {
      boinc_end_critical_section();
      boinc_finish(1);
      #ifdef __APPLE_CC__
         _exit(1);
      #else
         exit(1);
      #endif 
      return 1;
    }	
}



const char* strip_path(const char* path) {
    int jj;
    for (jj = (int) strlen(path);
    jj > 0 && path[jj-1] != '/' && path[jj-1] != '\\'; jj--);
    return (const char*) path+jj;
}


int check_child_status(long handleProcess, int process_status) {
    int stat;
    //fprintf(stderr,"waitpid: %i\n",waitpid(handleProcess,0,WNOHANG));

    // Check whether child processed has exited
    if (waitpid(handleProcess,&stat,WNOHANG)==-1) {
       process_status = 1;
       // Child exited normally
       if (WIFEXITED(stat)) {
	  process_status = 1;
          fprintf(stderr,"The child process terminated with status: %d\n",WEXITSTATUS(stat));
          fflush(stderr);
       }
       // Child process has exited
       else if (WIFSIGNALED(stat)) {
	  process_status = 3;  
          fprintf(stderr,"..The child process has been killed with signal: %d\n",WTERMSIG(stat));
          fflush(stderr);
       }
       // Child is stopped
       else if (WIFSTOPPED(stat)) {
	  process_status = 4;
          fprintf(stderr,"..The child process has stopped with signal: %d\n",WSTOPSIG(stat));
          fflush(stderr);
       }
    }
    return process_status;
}


int check_boinc_status(long handleProcess, int process_status) {
    BOINC_STATUS status;
    boinc_get_status(&status);

    // If a quit, abort or no heartbeat has been received from the BOINC client, end child process
    if (status.quit_request) {
       fprintf(stderr,"Quit request received from BOINC client, ending the child process\n");
       fflush(stderr);
       kill(handleProcess,SIGKILL);
       process_status = 2;
       return process_status;
    }
    else if (status.abort_request) {
       fprintf(stderr,"Abort request received from BOINC client, ending the child process\n");
       fflush(stderr);
       kill(handleProcess,SIGKILL);
       process_status = 1;
       return process_status;
    }
    else if (status.no_heartbeat) {
       fprintf(stderr,"No heartbeat received from BOINC client, ending the child process\n");
       fflush(stderr);
       kill(handleProcess,SIGKILL);
       process_status = 1;
       return process_status;
    }
    // Else if BOINC client is suspended, suspend child process and periodically check BOINC client status
    else {
       if (status.suspended) {
          fprintf(stderr,"Suspend request received from the BOINC client, suspending the child process\n");
          fflush(stderr);
          kill(handleProcess,SIGSTOP);

          while (status.suspended) {
             boinc_get_status(&status);
             if (status.quit_request) {
                fprintf(stderr,"Quit request received from the BOINC client, ending the child process\n");
                fflush(stderr);
                kill(handleProcess,SIGKILL);
                process_status = 2;
                return process_status;
             }
             else if (status.abort_request) {
                fprintf(stderr,"Abort request received from the BOINC client, ending the child process\n");
                fflush(stderr);
                kill(handleProcess,SIGKILL);
                process_status = 1;
                return process_status;
             }
             else if (status.no_heartbeat) {
                fprintf(stderr,"No heartbeat received from the BOINC client, ending the child process\n");
                fflush(stderr);
                kill(handleProcess,SIGKILL);
                process_status = 1;
                return process_status;
             }
             sleep_until(system_clock::now() + seconds(1));
          }
          // Resume child process
          fprintf(stderr,"Resuming the child process\n");
          fflush(stderr);
          kill(handleProcess,SIGCONT);
          process_status = 0;
       }
       return process_status;
    }
}

long launch_process(const char* slot_path,const char* strCmd,const char* exptid, const std::string app_name) {
    int retval = 0;
    long handleProcess;

    //fprintf(stderr,"slot_path: %s\n",slot_path);
    //fprintf(stderr,"strCmd: %s\n",strCmd);
    //fprintf(stderr,"exptid: %s\n",exptid);

    switch((handleProcess=fork())) {
       case -1: {
          fprintf(stderr,"..Unable to start a new child process\n");
          exit(0);
          break;
       }
       case 0: { //The child process
          char *pathvar;
          // Set the GRIB_SAMPLES_PATH environmental variable
          std::string GRIB_SAMPLES_var = std::string("GRIB_SAMPLES_PATH=") + slot_path + \
                                         std::string("/eccodes/ifs_samples/grib1_mlgrib2");
          if (putenv((char *)GRIB_SAMPLES_var.c_str())) {
            fprintf(stderr,"..Setting the GRIB_SAMPLES_PATH failed\n");
          }
          pathvar = getenv("GRIB_SAMPLES_PATH");
          //fprintf(stderr,"The GRIB_SAMPLES_PATH environmental variable is: %s\n",pathvar);

          // Set the GRIB_DEFINITION_PATH environmental variable
          std::string GRIB_DEF_var = std::string("GRIB_DEFINITION_PATH=") + slot_path + \
                                     std::string("/eccodes/definitions");
          if (putenv((char *)GRIB_DEF_var.c_str())) {
            fprintf(stderr,"..Setting the GRIB_DEFINITION_PATH failed\n");
          }
          pathvar = getenv("GRIB_DEFINITION_PATH");
          //fprintf(stderr,"The GRIB_DEFINITION_PATH environmental variable is: %s\n",pathvar);

          if((app_name=="openifs") || (app_name=="oifs_40r1")) { // OpenIFS 40r1
            fprintf(stderr,"Executing the command: %s -e %s\n",strCmd,exptid);
            retval = execl(strCmd,strCmd,"-e",exptid,NULL);
          }
          else {  // OpenIFS 43r3 and above
            fprintf(stderr,"Executing the command: %s\n",strCmd);
            retval = execl(strCmd,strCmd,NULL,NULL,NULL);
          }

          // If execl returns then there was an error
          fprintf(stderr,"..The execl() command failed slot_path=%s,strCmd=%s,exptid=%s\n",slot_path,strCmd,exptid);
          fflush(stderr);
          exit(retval);
          break;
       }
       default: 
          fprintf(stderr,"The child process has been launched with process id: %ld\n",handleProcess);
          fflush(stderr);
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
void process_trickle(double current_cpu_time,const char* wu_name,const char* result_name,const char* slot_path,int timestep) {
    char* trickle = new char[512];

    //fprintf(stderr,"current_cpu_time: %f\n",current_cpu_time);
    //fprintf(stderr,"wu_name: %s\n",wu_name);
    //fprintf(stderr,"result_name: %s\n",result_name);
    //fprintf(stderr,"slot_path: %s\n",slot_path);
    //fprintf(stderr,"timestep: %d\n",timestep);

    std::sprintf(trickle, "<wu>%s</wu>\n<result>%s</result>\n<ph></ph>\n<ts>%d</ts>\n<cp>%ld</cp>\n<vr></vr>\n",\
                           wu_name,result_name, timestep,(long) current_cpu_time);
    //fprintf(stderr,"Contents of trickle: %s\n",trickle);

    // Upload the trickle if not in standalone mode
    if (!boinc_is_standalone()) {
       fprintf(stderr,"Uploading trickle at timestep: %d\n",timestep);

       boinc_send_trickle_up((char*) "orig",(char*) trickle);
    }

    // Write out the trickle in standalone mode
    else {
       char trickle_name[_MAX_PATH];
       std::sprintf(trickle_name,"%s/trickle_%lu.xml",slot_path,(unsigned long) time(NULL));

       fprintf(stderr,"Writing trickle to: %s\n",trickle_name);

       FILE* trickle_file = boinc_fopen(trickle_name,"w");
       if (trickle_file) {
          fwrite(trickle, 1, strlen(trickle), trickle_file);
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
      second_part = exptid +"+"+ "0000" + last_iter;
   }
   else if (last_iter.length() == 3) {
      second_part = exptid +"+"+ "000" + last_iter;
   }
   else if (last_iter.length() == 4) {
      second_part = exptid +"+"+ "00" + last_iter;
   }
   else if (last_iter.length() == 5) {
      second_part = exptid +"+"+ "0" + last_iter;
   }
   else if (last_iter.length() == 6) {
      second_part = exptid +"+"+ last_iter;
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
        cerr << "Invalid characters in stoi string: " << cin << "\n";
        return false;
    }

    //  check stoi standard exceptions
    //  n.b. still need to check step <= max_step
    try {
        step = std::stoi(cin);
        //cerr << "step converted is : " << step << "\n";
        return true;
    }
    catch (const std::invalid_argument &excep) {
        cerr << "Invalid input argument for stoi : " << excep.what() << "\n";
        return false;
    }
    catch (const std::out_of_range &excep) {
        cerr << "Out of range value for stoi : " << excep.what() << "\n";
        return false;
    }
}
