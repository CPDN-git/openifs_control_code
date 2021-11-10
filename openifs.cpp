//
// Control code for the OpenIFS application in the climateprediction.net project
//
// Written by Andy Bowery (Oxford eResearch Centre, Oxford University) October 2021
//

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <chrono>
#include <thread>
#include <fstream>
#include <iostream>
#include <exception>
#include <dirent.h> 
#include <regex.h>
#include <sys/wait.h>
#include <string>
#include <sstream>
#include "./boinc/api/boinc_api.h"
#include "./boinc/zip/boinc_zip.h"
#include <signal.h>
#include <fcntl.h>

#ifndef _MAX_PATH
   #define _MAX_PATH 512
#endif

const char* stripPath(const char* path);
int checkChildStatus(long,int);
int checkBOINCStatus(long,int);
long launchProcess(const char*,const char*,const char*,const std::string);
std::string getTag(const std::string &str);
void process_trickle(double,const char*,const char*,const char*,int);
bool file_exists(const std::string &str);

using namespace std::chrono;
using namespace std::this_thread;
using namespace std;

int main(int argc, char** argv) {
    std::string IFSDATA_FILE,IC_ANCIL_FILE,CLIMATE_DATA_FILE,GRID_TYPE,TSTEP,NFRPOS,project_path,result_name,wu_name,version;
    int HORIZ_RESOLUTION,VERT_RESOLUTION,upload_interval,timestep_interval,ICM_file_interval,process_status,retval=0,i,j;
    char* strFind[9] = {NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL};
    char strCpy[9][_MAX_PATH],strTmp[_MAX_PATH];
    char *pathvar;
    long handleProcess;
    double tv_sec,tv_usec,cpu_time,fraction_done;
    float time_per_fclen;
    struct dirent *dir;
    struct rusage usage;
    regex_t regex;
    DIR *dirp;

    // Set defaults for input arguments
    std::string OIFS_EXPID;           // model experiment id, must match string in filenames
    int NTHREADS=1;                   // default number ofexi OPENMP threads
    std::string NAMELIST="fort.4";    // NAMELIST file, this name is fixed

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
    fflush(stderr);

    // Read the exptid, batchid, version, wuid from the command line
    std::string start_date = argv[1];
    std::string exptid = argv[2];
    std::string unique_member_id = argv[3];
    std::string batchid = argv[4];
    std::string wuid = argv[5];
    std::string fclen = argv[6];
    std::string app_name = argv[7];
	
    OIFS_EXPID = exptid;
    wu_name = dataBOINC.wu_name;

    // Get the slots path (the current working path)
    char slot_path[_MAX_PATH];
    if (getcwd(slot_path, sizeof(slot_path)) == NULL)
      fprintf(stderr,"..getcwd returned an error\n");
    else
      fprintf(stderr,"Current working directory is: %s\n",slot_path);

    if (!boinc_is_standalone()) {

      // Get the project path
      project_path = dataBOINC.project_dir + std::string("/");
      fprintf(stderr,"Current project directory is: %s\n",project_path.c_str());
	    
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
	    
      fprintf(stderr,"The current app is: %s\n",app_name.c_str());
      fprintf(stderr,"The current version is: %s\n",version.c_str());
      fprintf(stderr,"The current result_name is: %s\n",result_name.c_str());
    }
    // Running in standalone
    else {
      fprintf(stderr,"Running in standalone mode\n");
      // Set the project path
      project_path = slot_path + std::string("/../projects/");
      fprintf(stderr,"Current project directory is: %s\n",project_path.c_str());
	    
      // Get the app version and result name
      version = argv[8];
      fprintf(stderr,"The current app is: %s\n",app_name.c_str());
      fprintf(stderr,"(argv8) app_version: %s\n",argv[8]);
    }

    boinc_begin_critical_section();

    // Create temporary folder for moving the results to and uploading the results from
    // BOINC measures the disk usage on the slots directory so we must move all results out of this folder
    std::string temp_path = project_path + app_name + std::string("_") + wuid;
    fprintf(stderr,"Location of temp folder: %s\n",temp_path.c_str());
    if (mkdir(temp_path.c_str(),S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH) != 0) fprintf(stderr,"..mkdir for temp folder for results failed\n");

    // macOS
    #ifdef __APPLE__
       std::string app_file = app_name + std::string("_app_") + version + std::string("_x86_64-apple-darwin.zip");
    // Linux
    #else
       std::string app_file = app_name + std::string("_app_") + version + std::string("_x86_64-pc-linux-gnu.zip");
    #endif

    // Copy the app file to the working directory
    std::string app_target = project_path + app_file;
    std::string app_destination = slot_path + std::string("/") + app_file;
    fprintf(stderr,"Copying: %s to: %s\n",app_target.c_str(),app_destination.c_str());
    retval = boinc_copy(app_target.c_str(),app_destination.c_str());
    if (retval) {
       fprintf(stderr,"..Copying the app file to the working directory failed\n");
       return retval;
    }

    // Unzip the app zip file
    std::string app_zip = slot_path + std::string("/") + app_file;
    fprintf(stderr,"Unzipping the app zip file: %s\n",app_zip.c_str());
    fflush(stderr);

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
    // Get the name of the 'jf_' filename from a link within the namelist file
    std::string wu_target = getTag(slot_path + std::string("/") + app_name + std::string("_") + unique_member_id + std::string("_") + start_date +\
                      std::string("_") + fclen + std::string("_") + batchid + std::string("_") + wuid + std::string(".zip"));

    // Copy the namelist files to the working directory
    std::string wu_destination = slot_path + std::string("/") + app_name + std::string("_") + unique_member_id + std::string("_") + start_date +\
                      std::string("_") + fclen + std::string("_") + batchid + std::string("_") + wuid + std::string(".zip");
    fprintf(stderr,"Copying the namelist files from: %s to: %s\n",wu_target.c_str(),wu_destination.c_str());

    retval = boinc_copy(wu_target.c_str(),wu_destination.c_str());
    if (retval) {
       fprintf(stderr,"..Copying the namelist files to the working directory failed\n");
       return retval;
    }

    // Unzip the namelist zip file
    std::string namelist_zip = slot_path + std::string("/") + app_name + std::string("_") + unique_member_id + std::string("_") + start_date +\
                      std::string("_") + fclen + std::string("_") + batchid + std::string("_") + wuid + std::string(".zip");
    fprintf(stderr,"Unzipping the namelist zip file: %s\n",namelist_zip.c_str());
    fflush(stderr);
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
    std::string namelist_file = slot_path + std::string("/") + NAMELIST;
    const char strSearch[9][22]={"!IFSDATA_FILE=","!IC_ANCIL_FILE=","!CLIMATE_DATA_FILE=","!HORIZ_RESOLUTION=",\
                                 "!VERT_RESOLUTION=","!GRID_TYPE=","!UPLOAD_INTERVAL=","TSTEP=","NFRPOS="};
    memset(strCpy,0x00,9*_MAX_PATH);
    memset(strTmp,0x00,_MAX_PATH);
    FILE* fParse = boinc_fopen(namelist_file.c_str(),"r");

    // Read the namelist file
    if (!fParse) {
       fprintf(stderr,"..Opening the namelist file to read failed\n");
       return 1;
    }
    else {
       // Start at the top of the file
       fseek(fParse,0x00,SEEK_SET);
       memset(strTmp,0x00,_MAX_PATH);
       while (!feof(fParse)) {
           memset(strTmp,0x00,_MAX_PATH);
           fgets(strTmp,_MAX_PATH-1,fParse);
           if (!strFind[0]) {
               strFind[0] = strstr(strTmp,strSearch[0]);
               if (strFind[0]) {
                   strcpy(strCpy[0],strFind[0]);
               }
            }
            if (!strFind[1]) {
                strFind[1] = strstr(strTmp,strSearch[1]);
                if (strFind[1]) {
                    strcpy(strCpy[1],strFind[1]);
		}
            }
            if (!strFind[2]) {
                strFind[2] = strstr(strTmp,strSearch[2]);
                if (strFind[2]) {
                    strcpy(strCpy[2],strFind[2]);
                }
            }
            if (!strFind[3]) {
                strFind[3] = strstr(strTmp,strSearch[3]);
                if (strFind[3]) {
                    strcpy(strCpy[3],strFind[3]);
                }
            }
            if (!strFind[4]) {
                strFind[4] = strstr(strTmp,strSearch[4]);
                if (strFind[4]) {
                    strcpy(strCpy[4],strFind[4]);
                }
            }
            if (!strFind[5]) {
                strFind[5] = strstr(strTmp,strSearch[5]);
                if (strFind[5]) {
                    strcpy(strCpy[5],strFind[5]);
                }
            }
            if (!strFind[6]) {
                strFind[6] = strstr(strTmp,strSearch[6]);
                if (strFind[6]) {
                    strcpy(strCpy[6],strFind[6]);
                }
            }
            if (!strFind[7]) {
                strFind[7] = strstr(strTmp,strSearch[7]);
                if (strFind[7]) {
                    strcpy(strCpy[7],strFind[7]);
                }
            }
            if (!strFind[8]) {
                strFind[8] = strstr(strTmp,strSearch[8]);
                if (strFind[8]) {
                    strcpy(strCpy[8],strFind[8]);
                }
            }
            if (strFind[0]&&strFind[1]&&strFind[2]&&strFind[3]&&strFind[4]&&strFind[5]&&strFind[6]&&strFind[7]&&strFind[8]) {
                break;
            }
       }
       // Either feof or we hit the string		
       if (strCpy[0][0] != 0x00) {
            memset(strTmp,0x00,_MAX_PATH);
            strncpy(strTmp,(char*)(strCpy[0] + strlen(strSearch[0])),100);
            IFSDATA_FILE = strTmp;
            // Handle any white space in tags
            while(!IFSDATA_FILE.empty() && \
                  std::isspace(*IFSDATA_FILE.rbegin())) IFSDATA_FILE.erase(IFSDATA_FILE.length()-1);
            fprintf(stderr,"IFSDATA_FILE: %s\n",IFSDATA_FILE.c_str());
       }
       if (strCpy[1][0] != 0x00) {
            memset(strTmp,0x00,_MAX_PATH);
            strncpy(strTmp,(char*)(strCpy[1] + strlen(strSearch[1])),100);
            IC_ANCIL_FILE = strTmp; 
            while(!IC_ANCIL_FILE.empty() && \
                  std::isspace(*IC_ANCIL_FILE.rbegin())) IC_ANCIL_FILE.erase(IC_ANCIL_FILE.length()-1);
            fprintf(stderr,"IC_ANCIL_FILE: %s\n",IC_ANCIL_FILE.c_str());
       }
       if (strCpy[2][0] != 0x00) {
            memset(strTmp,0x00,_MAX_PATH);
            strncpy(strTmp,(char*)(strCpy[2] + strlen(strSearch[2])),100);
            CLIMATE_DATA_FILE = strTmp; 
            while(!CLIMATE_DATA_FILE.empty() && \
                  std::isspace(*CLIMATE_DATA_FILE.rbegin())) CLIMATE_DATA_FILE.erase(CLIMATE_DATA_FILE.length()-1);
            fprintf(stderr,"CLIMATE_DATA_FILE: %s\n",CLIMATE_DATA_FILE.c_str());
       }
       if (strCpy[3][0] != 0x00) {
            HORIZ_RESOLUTION=atoi(strCpy[3] + strlen(strSearch[3]));
            fprintf(stderr,"HORIZ_RESOLUTION: %i\n",HORIZ_RESOLUTION);
       }
       if (strCpy[4][0] != 0x00) {
            VERT_RESOLUTION=atoi(strCpy[4] + strlen(strSearch[4]));
            fprintf(stderr,"VERT_RESOLUTION: %i\n",VERT_RESOLUTION);
       }
       if (strCpy[5][0] != 0x00) {
            memset(strTmp,0x00,_MAX_PATH);
            strncpy(strTmp,(char*)(strCpy[5] + strlen(strSearch[5])),100);
            GRID_TYPE = strTmp; 
            while(!GRID_TYPE.empty() && std::isspace(*GRID_TYPE.rbegin())) GRID_TYPE.erase(GRID_TYPE.length()-1);
            fprintf(stderr,"GRID_TYPE: %s\n",GRID_TYPE.c_str());
       }
       if (strCpy[6][0] != 0x00) {
            upload_interval=atoi(strCpy[6] + strlen(strSearch[6]));
            fprintf(stderr,"UPLOAD_INTERVAL: %i\n",upload_interval);
       }
       if (strCpy[7][0] != 0x00) {
            memset(strTmp,0x00,_MAX_PATH);
            strncpy(strTmp,(char*)(strCpy[7] + strlen(strSearch[7])),100);
            TSTEP = strTmp; 
            while(!TSTEP.empty() && \
                  std::isspace(*TSTEP.rbegin())) TSTEP.erase(TSTEP.length()-1);
            // Remove the trailing comma
            if (!TSTEP.empty()) TSTEP.resize(TSTEP.size() - 1);
            fprintf(stderr,"TSTEP: %s\n",TSTEP.c_str());
            // Convert to an integer
            timestep_interval = std::stoi(TSTEP);
       }
       if (strCpy[8][0] != 0x00) {
            memset(strTmp,0x00,_MAX_PATH);
            strncpy(strTmp,(char*)(strCpy[8] + strlen(strSearch[8])),100);
            NFRPOS = strTmp; 
            while(!NFRPOS.empty() && \
                  std::isspace(*NFRPOS.rbegin())) NFRPOS.erase(NFRPOS.length()-1);
            // Remove the trailing comma
            if (!NFRPOS.empty()) NFRPOS.resize(NFRPOS.size() - 1);
            fprintf(stderr,"NFRPOS: %s\n",NFRPOS.c_str());
            // Convert to an integer
            ICM_file_interval = std::stoi(NFRPOS);
       }
       fclose(fParse);
    }


    // Process the IC_ANCIL_FILE:
    // For transfer downloading, BOINC renames download files to jf_HEXDECIMAL-NUMBER, these files
    // need to be renamed back to the original name
    // Get the name of the 'jf_' filename from a link within the IC_ANCIL_FILE
    std::string ic_ancil_target = getTag(slot_path + std::string("/") + IC_ANCIL_FILE + std::string(".zip"));

    // Copy the IC ancils to working directory
    std::string ic_ancil_destination = slot_path + std::string("/") + IC_ANCIL_FILE + std::string(".zip");
    fprintf(stderr,"Copying IC ancils from: %s to: %s\n",ic_ancil_target.c_str(),ic_ancil_destination.c_str());
    retval = boinc_copy(ic_ancil_target.c_str(),ic_ancil_destination.c_str());
    if (retval) {
       fprintf(stderr,"..Copying the IC ancils to the working directory failed\n");
       return retval;
    }

    // Unzip the IC ancils zip file
    std::string ic_ancil_zip = slot_path + std::string("/") + IC_ANCIL_FILE + std::string(".zip");
    fprintf(stderr,"Unzipping the IC ancils zip file: %s\n",ic_ancil_zip.c_str());
    fflush(stderr);
    retval = boinc_zip(UNZIP_IT,ic_ancil_zip.c_str(),slot_path);
    if (retval) {
       fprintf(stderr,"..Unzipping the IC ancils file failed\n");
       return retval;
    }
    // Remove the zip file
    else {
       std::remove(ic_ancil_zip.c_str());
    }


    // Process the IFSDATA_FILE:
    // Make the ifsdata directory
    std::string ifsdata_folder = slot_path + std::string("/ifsdata");
    if (mkdir(ifsdata_folder.c_str(),S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH) != 0) fprintf(stderr,"..mkdir for ifsdata folder failed\n");

    // Get the name of the 'jf_' filename from a link within the IFSDATA_FILE
    std::string ifsdata_target = getTag(slot_path + std::string("/") + IFSDATA_FILE + std::string(".zip"));

    // Copy the IFSDATA_FILE to working directory
    std::string ifsdata_destination = slot_path + std::string("/ifsdata/") + IFSDATA_FILE + std::string(".zip");
    fprintf(stderr,"Copying IFSDATA_FILE from: %s to: %s\n",ifsdata_target.c_str(),ifsdata_destination.c_str());
    retval = boinc_copy(ifsdata_target.c_str(),ifsdata_destination.c_str());
    if (retval) {
       fprintf(stderr,"..Copying the IFSDATA file to the working directory failed\n");
       return retval;
    }

    // Unzip the IFSDATA_FILE zip file
    std::string ifsdata_zip = slot_path + std::string("/ifsdata/") + IFSDATA_FILE + std::string(".zip");
    fprintf(stderr,"Unzipping IFSDATA_FILE zip file: %s\n", ifsdata_zip.c_str());
    fflush(stderr);
    retval = boinc_zip(UNZIP_IT,ifsdata_zip.c_str(),slot_path+std::string("/ifsdata/"));
    if (retval) {
       fprintf(stderr,"..Unzipping the IFSDATA file failed\n");
       return retval;
    }
    // Remove the zip file
    else {
       std::remove(ifsdata_zip.c_str());
    }


    // Process the CLIMATE_DATA_FILE:
    // Make the climate data directory
    std::string climate_data_path = slot_path + std::string("/") + \
                       std::to_string(HORIZ_RESOLUTION) + std::string(GRID_TYPE);
    if (mkdir(climate_data_path.c_str(),S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH) != 0) \
                       fprintf(stderr,"..mkdir for the climate data folder failed\n");

    // Get the name of the 'jf_' filename from a link within the CLIMATE_DATA_FILE
    std::string climate_data_target = getTag(slot_path + std::string("/") + CLIMATE_DATA_FILE + std::string(".zip"));

    // Copy the climate data file to working directory
    std::string climate_data_destination = slot_path + std::string("/") + \
                                           std::to_string(HORIZ_RESOLUTION) + std::string(GRID_TYPE) + \
                                           std::string("/") + CLIMATE_DATA_FILE + std::string(".zip");
    fprintf(stderr,"Copying the climate data file from: %s to: %s\n",climate_data_target.c_str(),climate_data_destination.c_str());
    retval = boinc_copy(climate_data_target.c_str(),climate_data_destination.c_str());
    if (retval) {
       fprintf(stderr,"..Copying the climate data file to the working directory failed\n");
       return retval;
    }	

    // Unzip the climate data zip file
    std::string climate_zip = slot_path + std::string("/") + \
                              std::to_string(HORIZ_RESOLUTION) + std::string(GRID_TYPE) + \
                              std::string("/") + CLIMATE_DATA_FILE + std::string(".zip");
    fprintf(stderr,"Unzipping the climate data zip file: %s\n",climate_zip.c_str());
    fflush(stderr);
    retval = boinc_zip(UNZIP_IT,climate_zip.c_str(),\
                       slot_path+std::string("/")+std::to_string(HORIZ_RESOLUTION)+std::string(GRID_TYPE));
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
    fprintf(stderr,"The OIFS_DUMMY_ACTION environmental variable is: %s\n",pathvar);

    // Set the OMP_NUM_THREADS environmental variable, the number of threads
    std::string OMP_NUM_var = std::string("OMP_NUM_THREADS=") + std::to_string(NTHREADS);
    if (putenv((char *)OMP_NUM_var.c_str())) {
      fprintf(stderr,"..Setting the OMP_NUM_THREADS environmental variable failed\n");
      return 1;
    }
    pathvar = getenv("OMP_NUM_THREADS");
    fprintf(stderr,"The OMP_NUM_THREADS environmental variable is: %s\n",pathvar);

    // Set the OMP_SCHEDULE environmental variable, this enforces static thread scheduling
    std::string OMP_SCHED_var = std::string("OMP_SCHEDULE=STATIC");
    if (putenv((char *)OMP_SCHED_var.c_str())) {
      fprintf(stderr,"..Setting the OMP_SCHEDULE environmental variable failed\n");
      return 1;
    }
    pathvar = getenv("OMP_SCHEDULE");
    fprintf(stderr,"The OMP_SCHEDULE environmental variable is: %s\n",pathvar);

    // Set the DR_HOOK environmental variable, this controls the tracing facility in OpenIFS, off=0 and on=1
    std::string DR_HOOK_var = std::string("DR_HOOK=1");
    if (putenv((char *)DR_HOOK_var.c_str())) {
      fprintf(stderr,"..Setting the DR_HOOK environmental variable failed\n");
      return 1;
    }
    pathvar = getenv("DR_HOOK");
    fprintf(stderr,"The DR_HOOK environmental variable is: %s\n",pathvar);

    // Set the DR_HOOK_HEAPCHECK environmental variable, this ensures the heap size statistics are reported
    std::string DR_HOOK_HEAP_var = std::string("DR_HOOK_HEAPCHECK=no");
    if (putenv((char *)DR_HOOK_HEAP_var.c_str())) {
      fprintf(stderr,"..Setting the DR_HOOK_HEAPCHECK environmental variable failed\n");
      return 1;
    }
    pathvar = getenv("DR_HOOK_HEAPCHECK");
    fprintf(stderr,"The DR_HOOK_HEAPCHECK environmental variable is: %s\n",pathvar);

    // Set the DR_HOOK_STACKCHECK environmental variable, this ensures the stack size statistics are reported
    std::string DR_HOOK_STACK_var = std::string("DR_HOOK_STACKCHECK=no");
    if (putenv((char *)DR_HOOK_STACK_var.c_str())) {
      fprintf(stderr,"..Setting the DR_HOOK_STACKCHECK environmental variable failed\n");
      return 1;
    }
    pathvar = getenv("DR_HOOK_STACKCHECK");
    fprintf(stderr, "The DR_HOOK_STACKCHECK environmental variable is: %s\n",pathvar);

    // Set the OMP_STACKSIZE environmental variable, OpenIFS needs more stack memory per process
    std::string OMP_STACK_var = std::string("OMP_STACKSIZE=128M");
    if (putenv((char *)OMP_STACK_var.c_str())) {
      fprintf(stderr,"..Setting the OMP_STACKSIZE environmental variable failed\n");
      return 1;
    }
    pathvar = getenv("OMP_STACKSIZE");
    fprintf(stderr,"The OMP_STACKSIZE environmental variable is: %s\n",pathvar);

	
    // Check for the existence of the namelist
    struct stat buffer;
    if(NAMELIST != "fort.4") {
       fprintf(stderr,"The namelist file path is: %s\n",namelist_file.c_str());
       fflush(stderr);
       if (stat((char *)namelist_file.c_str(),&buffer) < 0){
          fprintf(stderr,"..The namelist file %s does not exist\n",NAMELIST.c_str());
       }
       // Rename the namelist file to fort.4
       std::string namelist_target = namelist_file;
       std::string namelist_destination = slot_path + std::string("/fort.4");
       fprintf(stderr,"Renaming the namelist file from: %s to: %s\n",namelist_target.c_str(),namelist_destination.c_str());
       retval = boinc_copy(namelist_target.c_str(),namelist_destination.c_str());
       if (retval) {
          fprintf(stderr,"..Renaming the namelist file failed\n");
          return retval;
       }
    }
    if (stat((char *)namelist_file.c_str(), &buffer) < 0) {
       fprintf(stderr,"..The namelist file %s does not exist\n",NAMELIST.c_str());
    }


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


    cpu_time = 0;
    fraction_done = 0;
    time_per_fclen = 0.27;	

    ZipFileList zfl;
    std::string ifs_line, iter, last_iter, upload_file_name, ifs_word, second_part;
    int current_iter=0, count=0, upload_file_number = 0;
    std::ifstream ifs_stat_file;
    char upload_file[_MAX_PATH];
    char result_base_name[64]; 
    memset(result_base_name, 0x00, sizeof(char) * 64);

    // seconds between upload files: upload_interval
    // seconds between ICM files: ICM_file_interval * timestep_interval
    // upload interval in steps = upload_interval / timestep_interval

    //fprintf(stderr,"upload_interval x timestep_interval: %i\n",(upload_interval * timestep_interval));

    // Check if upload_interval x timestep_interval equal to zero
    if (upload_interval * timestep_interval == 0) {
       fprintf(stderr,"..upload_interval x timestep_interval equals zero\n");
       return 1;
    }

    // time of the last upload file (in seconds)
    int last_upload = 0;

    int total_length_of_simulation = std::stoi(fclen) * 86400;
    //fprintf(stderr,"total_length_of_simulation: %i\n",total_length_of_simulation);

    // Get result_base_name to construct upload file names using 
    // the first upload as an example and then stripping off '_1.zip'
    if (!boinc_is_standalone()) {
       memset(strTmp,0x00,_MAX_PATH);
       retval = boinc_resolve_filename("upload_file_1.zip",strTmp,_MAX_PATH);
       //fprintf(stderr,"strTmp: %s\n",strTmp);
       strncpy(result_base_name, stripPath(strTmp), strlen(stripPath(strTmp))-6);
       //fprintf(stderr,"result_base_name: %s\n",result_base_name);
       if (retval) {
          fprintf(stderr,"..Failed to get result name\n");
          return 1;
       }
    }


    // Start the OpenIFS job
    std::string strCmd = slot_path + std::string("/./master.exe");
    handleProcess = launchProcess(slot_path,strCmd.c_str(),exptid.c_str(),app_name);
    if (handleProcess > 0) process_status = 0;

    boinc_end_critical_section();


    int total_count = 0;
    last_iter = "0";

    // process_status = 0 running
    // process_status = 1 stopped normally
    // process_status = 2 stopped with quit request from BOINC
    // process_status = 3 stopped with child process being killed
    // process_status = 4 stopped with child process being stopped


    // Periodically check the process status and the BOINC client status
    while (process_status == 0) {
       sleep_until(system_clock::now() + seconds(1));

       count++;
       total_count++;

       // Check every 60 seconds whether an upload point has been reached
       if(count==60) {   
          if(!(ifs_stat_file.is_open())) {
             //fprintf(stderr,"Opening ifs.stat file\n");
             ifs_stat_file.open(slot_path + std::string("/ifs.stat"));
          }

          // Read last completed ICM file from ifs.stat file
          while(std::getline(ifs_stat_file, ifs_line)) {  //get 1 row as a string
             //fprintf(stderr,"Reading ifs.stat file\n");
             //fflush(stderr);

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
          //fprintf(stderr,"iter: %i\n",stoi(iter));
          //fprintf(stderr,"last_iter: %i\n",stoi(last_iter));

          if (stoi(iter) != stoi(last_iter)) {
             // Construct file name of the ICM result file
             second_part = "";
             if (last_iter.length() == 1) {
                second_part = "00000" + last_iter;
             }
             else if (last_iter.length() == 2) {
                second_part = "0000" + last_iter;
             }
             else if (last_iter.length() == 3) {
                second_part = "000" + last_iter;
             }
             else if (last_iter.length() == 4) {
                second_part = "00" + last_iter;
             }
             else if (last_iter.length() == 5) {
                second_part = "0" + last_iter;
             }
             else if (last_iter.length() == 6) {
                second_part = last_iter;
             }

             // Move the ICMGG result file to the temporary folder in the project directory
             if(file_exists(slot_path+std::string("/ICMGG")+exptid+"+"+second_part)) {
                fprintf(stderr,"Moving to projects directory: %s\n",(slot_path+std::string("/ICMGG")+exptid+"+"+second_part).c_str());
                retval = boinc_copy((slot_path+std::string("/ICMGG")+exptid+"+"+second_part).c_str() , \
                                    (temp_path+std::string("/ICMGG")+exptid+"+"+second_part).c_str());
                if (retval) {
                   fprintf(stderr,"..Copying ICMGG result file to the temp folder in the projects directory failed\n");
                   return retval;
                }
                // If result file has been successfully copied over, remove it from slots directory
                else {
                   std::remove((slot_path+std::string("/ICMGG")+exptid+"+"+second_part).c_str());
                }
             }

             // Move the ICMSH result file to the temporary folder in the project directory
             if(file_exists(slot_path+std::string("/ICMSH")+exptid+"+"+second_part)) {
                fprintf(stderr,"Moving to projects directory: %s\n",(slot_path+std::string("/ICMSH")+exptid+"+"+second_part).c_str());
                retval = boinc_copy((slot_path+std::string("/ICMSH")+exptid+"+"+second_part).c_str() , \
                                    (temp_path+std::string("/ICMSH")+exptid+"+"+second_part).c_str());
                if (retval) {
                   fprintf(stderr,"..Copying ICMSH result file to the temp folder in the projects directory failed\n");
                   return retval;
                }
                // If result file has been successfully copied over, remove it from slots directory
                else {
                   std::remove((slot_path+std::string("/ICMSH")+exptid+"+"+second_part).c_str());
                }
             }

             // Move the ICMUA result file to the temporary folder in the project directory (this is for 43r3 and above only)
             if(file_exists(slot_path+std::string("/ICMUA")+exptid+"+"+second_part)) {
                fprintf(stderr,"Moving to projects directory: %s\n",(slot_path+std::string("/ICMUA")+exptid+"+"+second_part).c_str());
                retval = boinc_copy((slot_path+std::string("/ICMUA")+exptid+"+"+second_part).c_str() , \
                                    (temp_path+std::string("/ICMUA")+exptid+"+"+second_part).c_str());
                if (retval) {
                   fprintf(stderr,"..Copying ICMUA result file to the temp folder in the projects directory failed\n");
                   return retval;
                }
                // If result file has been successfully copied over, remove it from slots directory
                else {
                   std::remove((slot_path+std::string("/ICMUA")+exptid+"+"+second_part).c_str());
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

                //fprintf(stderr,"total_count: %d\n",total_count);

                boinc_begin_critical_section();

                // Cycle through all the steps from the last upload to the current upload
                for (i = (last_upload / timestep_interval); i < (current_iter / timestep_interval); i++) {
                   //fprintf(stderr,"last_upload/timestep_interval: %i\n",(last_upload/timestep_interval));
                   //fprintf(stderr,"current_iter/timestep_interval: %i\n",(current_iter/timestep_interval));
                   //fprintf(stderr,"i: %s\n",(std::to_string(i)).c_str());

                   // Construct file name of the ICM result file
                   second_part = "";
                   if (std::to_string(i).length() == 1) {
                      second_part = "00000" + std::to_string(i);
                   }
                   else if (std::to_string(i).length() == 2) {
                      second_part = "0000" + std::to_string(i);
                   }
                   else if (std::to_string(i).length() == 3) {
                      second_part = "000" + std::to_string(i);
                   }
                   else if (std::to_string(i).length() == 4) {
                      second_part = "00" + std::to_string(i);
                   }
                   else if (std::to_string(i).length() == 5) {
                      second_part = "0" + std::to_string(i);
                   }
                   else if (std::to_string(i).length() == 6) {
                      second_part = std::to_string(i);
                   }

                   // Add ICMGG result files to zip to be uploaded
                   if(file_exists(temp_path+std::string("/ICMGG")+exptid+"+"+second_part)) {
                      fprintf(stderr,"Adding to the zip: %s\n",(temp_path+std::string("/ICMGG")+exptid+"+"+second_part).c_str());
                      zfl.push_back(temp_path+std::string("/ICMGG")+exptid+"+"+second_part);
                      // Delete the file that has been added to the zip
                   //   std::remove((temp_path+std::string("/ICMGG")+exptid+"+"+second_part).c_str());
                   }

                   // Add ICMSH result files to zip to be uploaded
                   if(file_exists(temp_path+std::string("/ICMSH")+exptid+"+"+second_part)) {
                      fprintf(stderr,"Adding to the zip: %s\n",(temp_path+std::string("/ICMSH")+exptid+"+"+second_part).c_str());
                      zfl.push_back(temp_path+std::string("/ICMSH")+exptid+"+"+second_part);
                      // Delete the file that has been added to the zip
                   //   std::remove((temp_path+std::string("/ICMSH")+exptid+"+"+second_part).c_str());
                   }
		
                   // Add ICMUA result files to zip to be uploaded
                   if(file_exists(temp_path+std::string("/ICMUA")+exptid+"+"+second_part)) {
                      fprintf(stderr,"Adding to the zip: %s\n",(temp_path+std::string("/ICMUA")+exptid+"+"+second_part).c_str());
                      zfl.push_back(temp_path+std::string("/ICMUA")+exptid+"+"+second_part);
                      // Delete the file that has been added to the zip
                   //   std::remove((temp_path+std::string("/ICMUA")+exptid+"+"+second_part).c_str());
                   }
                }

                // If running under a BOINC client
                if (!boinc_is_standalone()) {

                   if (zfl.size() > 0){

                      // Create the zipped upload file from the list of files added to zfl
                      memset(upload_file, 0x00, sizeof(upload_file));
                      std::sprintf(upload_file,"%s%s_%d.zip",project_path.c_str(),result_base_name,upload_file_number);

                      fprintf(stderr,"Zipping up file: %s\n",upload_file);
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
                   
                      // Upload the file. In BOINC the upload file is the logical name, not the physical name
                      upload_file_name = std::string("upload_file_") + std::to_string(upload_file_number) + std::string(".zip");
                      fprintf(stderr,"Uploading file: %s\n",upload_file_name.c_str());
                      fflush(stderr);
                      sleep_until(system_clock::now() + seconds(20));
                      boinc_upload_file(upload_file_name);
                      retval = boinc_upload_status(upload_file_name);
                      if (retval) {
                         fprintf(stderr,"Finished the upload of the result file: %s\n",upload_file_name.c_str());
                         fflush(stderr);
                      }
			
                      // Produce trickle
                      process_trickle(cpu_time,wu_name.c_str(),result_base_name,slot_path,current_iter);
                   }
                   last_upload = current_iter; 
                }

                // Else running in standalone
                else {
                   upload_file_name = app_name + std::string("_") + unique_member_id + std::string("_") + start_date + std::string("_") + \
                              fclen + std::string("_") + batchid + std::string("_") + wuid + std::string("_") + \
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
                   process_trickle(cpu_time,wu_name.c_str(),result_base_name,slot_path,current_iter);
                }
                boinc_end_critical_section();
                upload_file_number++;
             }
          }
          last_iter = iter;
          count = 0;
          // Closing ifs.stat file access
          ifs_stat_file.close();
       }


       // Calculate the fraction done
       getrusage(RUSAGE_SELF,&usage); //Return resource usage measurement
       tv_sec = usage.ru_utime.tv_sec; //Time spent executing in user mode (seconds)
       tv_usec = usage.ru_utime.tv_usec; //Time spent executing in user mode (microseconds)
       cpu_time = tv_sec+(tv_usec/1000000); //Convert to seconds
       fraction_done = (cpu_time-0.96)/(time_per_fclen*atoi(fclen.c_str()));

       //fprintf(stderr,"tv_sec: %.5f\n",tv_sec);
       //fprintf(stderr,"tv_usec: %.5f\n",(tv_usec/1000000));
       //fprintf(stderr,"cpu_time: %1.5f\n",cpu_time);
       //fprintf(stderr,"fraction_done: %.6f\n",fraction_done);

       // Provide the fraction done to the BOINC client, 
       // this is necessary for the percentage bar on the client
       boinc_fraction_done(fraction_done);
	    
       process_status = checkChildStatus(handleProcess,process_status);
       process_status = checkBOINCStatus(handleProcess,process_status);
    }

    // Time delay to ensure final ICM are complete
    sleep_until(system_clock::now() + seconds(90));

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

          fprintf(stderr,"Zipping up file: %s\n",upload_file);
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

          // Upload the file. In BOINC the upload file is the logical name, not the physical name
          upload_file_name = std::string("upload_file_") + std::to_string(upload_file_number) + std::string(".zip");
          fprintf(stderr,"Uploading file: %s\n",upload_file_name.c_str());
          fflush(stderr);
          sleep_until(system_clock::now() + seconds(20));
          boinc_upload_file(upload_file_name);
          retval = boinc_upload_status(upload_file_name);
          if (!retval) {
             fprintf(stderr,"Finished the upload of the result file\n");
             fflush(stderr);
          }
	       
	  // Produce trickle
          process_trickle(cpu_time,wu_name.c_str(),result_base_name,slot_path,current_iter);
       }
       boinc_end_critical_section();
       last_upload = current_iter;
    }
    // Else running in standalone
    else {
       upload_file_name = app_name + std::string("_") + unique_member_id + std::string("_") + start_date + std::string("_") + \
                          fclen + std::string("_") + batchid + std::string("_") + wuid + std::string("_") + \
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
        process_trickle(cpu_time,wu_name.c_str(),result_base_name,slot_path,current_iter);     
    }

    // Remove the temp folder
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



const char* stripPath(const char* path) {
    int jj;
    for (jj = (int) strlen(path);
    jj > 0 && path[jj-1] != '/' && path[jj-1] != '\\'; jj--);
    return (const char*) path+jj;
}


int checkChildStatus(long handleProcess, int process_status) {
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


int checkBOINCStatus(long handleProcess, int process_status) {
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

long launchProcess(const char* slot_path,const char* strCmd,const char* exptid, const std::string app_name) {
    int retval = 0;
    long handleProcess;

    fprintf(stderr,"slot_path: %s\n",slot_path);
    fprintf(stderr,"strCmd: %s\n",strCmd);
    fprintf(stderr,"exptid: %s\n",exptid);
    fflush(stderr);

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
          fprintf(stderr,"The GRIB_SAMPLES_PATH environmental variable is: %s\n",pathvar);

          // Set the GRIB_DEFINITION_PATH environmental variable
          std::string GRIB_DEF_var = std::string("GRIB_DEFINITION_PATH=") + slot_path + \
                                     std::string("/eccodes/definitions");
          if (putenv((char *)GRIB_DEF_var.c_str())) {
            fprintf(stderr,"..Setting the GRIB_DEFINITION_PATH failed\n");
          }
          pathvar = getenv("GRIB_DEFINITION_PATH");
          fprintf(stderr,"The GRIB_DEFINITION_PATH environmental variable is: %s\n",pathvar);

	  fprintf(stderr,"Executing the command: %s\n",strCmd);
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
std::string getTag(const std::string &filename) {
    std::ifstream file(filename);
    if (file.is_open()) {
       std::string line;
       while (getline(file, line)) {
          std::string::size_type start = line.find('>');
          if (start != line.npos) {
             std::string::size_type end = line.find('<', start + 1);
             if (end != line.npos) {
                ++start;
                std::string::size_type count = end - start;
                return line.substr(start, count);
             }
          }
          return "";
       }
       file.close();
    }
    return "";
}

// Produce the trickle and either upload to the project server or as a physical file
void process_trickle(double cpu_time,const char* wu_name,const char* result_name,const char* slot_path,int timestep) {
    char* trickle = new char[512];

    fprintf(stderr,"cpu_time: %f\n",cpu_time);
    fprintf(stderr,"wu_name: %s\n",wu_name);
    fprintf(stderr,"result_name: %s\n",result_name);
    fprintf(stderr,"slot_path: %s\n",slot_path);
    fprintf(stderr,"timestep: %d\n",timestep);

    std::sprintf(trickle, "<wu>%s</wu>\n<result>%s</result>\n<ph></ph>\n<ts>%d</ts>\n<cp>%ld</cp>\n<vr></vr>\n",\
                           wu_name,result_name, timestep,(long) cpu_time);
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
