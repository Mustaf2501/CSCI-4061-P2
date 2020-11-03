#include "mapreduce.h"

struct mymsg_t{
  long mtype;
  char mtext[1024];
};


// execute executables using execvp
void execute(char **argv, int nProcesses){
	pid_t  pid;

	int i;
	for (i = 0; i < nProcesses; i++){
		pid = fork();
		if (pid < 0) {
			printf("ERROR: forking child process failed\n");
			exit(1);
		} else if (pid == 0) {
			char *processID = (char *) malloc(sizeof(char) * 5); // memory leak
			sprintf(processID, "%d", i+1);
			argv[1] = processID;
			if (execvp(*argv, argv) < 0) {
				printf("ERROR: exec failed\n");
				exit(1);
			}
		}
     }
}



int main(int argc, char *argv[]) {
	
	if(argc < 4) {
		printf("Less number of arguments.\n");
		printf("./mapreduce #mappers #reducers inputFile\n");
		exit(0);
	}

	int nMappers 	= strtol(argv[1], NULL, 10);
	int nReducers 	= strtol(argv[2], NULL, 10);

	if(nMappers < nReducers){
		printf("ERROR: Number of mappers should be greater than or equal to number of reducers...\n");
		exit(0);
	}

	if(nMappers == 0 || nReducers == 0){
		printf("ERROR: Mapper and Reducer count should be grater than zero...\n");
		exit(0);
	}
	
	char *inputFile = argv[3];

	bookeepingCode();

	int status;
	pid_t pid = fork();
	if(pid == 0){
		//send chunks of data to the mappers in RR fashion
		sendChunkData(inputFile, nMappers);
		exit(0);
	}
	sleep(1);

	// spawn mappers
	char *mapperArgv[] = {"./mapper", NULL, NULL};
	execute(mapperArgv, nMappers);

	// wait for all children to complete execution
    while (wait(&status) > 0);

  key_t key = ftok("./ftok.txt", 4061);
  int  mid = msgget(key,0666|IPC_CREAT);
   
  if(mid == -1){
    perror("Failed to get Queue ID");
    exit(0); 
  }

  int delq1 = msgctl(mid, IPC_RMID, NULL); // delete queue after sendChunkData
   
  if(delq1 == -1){
    perror("Failed delete Queue");
    exit(0); 
  }

   
 	pid = fork();
	if(pid == 0){
		shuffle(nMappers, nReducers);
		exit(0);
	}
	sleep(1);

  
	// spawn reducers
	char *reducerArgv[] = {"./reducer", NULL, NULL};
	execute(reducerArgv, nReducers);

	// wait for all children to complete execution
  while (wait(&status) > 0);
   

 // close the Queue
 key = ftok("./ftok.txt", 4061);
 mid = msgget(key,0666|IPC_CREAT);
  
  if(mid == -1){
    perror("Failed to get Queue ID");
    exit(0); 
  }

 

  int delq2 = msgctl(mid, IPC_RMID, NULL); // final deletion of queue 

  if (delq2 == -1){
    perror("Failed to get Queue ID");
    exit(0); 
  }

   
  if(mid == -1){
    perror("Failed to get Queue ID");
    exit(0); 
  }


	return 0;
}