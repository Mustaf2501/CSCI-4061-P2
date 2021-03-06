#include "utils.h"


struct mymsg_t{
  long mtype;
  char mtext[1024];
};


char *getChunkData(int mapperID) {
  
  key_t key = ftok("./ftok.txt", 4061); // create key 
 
  int mid = msgget(key,0666|IPC_CREAT); // get queue from key
  
  if (mid == -1){
   perror("Failed to create message queue\n");
   exit(0);
  }

  struct mymsg_t chunk;

  memset((void *)chunk.mtext, '\0',1024); // blank out chunk 

  int check = msgrcv(mid,(void *)&chunk, 1024, mapperID, 0); // recieve from Queue 

  if (check  == (ssize_t)-1){
    perror("Failed to recieve message\n");
    exit(0);
  }

  char*c = malloc(sizeof(chunk.mtext)); 
  strcpy(c, chunk.mtext); // copy from recieved to c
  
  if (strcmp(c,"END") == 0){
        return NULL;
    }
   
    return c;
}

// sends chunks of size 1024 to the mappers in RR fashion
void sendChunkData(char *inputFile, int nMappers) {
   key_t key =ftok("./ftok.txt", 4061); 
  


  int totbytes = 0; // used to see if we've gone past 1024 bytes
  int newbytes;  // used to see how many bytes the next word is 

  char word [100]; // used to store the next word in the file 

  int mapperid = 1; // start the mapperid at 1 and increment to n

  struct mymsg_t chunk; // holds mapperid and chunk 

  
  int mid = msgget(key, 0666|IPC_CREAT);
  
  if(mid == -1){
    perror("Failed to get Queue ID\n");
    exit(0); 
  }

  
  FILE * f = fopen(inputFile, "r"); 
  
  memset((void *)chunk.mtext, '\0',1024); // blank out chunk 
 
 // go through file a single word at a time 
 // the next word is stored in word, above. 
  while(fscanf(f,"%s",word) !=EOF ){
    
    
    // word now holds the next word from the file 
    newbytes = strlen(word); // store size of word 

    if (totbytes+newbytes+1 <= 1024){ // 
      // underflow case 
      
       // add to totalbytes 
       totbytes = totbytes + newbytes + 1; // +1 for space character
       // ...

    }
    else{

      // overflow case 

      // set chunk id to the current mapperid
      chunk.mtype = mapperid; 

      // reset totalbytes, since we've exceed 1024 
      totbytes = newbytes + 1; 

      // use system calls to send chunk to Queue
      int check = msgsnd(mid, (void *)&chunk,sizeof(chunk.mtext),0); 
      if (check == -1){
        perror("Message send failed\n");
        exit(0);
      }
       memset(chunk.mtext, '\0', 1024); 

      // increment mapperid ; if it is n then set it to 1.
      mapperid = mapperid +1 ;
      if(mapperid > nMappers){
        mapperid = 1;
      }

    }

    strcat(chunk.mtext, strcat(word, " "));  // add word to chunk
    
  }
  
  // must send last bytes to Queue 
  if(totbytes > 0){
      
       // set chunk id to the current mapperid
      chunk.mtype = mapperid; 
      //add new word to chunk
      //strcat(chunk.mtext, strcat(word," ")); 
      // use system calls to send chunk to Queue
      int check1 = msgsnd(mid, (void *)&chunk,sizeof(chunk.mtext),0);
      if (check1 == -1){
        perror("Message send failed\n");
        exit(0);
      }
      // wipe chunk with memset
      memset(chunk.mtext, '\0', 1024); 
  }

  // create END message 
  memset(chunk.mtext, '\0', 1024); 
  strcat(chunk.mtext, "END"); 
  
  // send END message to every mapper by associating id of the mapper with the message 
  for(int i =1 ; i < nMappers + 1; i++){
      chunk.mtype = i;
      int check2 = msgsnd(mid, (void *)&chunk,sizeof(chunk.mtext),0) ;
      if(check2== -1){
        perror("Message send failed\n");
        exit(0);
      }

  }

  fclose(f); 
}


// hash function to divide the list of word.txt files across reducers
//http://www.cse.yorku.ca/~oz/hash.html
int hashFunction(char* key, int reducers){
	unsigned long hash = 0;
    int c;

    while ((c = *key++)!='\0')
        hash = c + (hash << 6) + (hash << 16) - hash;

    return (hash % reducers);
}

int getInterData(char *key, int reducerID) {
  key_t key1 = ftok("./ftok.txt", 4061);
  
  
  int  mid = msgget(key1,0666|IPC_CREAT);
  if(mid == -1){
     perror("Failed to get Queue ID\n");
    exit(0);

  }

  struct mymsg_t chunk; // holds recieved message


  memset((void *)chunk.mtext, '\0',1024); // blank out chunk
  int rcv = msgrcv(mid,(void *)&chunk, 1024, reducerID, 0);

  if (rcv == -1){
    perror("Failed to get recieve from Queue\n");
    exit(0);

  }
   char*c = malloc(sizeof(chunk.mtext)); // allocate space for chunk
   strcpy(c, chunk.mtext); // copy chunk into allocated space

   strcpy(key, c);// copy recieved chunk to key
  
  if (strcmp(key,"END") == 0){
        return 0;
    }
    
  return 1; 
}

void shuffle(int nMappers, int nReducers) {
  key_t key = ftok("./ftok.txt", 4061);  
   

   struct mymsg_t chunk;
  
  int reducerID;
  int mid = mid = msgget(key,0666|IPC_CREAT);
  if (mid == -1){
   perror("Failed to create message queue\n");
   exit(0);
  }
 
  struct dirent* entry;
   
  for(int i=1; i<nMappers+1; i++) {
     
    char path[50] = "output/MapOut/Map_"; // path of file 
    char strnum[5];
    
    sprintf(strnum,"%d",i);
    strcat(path,strnum); // add number
      
    DIR* dir = opendir(path);
    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
     
      if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))    continue;

       struct mymsg_t filechunk;

      memset(filechunk.mtext, '\0', 1024);
      // create and add file's path
      char filepath[50] =""; 
      strcpy(filepath, path); 
      strcat(filepath, "/");
      strcat(filepath, entry->d_name); 
      strcat(filechunk.mtext, filepath); 
  
      reducerID = hashFunction(entry->d_name, nReducers);

      filechunk.mtype = reducerID+1;

      int check3 = msgsnd(mid, (void *)&filechunk,sizeof //send file to a reducer chosen by hash function
      (filechunk.mtext),0);
      if (check3 == -1){
        perror("Message send failed\n");
        exit(0);
      } 
  }

  }
  

    // create END message 
  memset(chunk.mtext, '\0', 1024);
  strcat(chunk.mtext, "END"); 
  
  // send END message to every mapper by associating id of the mapper with the message 
 
  for(int i =1 ; i < nReducers + 1; i++){
    
      chunk.mtype = i;
      msgsnd(mid, (void *)&chunk,sizeof(chunk.mtext),0);

      
  }
   
}

// check if the character is valid for a word
int validChar(char c){
	return (tolower(c) >= 'a' && tolower(c) <='z') ||
					(c >= '0' && c <= '9');
}

char *getWord(char *chunk, int *i){
	char *buffer = (char *)malloc(sizeof(char) * chunkSize);
	memset(buffer, '\0', chunkSize);
	int j = 0;
	while((*i) < strlen(chunk)) {
		// read a single word at a time from chunk
		// printf("%d\n", i);
		if (chunk[(*i)] == '\n' || chunk[(*i)] == ' ' || !validChar(chunk[(*i)]) || chunk[(*i)] == 0x0) {
			buffer[j] = '\0';
			if(strlen(buffer) > 0){
				(*i)++;
				return buffer;
			}
			j = 0;
			(*i)++;
			continue;
		}
		buffer[j] = chunk[(*i)];
		j++;
		(*i)++;
	}
	if(strlen(buffer) > 0)
		return buffer;
	return NULL;
}

void createOutputDir(){
	mkdir("output", ACCESSPERMS);
	mkdir("output/MapOut", ACCESSPERMS);
	mkdir("output/ReduceOut", ACCESSPERMS);
}

char *createMapDir(int mapperID){
	char *dirName = (char *) malloc(sizeof(char) * 100);
	memset(dirName, '\0', 100);
	sprintf(dirName, "output/MapOut/Map_%d", mapperID);
	mkdir(dirName, ACCESSPERMS);
	return dirName;
}

void removeOutputDir(){
	pid_t pid = fork();
	if(pid == 0){
		char *argv[] = {"rm", "-rf", "output", NULL};
		if (execvp(*argv, argv) < 0) {
			printf("ERROR: exec failed\n");
			exit(1);
		}
		exit(0);
	} else{
		wait(NULL);
	}
}

void bookeepingCode(){
	removeOutputDir();
	sleep(1);
	createOutputDir();
}