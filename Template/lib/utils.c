#include "utils.h"
 
struct mymsg_t{
  long mtype;
  char mtext[1024];
};

char *getChunkData(int mapperID) {
  key_t key = ftok("P2", 4061);  
  //key_t k =101;
  int  mid = msgget(key,0666|IPC_CREAT);
  struct mymsg_t chunk;

  memset((void *)chunk.mtext, '\0',1024); // blank out chunk 

  msgrcv(mid,(void *)&chunk, 1024, mapperID, 0);
  char*c = malloc(sizeof(chunk.mtext)); 
  strcpy(c, chunk.mtext);
  
  if (strcmp(c,"END") == 0){
        printf("END MESSAGE RECIEVED : mapperid %d \n", mapperID);  
        return NULL;
    }
    printf("Check\n");
    return c;
}

void sendChunkData(char *inputFile, int nMappers) {
   key_t key = ftok("P2", 4061);  
  //printf("ENter sendChunk\n");
  //key_t key = 101; // One key for one Queue? Or n keys for n Queues??

  int totbytes = 0; // used to see if we've gone past 1024 bytes
  int newbytes;  // used to see how many bytes the next word is 

  char word [100]; // used to store the next word in the file 

  int mapperid = 1; // start the mapperid at 1 and increment to n

  struct mymsg_t chunk; // holds mapperid and chunk 
  int mid = msgget(key, 0666|IPC_CREAT);

  
  FILE * f = fopen(inputFile, "r"); 
  
  memset((void *)chunk.mtext, '\0',1024); // blank out chunk 
  //int mid = msgget(key, 0666|IPC_CREAT);
 
 // go through file a single word at a time 
 // the next word is stored in word, above. 
  while(fscanf(f,"%39s",word) !=EOF ){
    
    
    // word now holds the next word from the file 
    newbytes = strlen(word); // store size of word 

    //printf("%s %d\n", word,newbytes);
    if (totbytes+newbytes+1 <= 1024){ // 
      // underflow case 
      
      strcat(chunk.mtext, strcat(word, " "));  // add word to chunk   
      
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
       
      msgsnd(mid, (void *)&chunk,sizeof(chunk.mtext),0);

      // wipe chunk with memset
      memset(chunk.mtext, '\0', 1024); 
      
      //add new word to chunk
      strcat(chunk.mtext, strcat(word," ")); 
     
  
      // increment mapperid ; if it is n then set it to 1.
      mapperid = mapperid +1 ;
      if(mapperid > nMappers){
        mapperid = 1;
      }

    }
    
  }
  
  // must send last bytes to Queue 
  if(totbytes > 0){
       // set chunk id to the current mapperid
      chunk.mtype = mapperid; 
      //add new word to chunk
      strcat(chunk.mtext, strcat(word," ")); 
      // use system calls to send chunk to Queue
      msgsnd(mid, (void *)&chunk,sizeof(chunk.mtext),0);
      // wipe chunk with memset
      memset(chunk.mtext, '\0', 1024); 
  }
  // create END message 
  memset(chunk.mtext, '\0', 1024); 
  strcat(chunk.mtext, "END"); 
  
  // send END message to every mapper by associating id of the mapper with the message 
  for(int i =1 ; i < nMappers + 1; i++){
      chunk.mtype = i;
      msgsnd(mid, (void *)&chunk,sizeof(chunk.mtext),0);

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
  key_t key1 = ftok("P2", 4061);
  //key_t k =101;
  int  mid = msgget(key1,0666|IPC_CREAT);
  struct mymsg_t chunk;

  memset((void *)chunk.mtext, '\0',1024); // blank out chunk
 // printf("After memset\n");
  ssize_t e = msgrcv(mid,(void *)chunk.mtext, 1024, reducerID, 0);
  //printf("ERRNUM :%ld \n", e);
  strcpy(key, chunk.mtext);
  printf("test %s\n",chunk.mtext);
  if (strcmp(key,"END") == 0){
        printf("END MESSAGE RECIEVED : reducerID %d \n", reducerID);
        return 0;
    }
    
  return 1; 
}

void shuffle(int nMappers, int nReducers) {
  key_t key = ftok("P2", 4061); 
  int reducerID;
  int  mid = msgget(key,0666|IPC_CREAT);
  struct mymsg_t chunk;
  struct dirent* entry;
  for(int i=0; i<nMappers; i++) {
    
    char path[100]; 
    char strnum[50];
    strcat(path, "output/MapOut/Map_");
    sprintf(strnum, "%d", i+1);
    strcat(path,strnum); // add number
    
    DIR* dir = opendir(path);
    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
      printf("Test\n");
      if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))    continue;
      printf("d_name: %s\n", entry->d_name); //why no print?
      char filepath[100];
      strcpy(filepath, path); 
      strcat(filepath, "/");
      strcat(filepath, entry->d_name); 
      printf("filepath : %s \n ", filepath ); 
      memset(chunk.mtext, '\0', 1024); 
      strcat(chunk.mtext, filepath); 
  
      reducerID = hashFunction(entry->d_name, nReducers);
      chunk.mtype = reducerID;
      msgsnd(mid, (void *)&chunk, sizeof(chunk.mtext),0);
    }
    
  }

    // create END message 
  memset(chunk.mtext, '\0', 1024); 
  strcat(chunk.mtext, "END"); 
  
  // send END message to every mapper by associating id of the mapper with the message 
  for(int i =1 ; i < nReducers + 1; i++){
      chunk.mtype = reducerID;
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