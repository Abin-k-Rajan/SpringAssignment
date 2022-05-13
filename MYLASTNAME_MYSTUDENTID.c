//	FOR COMPILATION USE : gcc MYLASTNAME_MYSTUDENTID.c -o MYLASTNAME_MYSTUDENTID.exe -pthread
//	TO RUN USE : ./MYLASTNAME_MYSTUDENTID.exe (SJF / FCFS / PRIORITY) (input file name).txt MYLASTNAME_MYSTUDENTID_(SJF / FCFS / PRIORITY).out


#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string.h>
#include <limits.h>

#define MAX_QUEUE_SIZE 100
pthread_mutex_t mutex;			//	MUTEX LOCK FOR COORDINATING TASKS WHILE DISPATCHING
pthread_mutex_t fifo_mutex;		//	MUTEX LOCK FOR COORDINATING SEND() RECV() BETWEEN FIFO AND CORRECTLY WRITING THEM TO OUTPUT FILE

//	GLOBAL VARIABLESS CRITICAL FOR CORRECT OPERATION
int type_of_scheduler = 0; 	// 0-FCFS 1-SJF 2-priority (set in main function based on command line parameters)

//      MINI PCB STRUCTURE

struct PCB	//	MINI PCB DATA STRUCTURE
{
        int pid;
        int (*fp)(int x, int y);
        int priority;
        int arrival_time;
        int burst_time;
        int i;
        int j;
        int retval;
};


struct PCB* PCB_Constructor(int pid, void* fp, int priority, int arrival_time, int burst_time, int i, int j)	//	CREATING A PCB OBJECT USING FUNCTION
{
        struct PCB* res = (struct PCB*)malloc(sizeof(struct PCB));
        res->pid = pid;
        res->fp = fp;
        res->priority = priority;
        res->arrival_time = arrival_time;
        res->burst_time = burst_time;
        res->i = i;
        res->j = j;
        res->retval = 0;
        return res;
}



//      READY QUEUE IMPLEMENTATION PARAMS AND FUNCTIONS
int front = 0;
int rear = 0;
int isQueueEmpty();
int isQueueFull();
int peek_on_burst_time(struct PCB* arr[]);
int peek_on_priority(struct PCB* arr[]);
struct PCB* dequeue_on_index(struct PCB* arr[], int index);
int enqueue(struct PCB* arr[], struct PCB** val);
//      READY QUEUE IMPLEMENTATION PROTOTYPE ENDS HERE


//	FIFO DATA STRUCTURE USED FOR COMMUNICATION
int start = 0;
int end = 0;
struct PCB** fifo;
int isFifoEmpty();
int isFifoFull();
struct PCB* recv();
int send(struct PCB* pcb);


//      FILE PARSING FUNCTIONS FOR VARIOUS SCHEDULER PROTOTYPES
int parseLine(char* line, char*** res, int count);
struct PCB* parseFcfsLine(char* line);
struct PCB* parseSjfLine(char* line);
struct PCB* parsePriorityLine(char* line);
int handleReadAndPopulateQueue(char* fileName, int type, struct PCB* arr[]);
int get_int_from_scheduler_type(char* type);
void* get_address_of_scheduler_from_type(int type);
int get_function_name_from_function_pointer(int (*fp)(int x, int y), char** res);

//	UTILITY FUNCTIONS USED FOR PERFORMING BASIC COMPUTATIONS
int sum(int i, int j);
int product(int i, int j);
int power(int i, int j);
int fibonacii(int i, int j);
char* int_to_string(int num);

//	DISPATCHER - EXECUTES THE FUNCTION POINTER IN MINI PCB DATA STRUCTURE AND SENDS THE RESULTING PCB TO FIFO
void* dispatcher(struct PCB* pcb);
//	SCHEDULER_DISPATCHER SIMPLY CALLS THE APPROPRIATE SCHEDULER BASED ON THE SCHEDULER_TYPE VARIABLE DEFINED IN GLOBAL SECTION
void* scheduler_dispatcher(void* arg);
//	LOGGER SIMPLY LOGS THE OUTPUT ONTO THE FILE SPECIFIED IN COMMAND LINE
void* logger(void* arg);

//	SCHEDULING ALGORITHM PROTOTYPES	RETURNS PCB AFTER PICKING APPROPRIATE PROCESS
struct PCB* fcfs_scheduler(struct PCB** arr);
struct PCB* sjf_scheduler(struct PCB** arr);
struct PCB* priority_scheduler(struct PCB** arr);

//      ARRAY OF FUNCTION POINTERS
int (*fp[4])(int x, int y);
struct PCB* (*scheduler[3])(struct PCB** arr);



//	MAIN FUNCTION STARTS HERE


int main(int argc, char** argv)
{
        pthread_t pid, scheduler_pid, logger_pid;	//	THREAD ID FOR SCHEDULER_DISPATCHER AND LOGGER
        pthread_mutex_init(&mutex, NULL);
        pthread_mutex_init(&fifo_mutex, NULL);
        int output_fd;					//	FILE DESCRIPTOR FOR OUTPUT FILE
        
        struct PCB* arr[MAX_QUEUE_SIZE];	//	CREATING READY QUEUE 
        fifo = (struct PCB**)malloc(sizeof(struct PCB**) * MAX_QUEUE_SIZE);	//	INITIALIZING FIFO DATA STRUCTURE FOR THREAD COMMUNCICATION
        
        fp[0] = sum;		//	FUNCTION POINTERS TO PERFORM SUM, PRODUCT, POWER, FIBONACII
        fp[1] = product;	//	FUNCTION TAKES TWO PARAMETERS I, J
        fp[2] = power;		//	ARRAY IS DEFINED IN GLOBAL SCOPE
        fp[3] = fibonacii;
        
        scheduler[0] = fcfs_scheduler;		//	FUNCTION POINTERS TO SCHEDULING ALGORITHMS
        scheduler[1] = sjf_scheduler;		//	ARRAY DEFINED IN GLOBAL SCOPE
        scheduler[2] = priority_scheduler;	//	RETURNS MINI PCB AND ACCEPTS ADDRESS OF READY QUEUE AS INPUT PARAMETER
        
        if (argc < 4)
        {
        	printf("Commandline error (4 parameters required) Correct format: ./filename.exe SCHEDULER tasksfile outputfile\n");
        	exit(-1);
        }
        char* scheduler_type = argv[1];		//	CREATING APPROPRIATE VARIABLES FOR EACH COMMAND LINE ARGUMENT
        char* inputfilename = argv[2];
        char* outputfilename = argv[3];
        int type = get_int_from_scheduler_type(scheduler_type);
        
        if (type < 0)
        {
        	printf("Scheduler type not valid, use one of thse (FCFS, SJF, PRIORITY)\n");
        	exit(-2);
        }
        
        output_fd = open(outputfilename, O_RDWR | O_TRUNC);	//	OPEN OUTPUT FILE, AND CLEAR ALL THE DATA USING O_TRUNC
        if (output_fd < 0)
        	output_fd = open(outputfilename, O_RDWR | O_CREAT, S_IRWXU | S_IRWXG);		//	IF FILE DOESN'T EXIST CREATE ONE
        
        if (output_fd < 0)
        {
        	printf("Error occured when creating output file %s\n", outputfilename);		//	ERROR OPENING FILE
        	exit(-3);
        }
        
        type_of_scheduler = type;	//	SET SCHEDULER TYPE GLOBALLY
        
        handleReadAndPopulateQueue(inputfilename, type, arr);	//	FUNCTION THAT READS INPUTFILE AND POPULATES READYQUEUE
        
        pthread_create(&scheduler_pid, NULL, &scheduler_dispatcher, arr);	//	SCHEDULER_DISPACTHER THREAD
        pthread_create(&logger_pid, NULL, &logger, &output_fd);			//	LOGGER THREAD
        
        pthread_join(scheduler_pid, NULL);		//	WAIT FOR SCHEDULER DISPATCHER
        pthread_join(logger_pid, NULL);			//	WAIT FOR LOGGER
        
        pthread_mutex_destroy(&mutex);			//	DESTROY MUTEX VARIABLES
        pthread_mutex_destroy(&fifo_mutex);
        
        return 0;
}

//	END OF MAIN FUNCTION

void* logger(void* arg)
{
	int* fd = (int *)arg;
	char* function_name;
	char* res;
	while (1)
	{
		if (isFifoEmpty() == 1)		//	WAIT FOR FIFO TO BE POPULATED
			continue;
		struct PCB* pcb = recv();
		if (pcb == NULL)		//	IF PCB IS NULL THEN SCHEDULER_DISPATCHER HAS EXECUTED ALL THE PROCESS
			break;			//	HENCE TERMINATE THE LOOP
			
		pthread_mutex_lock(&fifo_mutex);	//	LOCK MUTEX_FIFO TILL OUTPUT IS LOGGED INTO FILE
		get_function_name_from_function_pointer(pcb->fp, &function_name);
		res = (char*)malloc(100);	//	CHAR* THAT STORES THE RESULT IN REQUIRED FORMAT
		strcat(res, function_name);
		strcat(res, ",");
		strcat(res, int_to_string(pcb->i));
		strcat(res, ",");
		strcat(res, int_to_string(pcb->j));
		strcat(res, ",");
		strcat(res, int_to_string(pcb->retval));
		strcat(res, "\n");
		write(*fd, res, 100);		//	WRITE THE RESULT INTO OUTPUT FILE
		pthread_mutex_unlock(&fifo_mutex);	//	RELEASE LOCK
	}
}


//      DISPATCHER CODE

void* dispatcher(struct PCB* pcb)
{
        int* args = (int *)malloc(2 * sizeof(int));
        args[0] = pcb->i;
      	args[1] = pcb->j;
      	int result;
      	pthread_t pid;
        pthread_mutex_lock(&mutex);		//	MUTEX LOCK TO LOCK DISPATCHER WHEN ONE PROCESS IN EXECUTION PHASE
        
        result = (pcb->fp)(pcb->i, pcb->j);
        // pthread_create(&pid, NULL, (void*) pcb->fp, args);	//	CREATE A THREAD THAT CALLS FUNCTION POINTED TO IN MINI PCB
        // pthread_join(pid, (void *)&result);			//	WAIT FOR TERMINATION OF PROCESS
        pcb->retval = result;					//	STORE THE RESULT IN MINI PCB
        send(pcb);						//	SEND MINI PCB TO FIFO
        pthread_mutex_unlock(&mutex);		//	UNLOCK MUTEX (READY TO EXECUTE NEW PROCESS)
}


//      SCHEDULER_DISPATCHER CODE

void* scheduler_dispatcher(void* arg)
{
	struct PCB** queue = arg;
	while (1)
	{
		if (isQueueEmpty() == 1)
		{
			send(NULL);		//	IF QUEUE IS EMPTY SEND NULL TO FIFO TO LET LOGGER KNOW THAT SCHEDULER HAS EXECUTED ALL THE PROCESS
			break;			//	TERMINATED THE LOOP
		}
		struct PCB* res = scheduler[type_of_scheduler](queue);		//	CALL THE APPROPRIATE SCHEDULER WITH REFERENCE TO READY QUEUE 
		dispatcher(res);		//	ONCE PROCESS IS PICKED EXECUTE IT USING DISPATCHER
	}
}

//	FCFS SCHEDULER (BASED ON ARRIVAL TIME)

struct PCB* fcfs_scheduler(struct PCB** arr)
{
	struct PCB* res;
       	res = dequeue_on_index(arr, 0);
	return res;
}

//	SJF SCHEDULER (BASED ON BURST TIME, TIE BREAKER : LOWER PID)

struct PCB* sjf_scheduler(struct PCB** arr)
{
	struct PCB* res;
	int index;
        index = peek_on_burst_time(arr);	//	FUNCTION TO GET INDEX OF PROCESS WITH LEASE BURST TIME
        if (index == -1)
       		return NULL;
       	res = dequeue_on_index(arr, index);
        return res;
}

//	PRIORITY SCHEDULER (BASED ON PRIORITY (1 BEING THE HIGHEST PRIORITY), TIE BREAKER : LOWER PID)

struct PCB* priority_scheduler(struct PCB** arr)
{
	struct PCB* res;
	int index;
        index = peek_on_priority(arr);		//	FUNCTION TO GET INDEX OF PROCESS WITH HIGHEST PRIORITY
        if (index == -1)
        	return NULL;
        res = dequeue_on_index(arr, index);
        return res;
}

//      QUEUE IMPLEMENTATION BEGINS HERE

int isQueueEmpty()
{
        if (front == rear)
                return 1;
        return -1;
}

int isQueueFull()
{
        if (rear == MAX_QUEUE_SIZE)
                return 1;
        return -1;
}


int enqueue(struct PCB* arr[], struct PCB** val)
{
        if (isQueueFull() == 1)
        {
                printf("Queue is full\n");
                return -1;
        }
        arr[rear] = *val;
        rear++;
}

//	RETURNS INDEX OF PROCESS WITH LEAST BURST TIME

int peek_on_burst_time(struct PCB* arr[])
{
	int lowest_burst_time = INT_MAX;
	int ind = -1;
	int i = 0;
	for (i = front; i < rear; i++)
	{
		struct PCB* temp = arr[i];
		if (lowest_burst_time > temp->burst_time)
		{
			ind = i;
			lowest_burst_time = temp->burst_time;
			continue;
		}
		if (lowest_burst_time == temp->burst_time && ind != -1 && temp->pid < arr[ind]->pid)
		{
			ind = i;
		}
	}
	return ind;
}

//	RETURNS INDEX OF PROCESS WITH HIGHEST PRIORITY

int peek_on_priority(struct PCB* arr[])
{
	int highest_priority = INT_MAX;
	int ind = -1;
	int i = 0;
	for (i = front; i < rear; i++)
	{
		struct PCB* temp = arr[i];
		if (highest_priority > temp->priority)
		{
			highest_priority = temp->priority;
			ind = i;
			continue;
		}
		if (highest_priority == temp->priority && ind != -1 && temp->pid < arr[ind]->pid)
		{
			ind = i;
		}
	}
	return ind;
}


struct PCB* dequeue_on_index(struct PCB* arr[], int index)
{
	int i = index;
	struct PCB* res = arr[index];
	for (; i < rear; i++)
	{
		arr[i] = arr[i + 1];
	}
	rear--;
	return res;
}

//      FILE HANDLING FUNCTIONS

int parseLine(char* line, char*** res, int count)
{
        char* x;
        x = (char *)malloc(11);
        *res = (char**)malloc(sizeof(char*) * count);
        int i = 0;
        int pos = 0;
        int res_count = 0;
        while (line[i] != 0x00)
        {
                if (line[i] == ',')
                {
                        x[pos] = 0x00;
                        (*res)[res_count] = x;
                        pos = 0;
                        x = (char*)malloc(11);
                        res_count++;
                        i++;
                        continue;
                }
                x[pos] = line[i];
                i++;
                pos++;
        }
        x[pos] = 0x00;
        (*res)[res_count] = x;
        return 0;
}



void* getFunctionPointerFromFunctionName(char* functionName)
{
        int i = 0;
        int arr[4] = {  strcmp(functionName, "sum"),
                        strcmp(functionName, "product"),
                        strcmp(functionName, "power"),
                        strcmp(functionName, "fibonacci") };
        for (; i < 4; i++)
        {
                if (arr[i] == 0)
                {
                        return (void*)fp[i];
                }
        }
        return NULL;
}


//	UTILITY FUNCTIONS TO READ INPUT FILE APPROPRIATELY

struct PCB* parseFcfsLine(char* line)
{
        char** res;
        parseLine(line, &res, 5);
        int pid = atoi(res[0]);
        char* functionName = res[1];
        int i = atoi(res[2]);
        int j = atoi(res[3]);
        return PCB_Constructor(pid, getFunctionPointerFromFunctionName(functionName), 1, pid, 1, i, j);
}


struct PCB* parseSjfLine(char* line)
{
        char** res;
        parseLine(line, &res, 5);
        int pid = atoi(res[0]);
        int burst_time = atoi(res[1]);
        char* functionName = res[2];
        int i = atoi(res[3]);
        int j = atoi(res[4]);
        return PCB_Constructor(pid, getFunctionPointerFromFunctionName(functionName), 1, pid, burst_time, i, j);
}


struct PCB* parsePriorityLine(char* line)
{
        char** res;
        parseLine(line, &res, 5);
        int pid = atoi(res[0]);
        int priority = atoi(res[1]);
        char* functionName = res[2];
        int i = atoi(res[3]);
        int j = atoi(res[4]);
        return PCB_Constructor(pid, getFunctionPointerFromFunctionName(functionName), priority, pid, 1, i, j);
}


int handleReadAndPopulateQueue(char* filename, int type, struct PCB* arr[])
{
        size_t len = 0;
        size_t nread;
        char* line;
        FILE* fp = fopen(filename, "r");
        if (fp == NULL)
                return -1;
        while ((nread = getline(&line, &len, fp)) != -1)
        {
                struct PCB* res;
                switch (type) {
                        case 0: res = parseFcfsLine(line); break;
                        case 1: res = parseSjfLine(line); break;
                        case 2: res = parsePriorityLine(line); break;
                        default: return -1;
                }
                enqueue(arr, &res);
        }
}



int get_int_from_scheduler_type(char* type)
{
	if (strcmp("FCFS", type) == 0)
        {
        	return 0;
        }
        if (strcmp("SJF", type) == 0)
        {
        	return 1;
        }
        if (strcmp("PRIORITY", type) == 0)
        {
        	return 2;
        }
	return -1;
}


void* get_address_of_scheduler_from_type(int type)
{
	if (type == 0)
	{
		return scheduler[0];
	}
	if (type == 1)
	{
		return scheduler[1];
	}
	return scheduler[2];
}
  
 
 
 int get_function_name_from_function_pointer(int (*ptr)(int x, int y), char** res)
 {
 	if (ptr == fp[0])
 		*res = "sum";
 	else if (ptr == fp[1])
 		*res = "product";
	else if (ptr == fp[2])
		*res = "power";
	else
		*res = "fibonacii";
 	return 0;
 }
 
 
 
 //	FIFO DATA STRUCTURE
int isFifoEmpty()
{
	if (start == end)
		return 1;
	return -1;
}

int isFifoFull()
{
	if (end == MAX_QUEUE_SIZE)
		return 1;
	return -1;
}

struct PCB* recv()
{
	pthread_mutex_lock(&fifo_mutex);
	if (isFifoEmpty() == 1)
		return NULL;
	struct PCB* res = fifo[start];
	start++;
	pthread_mutex_unlock(&fifo_mutex);
	return res;
}

int send(struct PCB* pcb)
{
	pthread_mutex_lock(&fifo_mutex);
	if (isFifoFull() == 1)
		return -1;
	fifo[end] = pcb;
	end++;
	pthread_mutex_unlock(&fifo_mutex);
	return 0;
}


//	UTILITY FUNCTION

int sum(int i, int j)
{
        return i + j;
}

int product(int i, int j)
{
        return i * j;
}


int power(int i, int j)
{
        j--;
        int base = i;
        while (j > 0)
        {
                i *= base;
                j--;
        }
        return i;
}

int fibonacii(int i, int j)
{
        if (j <= 1)
                return j;
        return fibonacii(i, j - 1) + fibonacii(i, j - 2);
}

char* int_to_string(int num)
{
	int digit = 0;
	char* res = (char*)malloc(10);
	int pos = 0;
	int i = 0;
	if (num == 0)
		return "0";
	while (num)
	{
		digit = num % 10;
		num /= 10;
		res[pos++] = digit + '0';
	}
	for (i = 0; i < pos / 2; i++)
	{
		char temp = res[i];
		res[i] = res[pos - i - 1];
		res[pos - i - 1] = temp;
	}
	return res;
}
