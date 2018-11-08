#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <sys/time.h>
#include <inttypes.h>
#include "mpi.h"

// ==============================================================================
// Author:          Paul Johnston
// University:      Queen's University Belfast
// Student Number:  40081039
// Email:           pjohnston36@qub.ac.uk
// ==============================================================================

#define MAXCONTROLS 500
#define MAXTEXTS 20
#define MAXPATTERNS 20
#define MASTER 0

char *textData[MAXTEXTS];
int textLength[MAXTEXTS];

char *patternData[MAXPATTERNS];
int patternLength[MAXPATTERNS];

int controlData[3][MAXCONTROLS];
	
char *result;
int resultLength;
int allocatedResultLength;

char *resultCurrentProcess;
int resultLengthCurrentProcess;
int allocatedLength;

int controlLength;

void outOfMemory()
{
	fprintf (stderr, "Out of memory\n");
	exit (0);
}

void readFromFile (FILE *f, char **data, int *length)
{
	int ch;
	int allocatedLength;
	char *result;
	int resultLength = 0;

	allocatedLength = 0;
	result = NULL;

	
	ch = fgetc (f);
	while (ch >= 0)
	{
		resultLength++;
		if (resultLength > allocatedLength)
		{
			allocatedLength += 10000;
			result = (char *) realloc (result, sizeof(char)*allocatedLength);
			if (result == NULL)
				outOfMemory();
		}
		result[resultLength-1] = ch;
		ch = fgetc(f);
	}
	*data = result;
	*length = resultLength;
}

int readControlData()
{
	FILE *f;
	char fileName[1000];
#ifdef DOS
        sprintf (fileName, "inputs\\control.txt");
#else
	sprintf (fileName, "inputs/control.txt");
#endif
	f = fopen (fileName, "r");
	if (f == NULL)
		return 0;

	int i = 0;
	int c;
	while (!feof (f))
	{
		c = fscanf(f, "%d %d %d", &controlData[0][i], &controlData[1][i], &controlData[2][i]);
		if (c == 3) 
		{ 
			i++; 
		}
	}
	fclose (f);
	return i;
}

int readAllTextData()
{	
	int textNumber = 0;
	while(textNumber<MAXTEXTS)
	{
		FILE *f;
		char fileName[1000];
	#ifdef DOS
			sprintf (fileName, "inputs\\text%d.txt", textNumber);
	#else
		sprintf (fileName, "inputs/text%d.txt", textNumber);
	#endif
		f = fopen (fileName, "r");
		if (f == NULL)
			return textNumber;
		readFromFile (f, &textData[textNumber], &textLength[textNumber]);
		fclose (f);
		textNumber++;
	}
	return MAXTEXTS;
}

int readAllPatternData()
{
	int patternNumber = 0;
	while(patternNumber<MAXPATTERNS)
	{
		FILE *f;
		char fileName[1000];
	#ifdef DOS
			sprintf (fileName, "inputs\\pattern%d.txt", patternNumber);
	#else
		sprintf (fileName, "inputs/pattern%d.txt", patternNumber);
	#endif
		f = fopen (fileName, "r");
		if (f == NULL)
			return patternNumber;
		readFromFile (f, &patternData[patternNumber], &patternLength[patternNumber]);
		fclose (f);
		patternNumber++;
	}
	return MAXPATTERNS;
}

void writeToResultString(int textNumber, int patternNumber, int searchResult)
{
	char tmpResult[100];
	if (strlen(resultCurrentProcess) > allocatedLength-100)
	{
		allocatedLength += 100000;
		resultCurrentProcess = (char *) realloc (resultCurrentProcess, sizeof(char)*allocatedLength);
	}

	sprintf(tmpResult, "%d %d %d\n", textNumber, patternNumber, searchResult);
	strcat(resultCurrentProcess, tmpResult);
}

void outputResults(char *result)
{
	FILE *f = fopen("result_MPI.txt", "w");
	if (f == NULL)
	{
	    printf("Error opening file!\n");
	    exit(1);
	}
	//write results to file
	fprintf(f, "%s", result);
	fclose(f);
}

// Find all instances of patterns in textChunk
int patternMatchAll(int textNumber, int patternNumber, int textStartPos, int textChunk)
{
	int i;
	int found = 0;
	int searchResult = -1;
		
	for (i = 0; i < textChunk; i++)
	{
		if (textLength[textNumber] - (textStartPos + i) < patternLength[patternNumber])
			break;
		int k = textStartPos + i;
		int j = 0;
			
		while (textData[textNumber][k] == patternData[patternNumber][j] && j < patternLength[patternNumber])
		{
			k++;
			j++;
		}
		
		if (j == patternLength[patternNumber])
		{
			found = 1;
			searchResult = textStartPos + i;
			writeToResultString(textNumber, patternNumber, searchResult);
		}
	}		
	return found;
}

// Find any instance of patterns in textChunk
int patternMatchAny(int textNumber, int patternNumber, int textStartPos, int textChunk)
{
	int i;
	int found = 0;
	int searchResult = -1;
		
	for (i = 0; i < textChunk; i++)
	{
		if(found)
			break;
		if (textLength[textNumber] - (textStartPos + i) < patternLength[patternNumber])
			break;
		int k = textStartPos + i;
		int j = 0;
			
		while (textData[textNumber][k] == patternData[patternNumber][j] && j < patternLength[patternNumber])
		{
			k++;
			j++;
		}
		
		if (j == patternLength[patternNumber])
		{
			found = 1;
		}
	}		
	return found;
}

//Gather all slave results to master
void gatherResults(int currentProcess, int numProcesses,int *recvcountsResult, int *displsResult)
{
	int totalResultLength;
	//Allocate recvcountsResult buffer array based on resultLengthCurrentProcess
	resultLengthCurrentProcess = strlen(resultCurrentProcess);
    if (currentProcess == 0)
    {
        recvcountsResult = malloc(numProcesses * sizeof(int)) ;
    }
	MPI_Gather(&resultLengthCurrentProcess, 1, MPI_INT, recvcountsResult, 1, MPI_INT, 0, MPI_COMM_WORLD);

	//Calculate displsResult array values based on recvcountsResult 
    if (currentProcess == 0) {
    	allocatedResultLength = 1000000;
		result = malloc(sizeof(char) * allocatedResultLength); 
        displsResult = malloc(numProcesses * sizeof(int));
        displsResult[0] = 0;
		int i;
        for (i=1; i<numProcesses; i++) 
        {
           displsResult[i] = displsResult[i-1] + recvcountsResult[i-1];
        }
    	for (i=0; i<numProcesses; i++) 
    	{
    		totalResultLength = totalResultLength + recvcountsResult[i];
    	} 
    	if (totalResultLength > allocatedResultLength)
		{
			result = (char *) realloc (result, sizeof(char)*totalResultLength);
		}
    }

    MPI_Gatherv(resultCurrentProcess, resultLengthCurrentProcess, MPI_CHAR,
                result, recvcountsResult, displsResult, MPI_CHAR,
                0, MPI_COMM_WORLD);
}

void master(int numProcesses)
{
	// Loop through each test case in control file
	int controlNum;
	for (controlNum = 0; controlNum < controlLength; controlNum++)
	{
		int searchType = controlData[0][controlNum];
		int textNumber = controlData[1][controlNum];
		int patternNumber = controlData[2][controlNum];
		long long worstCaseComparisons;
		int activeThreads = 0;	
		int startPos = 0;	
		int remainder;
		int textChunkSize; //default for small textlength
		
		// If pattern is longer than text, dont search, return not found
		if (patternLength[patternNumber] > textLength[textNumber])
		{
			writeToResultString(textNumber, patternNumber, -1);
			continue;
		}
		
		//Calculate textchunk size for each thread
		//worstCaseComparisons = (long long)patternLength[patternNumber]*((long long)textLength[textNumber] - (long long)patternLength[patternNumber]);
		//Use default textChunkSize 1000 for small texts, low worst case comparisons or searchtype 0
		if((textLength[textNumber] < 1000) || (worstCaseComparisons < 10000ULL) || searchType==0)
		{
			textChunkSize = 1000;
		}
		//For Middle cases, Divide text up between threads
		//If text isn't divisible by numProcesses, add remainder to each process
		else
		{
			if (textLength[textNumber] % numProcesses == 0)
			{
				textChunkSize = (textLength[textNumber]/numProcesses);
			}
			else 
			{
				remainder = textLength[textNumber] % numProcesses;
				textChunkSize = (textLength[textNumber]/numProcesses)+remainder;
			}
		}
		
		int patternMatchData[5] = {textNumber, patternNumber, startPos, textChunkSize, searchType};
		int lastI = textLength[textNumber] - patternLength[patternNumber];

		// Send patternMatchData to all threads, indicating startpos, textchunksize and test case data
		int i;
		for (i = 1; i < numProcesses; i++)
		{
			// If text length remaining is greater than patternlength send patternMatchData
			if (patternMatchData[2] <= lastI)
			{
				MPI_Send(&patternMatchData, 5, MPI_INT, i, 0, MPI_COMM_WORLD);
				startPos += patternMatchData[3]; 
				patternMatchData[2]=startPos;
				activeThreads++;
			}
			else
				break;
		}
		
		int finished = 0;
		int foundFlag = 0;
		// Loop over each character in the text until the test is complete
		while (!finished && activeThreads > 0) 
		{
			int found;
			MPI_Status statusData;
			MPI_Recv(&found, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &statusData);
			activeThreads--;
			
			// If a thread has found an occurrence of the pattern
			if (found)
			{
				// searchType 1 - find all instances
				if (searchType == 1)
				{
					foundFlag = 1;
				}
				// searchType 0 - find any instances
				else if(searchType == 0)
				{
					//Wait for remaining threads to finish
					while(activeThreads>0)
					{
						MPI_Recv(&found, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &statusData);
						activeThreads--;
					}
					foundFlag = 1;
					finished = 1;
				}
			}
			
			//searchType 1:	If all text has not been searched, keep searching	
			//searchType 0:	If pattern not found keep searching		
			if (!finished && patternMatchData[2] <= textLength[textNumber] - patternLength[patternNumber])
			{					
				MPI_Send(&patternMatchData, 5, MPI_INT, statusData.MPI_SOURCE, 0, MPI_COMM_WORLD);
				patternMatchData[2] += textChunkSize;
				activeThreads++;
			}	
		}	
		// If pattern not found
		if (!foundFlag)
			writeToResultString(textNumber, patternNumber, -1);
		else if(foundFlag && searchType == 0)
			writeToResultString(textNumber, patternNumber, -2);
	}
	
	// Test case is finished as all text has been searched, master signals finished.
	int finished = 1;
	MPI_Request requestFinished;
	int i;
	for (i = 1; i < numProcesses; i++)
	{
		MPI_Isend(&finished, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &requestFinished);
	}
}

void slave(int currentProcess)
{
	// Receive signal from master when test case is finished
	int finished = 0;
	int finishedFlag = 0;
	MPI_Request requestFinished;
	MPI_Status statusFinished;
	MPI_Irecv(&finished, 1, MPI_INT, MASTER, 1, MPI_COMM_WORLD, &requestFinished);
	
	while (!finishedFlag) 
	{
		// Wait for new patternMatchData from Master
		MPI_Request requestData;
		MPI_Status statusData;
		int newPatternMatchData = 0;
		int patternMatchData[5];
		MPI_Irecv(&patternMatchData, 5, MPI_INT, MASTER, 0, MPI_COMM_WORLD, &requestData);
		while(!newPatternMatchData)
		{
			MPI_Test(&requestFinished, &finishedFlag, &statusFinished);
			if (finishedFlag)
				return;
		
			MPI_Test(&requestData, &newPatternMatchData, &statusData);
		}
		
		int found;
		
		//SearchType 0: Find if pattern occurs
		if(patternMatchData[4] == 0)
			found = patternMatchAny(patternMatchData[0], patternMatchData[1], patternMatchData[2], patternMatchData[3]);
		//SearchType 1: Find all occurrences of pattern
		else
			found = patternMatchAll(patternMatchData[0], patternMatchData[1], patternMatchData[2], patternMatchData[3]);
			
		MPI_Send(&found, 1, MPI_INT, MASTER, 0, MPI_COMM_WORLD);
		
		// Check if master signals finished
		MPI_Test(&requestFinished, &finishedFlag, &statusFinished);
	}
}

int main(int argc, char **argv)
{
	int controlNum, numProcesses, currentProcess;
	int *recvcountsResult;
	int *displsResult;
	allocatedLength = 100000;
	resultCurrentProcess = malloc(sizeof(char) * allocatedLength);  

	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numProcesses);
	MPI_Comm_rank(MPI_COMM_WORLD, &currentProcess);
	printf ("Number of Processes = %d\n", numProcesses);
	
	//All processes read control, texts and patterns for minimal bcasts
	controlLength = readControlData();
	int numPatterns = readAllPatternData();
	int numTexts = readAllTextData();
	
	//Master Process
	if (currentProcess == MASTER)
		master(numProcesses); 
	//Slave Processes
	else
		slave(currentProcess);
	
	//Gather results from each slave to master
	gatherResults(currentProcess, numProcesses, recvcountsResult, displsResult);
	
	//Master outputs results to file 
	if(currentProcess == MASTER)
	{
		printf("%s", result);
		outputResults(result);
		free(displsResult);
		free(recvcountsResult);
	}

	MPI_Finalize();
}