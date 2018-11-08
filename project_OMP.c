#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <sys/time.h>
#include <inttypes.h>

// ==============================================================================
// Author:          Paul Johnston
// University:      Queen's University Belfast
// Student Number:  40081039
// Email:           pjohnston36qub.ac.uk
// ==============================================================================

#define MAXCONTROLS 500
#define MAXTEXTS 20
#define MAXPATTERNS 20

int NUMTHREADS;

char *textData[MAXTEXTS];
int textLength[MAXTEXTS];

char *patternData[MAXPATTERNS];
int patternLength[MAXPATTERNS];

int controlData[3][MAXCONTROLS];
int controlLength;

char result[100000];
int resultLength;

struct timeval t0, t1, t2;

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
	controlLength = i;
	return 1;
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

void outputResults(char result[])
{
	FILE *f = fopen("result_OMP.txt", "a+");
	if (f == NULL)
	{
	    printf("Error opening file!\n");
	    exit(1);
	}
	//write results to file
	fprintf(f, "%s", result);
	fclose(f);
	result[0] = '\0';
}

void writeToResultString(int textNumber, int patternNumber, int searchResult)
{
	char tmpResult[100];
	if (strlen(result) > 99900)
		outputResults(result);

	sprintf(result+strlen(result), "%d %d %d\n", textNumber, patternNumber, searchResult);
}

void patternMatchAny(int textNumber, int patternNumber)
{
	int i,j,k,lastI,found,searchResult;
	found = 0;
	searchResult = -1;
	j=0;
	k=0;
	lastI = textLength[textNumber]-patternLength[patternNumber];
	int getNumThreads;

	#pragma omp parallel for private(i,j,k) shared(found) num_threads(NUMTHREADS) schedule(static,patternLength[patternNumber])
	for(i = 0; i <= lastI; i++) 
	{
		//getNumThreads = omp_get_num_threads();
		if(found) continue;
		k=i;
		j=0;

		while( textData[textNumber][k] == patternData[patternNumber][j] && j < patternLength[patternNumber] )
		{
			k++;
			j++;
		}
		
		if(j==patternLength[patternNumber])
		{
			//Critical for printing to result string
			#pragma omp critical(check)
			{
				found = 1;
				searchResult = -2;
			}
		}
	}
	writeToResultString(textNumber, patternNumber, searchResult);
	//printf("NUMTHREADS: %d \n", getNumThreads);
}

void patternMatchAll(int textNumber, int patternNumber)
{
	int i,j,k,lastI,searchResult;
	searchResult = -1;
	j=0;
	k=0;

	lastI = textLength[textNumber]-patternLength[patternNumber];

	#pragma omp parallel for private(i,j,k) num_threads(NUMTHREADS) schedule(static,patternLength[patternNumber])
	for(i = 0; i <= lastI; i++) 
	{
		k=i;
		j=0;

		while( textData[textNumber][k] == patternData[patternNumber][j] && j < patternLength[patternNumber] )
		{
			k++;
			j++;
		}
		
		if(j==patternLength[patternNumber])
		{
			//Critical for printing to result string
			#pragma omp critical(check)
			{
				searchResult = i;
				writeToResultString(textNumber, patternNumber, searchResult);
			}
		}
	}
	if(searchResult == -1)
	{
		writeToResultString(textNumber, patternNumber, searchResult);
	}
}

void processData(int searchType, int textNumber, int patternNumber)
{	
	if(searchType == 0)
		patternMatchAny(textNumber, patternNumber);
	else
		patternMatchAll(textNumber, patternNumber);
}

void project_OMP_0()
{
	printf("\n|____project_OMP_0____|\n");

	NUMTHREADS = 20;
	if(readControlData())
	{
		printf("\nREADCONTROL");
		int numPatterns = readAllPatternData();
		printf("\nREADPATTERNS");
		int numTexts = readAllTextData();
		printf("\nREADTEXTS");
		int i;
		for(i=0;i<controlLength;i++)
		{
			//If pattern is longer than text, dont search
			if (patternLength[controlData[2][i]] > textLength[controlData[1][i]])
				writeToResultString(controlData[1][i], controlData[2][i], -1);
			else
				processData(controlData[0][i],controlData[1][i], controlData[2][i]);
		}
	}
	printf("\nNUMTHREADS: %d\n", NUMTHREADS);
	printf("%s", result);
	outputResults(result);
}

void project_OMP_1()
{
	printf("\n|____project_OMP_1____|\n");

	if(readControlData())
	{
		struct timeval t0, t1;
		uint64_t wallclockMicro;
		uint64_t minWallclockMicro[controlLength];
		int optimal_NUMTHREADS[controlLength];
		long long worstcaseComparisons[controlLength];
		int numPatterns = readAllPatternData();
		int numTexts = readAllTextData();
		int i;
		int STARTNUMTHREAD = 16;

		for(NUMTHREADS=STARTNUMTHREAD;NUMTHREADS<=20;NUMTHREADS++)
		{
			for(i=0;i<controlLength;i++)
			{
				gettimeofday(&t0, NULL);
				processData(controlData[0][i],controlData[1][i], controlData[2][i]);
				gettimeofday(&t1, NULL);
				wallclockMicro = ((t1.tv_sec - t0.tv_sec) * 1000000) + (t1.tv_usec - t0.tv_usec);
				if(textLength[controlData[1][i]] == patternLength[controlData[2][i]])
					worstcaseComparisons[i] = (long long)textLength[controlData[1][i]];
				else
					worstcaseComparisons[i] = (long long)patternLength[controlData[2][i]] * ((long long)textLength[controlData[1][i]] - (long long)patternLength[controlData[2][i]]);
				//printf("\n%d %d %d WALLCLOCK: %u NUMTHREADS: %d WorstCaseComparisons: %d\n", controlData[0][i],controlData[1][i], controlData[2][i], minWallclockMicro[i], NUMTHREADS, worstcaseComparisons[i]);
				if(wallclockMicro < minWallclockMicro[i] || NUMTHREADS==STARTNUMTHREAD)
				{
					minWallclockMicro[i] = wallclockMicro;
					optimal_NUMTHREADS[i] = NUMTHREADS;
				}

			}
		}
		for(i=0;i<controlLength;i++)
			{
				printf("\n%d %d %d MINWALLCLOCK: %u optimal_NUMTHREADS: %d WorstCaseComparisons: %d\n", controlData[0][i],controlData[1][i], controlData[2][i], minWallclockMicro[i], optimal_NUMTHREADS[i], worstcaseComparisons[i]);
			}
	}
	printf("%s", result);
	outputResults(result);
}


void project_OMP_2()
{
	printf("\n|____project_OMP_2____|\n");

	if(readControlData())
	{
		int numPatterns = readAllPatternData();
		int numTexts = readAllTextData();
		int i;
		long long worstcaseComparisons;
		for(i=0;i<controlLength;i++)
		{
			//If pattern is longer than text, dont search
			if (patternLength[controlData[2][i]] > textLength[controlData[1][i]])
			{
				writeToResultString(controlData[1][i], controlData[2][i], -1);
			}
			else
			{
				//Allocate optimal num threads based on worst case num comparisons
				if(textLength[controlData[1][i]] == patternLength[controlData[2][i]])
					worstcaseComparisons = (long long)textLength[controlData[1][i]];
				else if(textLength[controlData[1][i]] < patternLength[controlData[2][i]])
					worstcaseComparisons = 0ULL;
				else
					worstcaseComparisons = (long long)patternLength[controlData[2][i]] * ((long long)textLength[controlData[1][i]] - (long long)patternLength[controlData[2][i]]);			
				
				if(worstcaseComparisons > 100000000ULL)
				{
					NUMTHREADS = 20;
				}
				else if(worstcaseComparisons <= 1000ULL)
				{
					NUMTHREADS = 1;
				}
				else if(worstcaseComparisons <= 10000ULL)
				{
					NUMTHREADS = 3;
				}
				else if(worstcaseComparisons <= 100000000ULL)
				{
					NUMTHREADS = 18;
				}
				else
				{
					NUMTHREADS = 20;
				}
				//printf("%d %d %d WORSTCASE: %llu NUMTHREADS: %d\n", controlData[0][i],controlData[1][i], controlData[2][i], worstcaseComparisons, NUMTHREADS);
				processData(controlData[0][i],controlData[1][i], controlData[2][i]);
			}
		}
	}
	printf("%s", result);
	outputResults(result);
}

int main(int argc, char **argv)
{
	//Uses constant 20 (Max) threads for all test cases
	//Parallelises pattern matching for loops
	//project_OMP_0();
	
	//Experiments to find optimal num threads based on worst case num comparisons of each test case
	//project_OMP_1();
	
	//Dynamically allocates optimal num threads for each test case based on worst case num comparisons
	project_OMP_2(); //Final Version 
}