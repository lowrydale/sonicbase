

#include "stdafx.h"
#include <jni.h>
#ifdef _WIN32
#include <windows.h>
#endif
#include <stdio.h>
#include <conio.h>

#include "WindowsTerminal.h"


#ifdef _WIN32
// This is an example of an exported variable
WINDOWSTERMINAL_API int nWindowsTerminal = 0;

// This is an example of an exported function.
WINDOWSTERMINAL_API int fnWindowsTerminal(void)
{
	return 42;
}

// This is the constructor of a class that has been exported.
// see WindowsTerminal.h for the class definition
CWindowsTerminal::CWindowsTerminal()
{
	return;
}

#ifndef ENABLE_VIRTUAL_TERMINAL_PROCESSING
#define ENABLE_VIRTUAL_TERMINAL_PROCESSING 0x0004
#endif

/*
* Class:     com_sonicbase_common_WindowsTerminal
* Method:    getConsoleSize
* Signature: ()Ljava/lang/String;
*/
JNIEXPORT jstring JNICALL Java_com_sonicbase_common_WindowsTerminal_getConsoleSize
(JNIEnv *env, jobject) {
	char str[80];

	CONSOLE_SCREEN_BUFFER_INFO csbi;
	int columns, rows;

	GetConsoleScreenBufferInfo(GetStdHandle(STD_OUTPUT_HANDLE), &csbi);
	columns = csbi.srWindow.Right - csbi.srWindow.Left;
	rows = csbi.srWindow.Bottom - csbi.srWindow.Top;

	printf("columns: %d\n", columns);
	printf("rows: %d\n", rows);

	sprintf_s(str, 80, "%d,%d", csbi.srWindow.Right - csbi.srWindow.Left, csbi.srWindow.Bottom - csbi.srWindow.Top);

	jstring result = env->NewStringUTF(str); // C style string to Java String

	return result;
}

JNIEXPORT void JNICALL Java_com_sonicbase_common_WindowsTerminal_enableAnsi
(JNIEnv *, jobject)

{
	// Set output mode to handle virtual terminal sequences
	HANDLE hOut = GetStdHandle(STD_OUTPUT_HANDLE);
	if (hOut == INVALID_HANDLE_VALUE)
	{
		return;
	}
	HANDLE hIn = GetStdHandle(STD_INPUT_HANDLE);
	if (hIn == INVALID_HANDLE_VALUE)
	{
		return;
	}

	DWORD dwOriginalOutMode = 0;
	DWORD dwOriginalInMode = 0;
	if (!GetConsoleMode(hOut, &dwOriginalOutMode))
	{
		return;
	}
	if (!GetConsoleMode(hIn, &dwOriginalInMode))
	{
		return;
	}

	DWORD dwRequestedOutModes = ENABLE_VIRTUAL_TERMINAL_PROCESSING;
	//DWORD dwRequestedOutModes = ENABLE_VIRTUAL_TERMINAL_PROCESSING | DISABLE_NEWLINE_AUTO_RETURN;
	//    DWORD dwRequestedInModes = ENABLE_VIRTUAL_TERMINAL_INPUT;

	DWORD dwOutMode = dwOriginalOutMode | dwRequestedOutModes;
	if (!SetConsoleMode(hOut, dwOutMode))
	{
		// we failed to set both modes, try to step down mode gracefully.
		dwRequestedOutModes = ENABLE_VIRTUAL_TERMINAL_PROCESSING;
		dwOutMode = dwOriginalOutMode | dwRequestedOutModes;
		if (!SetConsoleMode(hOut, dwOutMode))
		{
			// Failed to set any VT mode, can't do anything here.
			return;
		}
	}

	//    DWORD dwInMode = dwOriginalInMode | ENABLE_VIRTUAL_TERMINAL_INPUT;
	//    if (!SetConsoleMode(hIn, dwInMode))
	//    {
	//        // Failed to set VT input mode, can't do anything here.
	//        return;
	//    }

	return;
}

#else

JNIEXPORT jstring JNICALL Java_com_sonicbase_common_WindowsTerminal_getConsoleSize
(JNIEnv *env, jobject) {

}

JNIEXPORT void JNICALL Java_com_sonicbase_common_WindowsTerminal_enableAnsi
(JNIEnv *, jobject)

{
}


#endif



