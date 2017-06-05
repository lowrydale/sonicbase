// The following ifdef block is the standard way of creating macros which make exporting 
// from a DLL simpler. All files within this DLL are compiled with the WINDOWSTERMINAL_EXPORTS
// symbol defined on the command line. This symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see 
// WINDOWSTERMINAL_API functions as being imported from a DLL, whereas this DLL sees symbols
// defined with this macro as being exported.
#ifdef WINDOWSTERMINAL_EXPORTS
#define WINDOWSTERMINAL_API __declspec(dllexport)
#else
#define WINDOWSTERMINAL_API __declspec(dllimport)
#endif

// This class is exported from the WindowsTerminal.dll
class WINDOWSTERMINAL_API CWindowsTerminal {
public:
	CWindowsTerminal(void);
	// TODO: add your methods here.
};

extern WINDOWSTERMINAL_API int nWindowsTerminal;

WINDOWSTERMINAL_API int fnWindowsTerminal(void);


/* Header for class com_sonicbase_common_WindowsTerminal */

#ifndef _Included_com_sonicbase_common_WindowsTerminal
#define _Included_com_sonicbase_common_WindowsTerminal
#ifdef __cplusplus
extern "C" {
#endif
	/*
	* Class:     com_sonicbase_common_WindowsTerminal
	* Method:    enableAnsi
	* Signature: ()V
	*/
	JNIEXPORT void JNICALL Java_com_sonicbase_common_WindowsTerminal_enableAnsi
	(JNIEnv *, jobject);

	/*
	* Class:     com_sonicbase_common_WindowsTerminal
	* Method:    getConsoleSize
	* Signature: ()Ljava/lang/String;
	*/
	JNIEXPORT jstring JNICALL Java_com_sonicbase_common_WindowsTerminal_getConsoleSize
	(JNIEnv *, jobject);
#ifdef __cplusplus
}
#endif
#endif
