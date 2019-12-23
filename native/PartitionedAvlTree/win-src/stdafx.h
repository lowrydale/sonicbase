// stdafx.h : include file for standard system include files,
// or project specific include files that are used frequently, but
// are changed infrequently
//


#ifdef _WIN32
#include "targetver.h"

#pragma once

#include <stdio.h>
#include <string>
#include <stdlib.h>
#include <mutex>
#include <thread>
#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN             // Exclude rarely-used stuff from Windows headers
// Windows Header Files:
#include <windows.h>
#include <jni.h>
#else
#include <unistd.h>
#include "linux/jni.h"        // JNI header provided by JDK#include <stdio.h>      // C Standard IO Header
#endif
#include <map>
#include <iterator>
#include "utf8.h"


#endif
// TODO: reference additional headers your program requires here
