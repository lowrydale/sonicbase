#ifdef _WIN32
#include "stdafx.h"
#endif
#include <stdio.h>
#include <string>
#include <stdlib.h>
#include <mutex>
#include <thread>
#include <chrono>
#include <iomanip>
#include <map>
#include <iterator>
#include <vector>
#include <algorithm>
#include <cstring>
#include <locale>
#include <future>
#include <math.h>
//#include "utf8.h"
//#include "BigDecimal.h"
//#include <lzo/lzoconf.h>
//#include <lzo/lzo1x.h>
#include <atomic>
#include <condition_variable>
#include <stdexcept>
#include <iostream>
#include <list>
//#include "ctpl_stl.h"

#include <jni.h>

#include "sonicbase.h"

#include "Comparator.h"
#include "Comparable.h"
using namespace skiplist;
#include "Iterator.h"
#include "Iterable.h"
using namespace skiplist;
#include "AbstractCollection.h"
#include "Collection.h"
#include "Set.h"
#include "Map.h"
#include "AbstractSet.h"
#include "AbstractMap.h"
#include "SortedMap.h"
#include "NavigableSet.h"
#include "NavigableMap.h"
#include "ConcurrentMap.h"
#include "ConcurrentNavigableMap.h"
#include "AtomicJava.h"
#include "ConcurrentSkipListMap.h"

#include "com_sonicbase_index_NativeSkipListMap.h"



#define PARTITION_COUNT 1
