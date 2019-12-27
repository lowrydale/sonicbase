#include "Test.h"
#include "ConcurrentSkipListMap.h"

namespace skiplist
{

	Test::MyKey::MyKey(long long value)
	{
	  this->value = value;
	}

	void Test::main(std::wstring args[])
	{
	  ConcurrentSkipListMap<MyKey*, MyKey*> *map = new ConcurrentSkipListMap<MyKey*, MyKey*>();
	  MyKey tempVar(1);
	  MyKey tempVar2(2);
	  map->put(&tempVar, &tempVar2);
	  MyKey tempVar3(1);
	  std::wcout << L"out: " << map->get((&tempVar3)->value) << std::endl;
	}
}
