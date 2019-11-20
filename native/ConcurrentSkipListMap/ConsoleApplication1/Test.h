#pragma once

#include <string>
#include <iostream>

namespace skiplist
{

	class Test
	{
  public:
	  class MyKey
	  {
	public:
		long long value = 0;
		MyKey(long long value);
	  };
	  static void main(std::wstring args[]);
	};
}
