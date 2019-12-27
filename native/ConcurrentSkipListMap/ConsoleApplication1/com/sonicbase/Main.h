#pragma once

#include <string>
#include <vector>
#include <memory>

namespace com::sonicbase
{

	class Main : public std::enable_shared_from_this<Main>
	{

		static void main(std::vector<std::wstring> &args);
	};

}
