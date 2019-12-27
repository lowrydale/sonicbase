#pragma once

#include <exception>

class UnsupportedOperationException : public std::exception
{
private:
    std::string msg;

public:
    UnsupportedOperationException(const std::string& message = "") : msg(message)
    {
    }

    const char * what() const throw()
    {
        return msg.c_str();
    }
};

class IllegalStateException : public std::exception
{
private:
    std::string msg;

public:
    IllegalStateException(const std::string& message = "") : msg(message)
    {
    }

    const char * what() const throw()
    {
        return msg.c_str();
    }
};

class ClassCastException : public std::exception
{
private:
    std::string msg;

public:
    ClassCastException(const std::string& message = "") : msg(message)
    {
    }

    const char * what() const throw()
    {
        return msg.c_str();
    }
};

class NullPointerException : public std::exception
{
private:
    std::string msg;

public:
    NullPointerException(const std::string& message = "") : msg(message)
    {
    }

    const char * what() const throw()
    {
        return msg.c_str();
    }
};
