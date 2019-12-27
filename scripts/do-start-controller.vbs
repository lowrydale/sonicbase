Set WshShell = CreateObject("WScript.Shell")
Dim mystring
mystring =  WScript.Arguments(0)
WshShell.Run mystring, 0
Set WshShell = Nothing