Set WshShell = CreateObject("WScript.Shell")
Dim mystring
mystring = WScript.Arguments(0)
mystring = mystring & " " & WScript.Arguments(1)
WshShell.Run mystring, 0
Set WshShell = Nothing