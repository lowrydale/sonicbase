#Read-host -assecurestring | convertfrom-securestring | out-file C:\cred.txt
$password=get-content c:\cred.txt | convertto-securestring
$credential = new-object -typename System.Management.Automation.PSCredential -argumentlist "Administrator",$password
Invoke-Command -ComputerName $1 -ScriptBlock {$2\bin\kill-server.bat $3} -credential $credential