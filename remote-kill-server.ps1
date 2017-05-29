#Read-host -assecurestring | convertfrom-securestring | out-file C:\cred.txt
#$password=get-content c:\cred.txt | convertto-securestring
$pass=get-content $1 | convertto-securestring
$credential = new-object -typename System.Management.Automation.PSCredential -argumentlist "$2",$pass
Invoke-Command -ComputerName $3 -ScriptBlock {$4\bin\kill-server.bat $5} -credential $credential