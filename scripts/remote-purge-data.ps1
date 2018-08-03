#Read-host -assecurestring | convertfrom-securestring | out-file C:\cred.txt
$pass=get-content $1 | convertto-securestring
$credential = new-object -typename System.Management.Automation.PSCredential -argumentlist "$2",$pass
Invoke-Command -ComputerName $3 -ScriptBlock {$4\bin\purge-data.bat $5} -credential $credential