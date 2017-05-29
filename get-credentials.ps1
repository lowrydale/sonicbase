$Credential = Get-Credential
$credential.Password | convertfrom-securestring | Set-Content $1\$2-$($credential.UserName)