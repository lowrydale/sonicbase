
if "%1" == "local" (
    echo "calling start-bench-server: workingDir=" + %cd%
    bin\start-bench-server.bat %3 %4 %5 %6 %7
)
else (
    ssh -n -f -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $1 nohup bash $2/bin/start-bench-server $3 $4 $5 $6 $7
)