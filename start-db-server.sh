export _XMX_=$3
export SEARCH_HOME=$4
#export MODE=yprof
mkdir $4/logs

#if [ $2 == "9010" ]
#then
#  export MODE=yprof
#fi

if [ ${4:0:1} != "/" ]
then
  export SEARCH_HOME=$HOME/$SEARCH_HOME
fi

cd $SEARCH_HOME
echo "start-search_home=$SEARCH_HOME"

export _LOG4J_FILENAME_=$SEARCH_HOME/logs/$2.log
export _GC_LOG_FILENAME_=$SEARCH_HOME/logs/gc-$2.log
export LOG4J_FILE="log4j.xml"
nohup $SEARCH_HOME/bin/runclass com.lowryengineering.research.socket.NettyServer -host $1 -port $2 -cluster $5 > $SEARCH_HOME/logs/$2.sysout.log 2>&1 &
#nohup $SEARCH_HOME/bin/runclass com.lowryengineering.research.socket.NettyServer -host $1 -port $2 -cluster $5 &
