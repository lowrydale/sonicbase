
mkdir -p $HOME/database/target
mkdir -p $HOME/database/bin
mkdir -p $HOME/database/config
mkdir -p $HOME/database/web
mkdir -p $HOME/database/src
rm -rf $HOME/database/logs
mkdir -p $HOME/database/logs
mkdir -p $HOME/database/lib/linux64

mkdir -p $HOME/database/lib
rsync -vlLt repo/* $HOME/database/lib
mvn dependency:copy-dependencies -DoutputDirectory=$HOME/database/lib
rsync -rvlLt repo/linux64/* $HOME/database/lib/linux64

rsync -rvlLt db/target/classes/config/config-* $HOME/database/config
rsync -rvlLt $HOME/database-config/* $HOME/database/config
rsync -rvlLt runclass $HOME/database/bin
rsync -rvlLt cli.sh $HOME/database/bin
rsync -rvlLt start-local-db $HOME/database/bin
rsync -rvlLt kill-server.sh $HOME/database/bin
rsync -rvlLt is-server-running $HOME/database/bin
rsync -rvlLt start-local-rest $HOME/database/bin
rsync -rvlLt start $HOME/database/bin
rsync -rvlLt start-db-server.sh $HOME/database/bin
rsync -rvlLt remote-start-db-server $HOME/database/bin
rsync -rvlLt remote-start-rest-server $HOME/database/bin
rsync -rvlLt kill-server $HOME/database/bin
rsync -rvlLt dump-threads $HOME/database/bin
rsync -rvlLt terminal-size.py $HOME/database/bin
rsync -rvlLt terminal-resize.py $HOME/database/bin
rsync -rvlLt do-rsync.sh $HOME/database/bin
rsync -rvlLt do-start.sh $HOME/database/bin
rsync -rvlLt get-mem-total $HOME/database/bin
rsync -rvlLt remote-get-mem-total $HOME/database/bin
rsync -rvlLt db/src/main/resources/log4j.xml $HOME/database/config
rsync -rvlLt db/src/main/resources/cli-log4j.xml $HOME/database/config
chmod +x $HOME/database/bin/runclass
chmod +x $HOME/database/bin/cli.sh
chmod +x $HOME/database/bin/start-local-db
chmod +x $HOME/database/bin/kill-server.sh
chmod +x $HOME/database/bin/is-server-running
chmod +x $HOME/database/bin/start-local-rest
chmod +x $HOME/database/bin/start
chmod +x $HOME/database/bin/start-id-servers
chmod +x $HOME/database/bin/start-db-server.sh
chmod +x $HOME/database/bin/remote-start-db-server
chmod +x $HOME/database/bin/remote-start-rest-server
chmod +x $HOME/database/bin/kill-server
chmod +x $HOME/database/bin/terminal-size.py
chmod +x $HOME/database/bin/terminal-resize.py
chmod +x $HOME/database/bin/do-rsync.sh
chmod +x $HOME/database/bin/do-start.sh
chmod +x $HOME/database/bin/dump-threads
chmod +x $HOME/database/bin/get-mem-total
chmod +x $HOME/database/bin/remove-get-mem-total

rsync -rvlLt src/main/resources/* $HOME/database/target

rsync -rvlLt db/target/test-classes/* $HOME/database/target
rsync -rvlLt db/target/classes/main/java/* $HOME/database/target
rsync -rvlLt db/target/classes/* $HOME/database/target
rsync -rvlLt db/src/main/resources/web/* $HOME/database/web

rm $HOME/database/target/*log4j*

#rsync -rvlLt db/src/main/* $HOME/database/src


