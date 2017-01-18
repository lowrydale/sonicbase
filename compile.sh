amkdir -p $HOME/pinyata-java/lib
mvn dependency:copy-dependencies -DoutputDirectory=$HOME/pinyata-java/lib
mvn install -DskipTests

