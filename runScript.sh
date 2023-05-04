#javac -Xlint -cp src/:lib/kvs.jar:lib/webserver.jar:lib/flame.jar src/cis5550/jobs/Indexer.java
#jar  cvf lib/indexer.jar src/cis5550/jobs/Indexer.class
java -cp lib/build/download/kvs.jar:lib/build/download/webserver.jar:lib/build/download/flame.jar cis5550.flame.FlameSubmit localhost:9000 lib/build/libs/Indexer.jar cis5550.jobs.Indexer
