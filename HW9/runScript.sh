javac -Xlint -cp src/:lib/kvs.jar:lib/webserver.jar:lib/flame.jar src/cis5550/jobs/Indexer.java
jar  cvf lib/indexer.jar src/cis5550/jobs/Indexer.class
java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 lib/indexer.jar cis5550.jobs.Indexer
