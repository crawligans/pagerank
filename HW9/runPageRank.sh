javac -Xlint -cp src/:lib/kvs.jar:lib/webserver.jar:lib/flame.jar src/cis5550/jobs/PageRank.java
jar  cvf lib/pagerank.jar src/cis5550/jobs/PageRank.class
java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 lib/pagerank.jar cis5550.jobs.PageRank 0.01
