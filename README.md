# spark-proj-template
Based on Sample Apache Spark Java project

Build project in each of exercises:
```
mvn package
```

Run project on HDP instance:
```
spark-submit --class "exercise{x}.Exercise{x}" --master local ./sparkexample-0.0.1-SNAPSHOT.jar
```
{x} -> replace to exercise number e.g. 01, 02, 03, etc.

Run project on local PC (without Hadoop):
```
java -jar target/sparkexample-0.0.1-SNAPSHOT.jar
```
