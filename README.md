# spark-proj-template
Based on Sample Apache Spark Java project

Build project in each of exercises:
```
mvn package
```

Run project on HDP instance:
```
spark-submit --class "exercise01.Exercise01" --master local ./sparkexample-0.0.1-SNAPSHOT.jar
```
or
```
spark-submit --class "exercise02.Exercise02" --master local ./sparkexample-0.0.1-SNAPSHOT.jar
```
or
```
spark-submit --class "exercise03.Exercise03" --master local ./sparkexample-0.0.1-SNAPSHOT.jar
```

Run project on local PC (without Hadoop):
```
java -jar target/sparkexample-0.0.1-SNAPSHOT.jar
```
