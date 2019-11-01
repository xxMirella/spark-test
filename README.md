# Spark test

Repository with a spark application, where analyse logs from NASA.

With this application you will have the answer for this questions:

- Number of unique hosts
- Total of 404 errors
- 5 urls with more 404 errors
- Total of errors per day
- Total of bytes returned

# Installation

You must have already installed Java, Scala, Apache Spark installed, if you dont, please [click here](https://intellipaat.com/blog/tutorial/spark-tutorial/downloading-spark-and-getting-started/) to check an guide on how to install this tools.

The log files, must be placed at the `data/` folder.

# Run application

Run `mvn package` to generate a jar.

Run `spark-submit --class app.Main \
     --master local --deploy-mode client --executor-memory 1G \
     --name spark-test --conf "spark.app.id=spark-test" \
     spark-test-1.0-shaded.jar`
     
Or, you can open this repository on IntelliJ and after maven recognizes the pom file and download all the sources, you just have to press play.
