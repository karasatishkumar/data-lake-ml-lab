We will be using Maven to create a sample project for the demonstration. To create the project, execute the following command in a directory that you will use as workspace

    mvn archetype:generate -DgroupId=org.satish.spark -DartifactId=sample-spark-wordcount -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false

### POM

    <project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
      <modelVersion>4.0.0</modelVersion>
      <groupId>org.satish.spark</groupId>
      <artifactId>sample-spark-wordcount</artifactId>
      <packaging>jar</packaging>
      <version>1.0-SNAPSHOT</version>
      <name>sample-spark-wordcount</name>
      <url>http://maven.apache.org</url>
      <dependencies>
        <!-- Import Spark -->
        <dependency>
          <groupId>org.apache.spark</groupId>
          <artifactId>spark-core_2.11</artifactId>
          <version>1.4.0</version>
        </dependency>
    
        <dependency>
          <groupId>junit</groupId>
          <artifactId>junit</artifactId>
          <version>4.11</version>
          <scope>test</scope>
        </dependency>
      </dependencies>
    
      <build>
        <plugins>
          <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-compiler-plugin</artifactId>
            <version>2.0.2</version>
            <configuration>
              <source>1.8</source>
              <target>1.8</target>
            </configuration>
          </plugin>
          <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-jar-plugin</artifactId>
            <configuration>
              <archive>
                <manifest>
                  <addClasspath>true</addClasspath>
                  <classpathPrefix>lib/</classpathPrefix>
                  <mainClass>com.geekcap.javaworld.sparkexample.WordCount</mainClass>
                </manifest>
              </archive>
            </configuration>
          </plugin>
          <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-dependency-plugin</artifactId>
            <executions>
              <execution>
                <id>copy</id>
                <phase>install</phase>
                <goals>
                  <goal>copy-dependencies</goal>
                </goals>
                <configuration>
                  <outputDirectory>${project.build.directory}/lib</outputDirectory>
                </configuration>
              </execution>
            </executions>
          </plugin>
        </plugins>
      </build>
    </project>

### Creating an Input File    

As we’re going to create a Word Counter program, we will create a sample input file for our project in the root directory of our project with name `input.txt`. Put any content inside it, we use the following text:

    Hello, my name is Shubham and I am author at JournalDev . JournalDev is a great website to ready
    great lessons about Java, Big Data, Python and many more Programming languages.
    
    Big Data lessons are difficult to find but at JournalDev , you can find some excellent
    pieces of lessons written on Big Data.

### Creating the WordCounter

    package org.satish.spark;
    
    import org.apache.spark.SparkConf;
    import org.apache.spark.api.java.JavaPairRDD;
    import org.apache.spark.api.java.JavaRDD;
    import org.apache.spark.api.java.JavaSparkContext;
    import scala.Tuple2;
    
    import java.util.Arrays;
    
    public class WordCounter
    {
        private static void wordCount (String fileName)
        {
    
            /*
            The master specifies local which means that this program should connect to Spark
            thread running on the localhost. App name is just a way to provide Spark with
            the application metadata.
            */
            SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JD Word Counter");
    
            JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
    
            JavaRDD<String> inputFile = sparkContext.textFile(fileName);
    
            /*
             Java 8 APIs to process the JavaRDD file and split the words the file contains
             into separate words
             */
            JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")));
    
            /*
            Java 8 mapToPair(...) method to count the words and provide a word, number
            pair which can be presented as an output
             */
            JavaPairRDD countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) ->
                (int) x + (int) y);
    
            countData.saveAsTextFile("count-data");
        }
    
        public static void main (String[] args)
        {
    
            if (args.length == 0) {
                System.out.println("No files provided.");
                System.exit(0);
            }
    
            wordCount(args[0]);
        }
    }

### Running the Application

    mvn exec:java -Dexec.mainClass=org.satish.spark.WordCounter -Dexec.args="input.txt"

    
