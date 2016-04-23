/*-----------------------------------------------------------------------------

                   Spark with Scala

             Copyright : V2 Maestros @2016
                    
Code Samples : Spark SQL 
-----------------------------------------------------------------------------
*/

//val datadir = "C:/Personal/V2Maestros/Courses/Big Data Analytics with Spark/Scala"

val datadir = "C:/Users/David/Documents/udemy_spark_sql/BDAS-S-Resource-Bundle/RB-Scala"

//Create a SQL Context from Spark context
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

//............................................................................
////   Working with Data Frames
//............................................................................

//Create a data frame from a JSON file
val empDf = sqlContext.read.json(datadir + "/customerData.json")
empDf.show()
empDf.printSchema()

//Do SQL queries
empDf.select("name").show()
empDf.filter(empDf("age") === 40 ).show()
empDf.groupBy("gender").count().show()
empDf.groupBy("deptid").agg(avg("salary"), max("age")).show()

//create a data frame from a list
val deptList = Array("{'name': 'Sales', 'id': '100'}",
     "{ 'name':'Engineering','id':'200' }")
//Convert list to RDD
val deptRDD = sc.parallelize(deptList)
//Load RDD into a data frame
val deptDf = sqlContext.read.json(deptRDD)
deptDf.show()
 
//join the data frames
 empDf.join(deptDf, empDf("deptid") === deptDf("id")).show()
 
//cascading operations
empDf.filter(empDf("age") >30).join(deptDf, 
        empDf("deptid") === deptDf("id")).
        groupBy("deptid").
        agg(avg("salary"), max("age")).show()

//register a data frame as table and run SQL statements against it
empDf.registerTempTable("employees")
sqlContext.sql("select * from employees where salary > 4000").show()

//............................................................................
////   Working with Databases
//............................................................................
//Make sure that the spark classpaths are set appropriately in the 
//spark-defaults.conf file to include the driver files
    
val demoDf = sqlContext.read.format("jdbc").options(
    Map("url" -> "jdbc:mysql://localhost:3306/demo",
    "driver" -> "com.mysql.jdbc.Driver",
    "dbtable" -> "demotable",
    "user" ->"root",
    "password" -> "")).load()
    
demoDf.show()

//............................................................................
////   Creating data frames from RDD
//............................................................................

val lines = sc.textFile(datadir + "/auto-data.csv")
//remove the first line
val datalines = lines.filter(x =>  ! x.contains("FUELTYPE"))
datalines.count()

val rowRDD = datalines.map(x => x.split(",")).map(
                    x => (x(0),x(4),x(7)) )

val autoDF = rowRDD.toDF("make","type","hp")
autoDF.select("make","hp").show()

//............................................................................
////   Creating data frames from RDD
//............................................................................

val lines = sc.textFile(datadir + "/insurance_data.csv")
//remove the first line
val datalines = lines.filter(x =>  ! x.contains("Treatment"))
datalines.count()

val rowRDD = datalines.map(x => x.split(",")).map(
                    x => (x(0),x(1),x(2),x(3),x(4),x(5)) )

val insuranceDF = rowRDD.toDF("date","numOfpatient","treatment","drug","insurance","patientAvgCost");
insuranceDF.select("date","numOfpatient").show()
insuranceDF.registerTempTable("patients")
sqlContext.sql("select * from patients where drug = 'D6' ").show()
sqlContext.sql("select drug, sum(patientAvgCost) Total from patients group by drug").show()
sqlContext.sql("select treatment, drug, sum(patientAvgCost+insurance) Total from patients group by treatment, drug").show()

