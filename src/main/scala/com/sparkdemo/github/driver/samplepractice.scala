package com.sparkdemo.github.driver


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


case class Address(city:String, state: String, country:String)
case class Student(name: String, id: String, address: Address, books: List[String] , addressold : List[Address])
case class Marks(subject:String, marks: String)

object MyPrac {
  
  def main(args : Array[String]) = {
  
  
  val students = List(new Student("ROOSE","568",new Address("chennai","TN","India"), List("physics","chemistry"),List(new Address("hyd","telegana","India"),new Address("mumbai","pune","India"))),
      new Student("KANNAN","600",new Address("banglaore","Karnataka","India"), List("maths","science"),List(new Address("hyd","telegana","India"))))
  
  val students1 = List(("roose","568",("chennai","TN","India")),("kannan","600",("bangalore","TN","India")))
  val students2 = List(("roose","568"),("kannan","600"))
  
  val spark = SparkSession.builder.appName("my new test").master("local").getOrCreate()
  
  val rdd = spark.sparkContext.parallelize(students)
  val rddstud1 = spark.sparkContext.parallelize(students1)
  val rddstud2 = spark.sparkContext.parallelize(students2)
  
  import spark.implicits._
  
  val df = rdd.toDF
  
  df.printSchema()
  //Practice
  
  // 1. Write udf to retrieve values from 2 columns and return concatenated value
  val convertCon = udf{ (name: String, id: String) => name + "-" + id }
  val dfnew = df.select(col("name"),col("id"),convertCon(col("name"),col("id")))
  dfnew.show
  //**********************************************************************************
   
  // 2. Write udf to send struct column value as row and retreive new value from udf
  //val structsize = udf{ (address:Row)  => address.getString(address.fieldIndex("city"))}
  val struct_column = udf{ (address:Row)  => address.getString(address.fieldIndex("country"))}
  val dfnew1 = df.select(col("name"),col("id"),struct_column(col("address")))
  dfnew1.show
  
  // 3. Write udf to send array column value as Seq[String] and return new struct value from udf
  //val structsize = udf{ (address:Row)  => address.getString(address.fieldIndex("city"))}
  val array_column = udf{ (books: Seq[String])  => books.size}
  val dfnew2 = df.select(col("name"),col("id"),array_column(col("books")).as("Total books"))
  dfnew2.show
  
  // 4. Write udf to send array of struct column as Seq[row] and retreive new value from udf
  val struct_array_column = udf{(oldaddress: Seq[Row]) => oldaddress.size}
  val dfnew3=df.select(col("name"),col("id"),struct_array_column(col("addressold")).as("Total old address"))
  dfnew3.show
  
  // 5.write udf to send few columns and return struct value
  val custom_type = udf{(name: String) => List(Marks("physiscs","90"),Marks("maths","100"))}
  val dfnew4=df.select(col("name"),custom_type(col("name")).as("marks"))
  dfnew4.printSchema()
  dfnew4.show
  
  // 6. pass entire row of df as arugment to udf function and retrieve value from row object
  val studentfunc = udf{ (student: Row) => displayStudent(student) }
  val dfnewtest = df.select(studentfunc(struct("*")))
  dfnewtest.show
  
  //5. create custom objects for each row object of rdd and invoke objects method to display the return value of the function 
  val enrolllist = rdd.map( f => createEnrollment(f))
  enrolllist.map(h => h.getNo()).collect.foreach(println)
  
   // 6. Create dataframe without specifying schema
  val dfstudents = spark.createDataFrame(students)
  dfstudents.show
  
  //7. Create empty dataframe with schema
  val fields = List(StructField("name",StringType, true),StructField("id",StringType,true))
  //val fields1 = List(StructField("name",StringType, true),StructField("id",StringType,true), StructField("id",Address,true))
  val schema = StructType(fields)
  val newdf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
  newdf.show()
  
  // 8. Create dataframe with schema.
  //val rddrow = rdd.map( g => Row(g.name,g.id,g.books, g.address, g.addressold))
  val rddrow = rddstud1.map( g => Row(g._1,g._2,g._3))
  val dfstud2 = spark.createDataFrame(rddrow,schema)
  dfstud2.show()
  
  // 9. Select only few columns by providing the list holding list of columns
  val flist = List(col("name"),col("id"))
  val dflist = List(("roose","568","tn"),("vignesh","561","kar")).toDF("name","id","state")
  val dflistnew = dflist.select(flist:_*)
  dflistnew.show


    val travelDF = spark.read.option("delimiter",",").csv("dbfs:/FileStore/tables/inputdata/TRIPS_MOCK_DATA1.txt")
    //10. Apply regular column function (alternative of udf function)
    def stripSpecialCharcters(colname:String) : Column = {
      regexp_replace(col(colname),"Danita","Roose").as(colname)
    }
    travelDF.select(travelDF.columns.toList.map(stripSpecialCharcters):_*).show

    //11. Apply anonymous column function (alternative of udf function)
    val stripSpecialChar  = (colname:String)  => regexp_replace(col(colname),"Danita","Roose").as(colname)
    travelDF.select(travelDF.columns.toList.map(stripSpecialChar):_*).show
  }

  def createEnrollment(student: Student) : Enroll = {
    val enrobj = new Enroll(student.name,student.id)
    enrobj.incEnrollNo()
    enrobj
  }

  def displayStudent(student: Row) : Address = {
    return student.getAs[Address](student.fieldIndex("address"))
  }
}

