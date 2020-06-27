package com.sparkdemo.github.driver

import org.apache.spark.sql.Row

object Enroll {
  var enrollno: Integer = 0
}

class Enroll(name:String, id:String)  {
  val test2 = new String()
  def getNo() : Integer = {
    return Enroll.enrollno
  }
  // def getenroll
  def incEnrollNo() = {
    Enroll.enrollno=Enroll.enrollno+1
  }
}