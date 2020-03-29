package com.example.livy

import com.holdenkarau.spark.testing._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest._

class WordCountSpec extends FunSpec with BeforeAndAfter with DatasetSuiteBase {
  lazy val session: SparkSession =
    SparkSession.builder().master("local").appName("test").config("spark.sql.shuffle.partitions", "1").getOrCreate()

  override def afterAll(): Unit = {
    session.stop
  }

  describe("test sample app") {

    // executeが正常に動作すること
    it("should work >=") {
      val ds = WordCount.executor()
      ds.show
    }

  }

}
