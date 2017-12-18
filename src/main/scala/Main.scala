import java.io._

import org.apache.spark._


object Main {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SparkAverageRatingByGender").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val rating_text = sc.textFile("input/ratings.data")
//    val rating_text = sc.textFile(args(0))
//    val user_text = sc.textFile(args(1))
    val user_text = sc.textFile("input/users.data")

    case class rating(UserID:Int,MovieID:Int,Rating:Int,TimeStamp:Int)
    case class user(UserID:Int,Gender:String,Age:Int,Occupation:String,Zip:String)

    val rating_split = rating_text.map(line=>line.split("::")).map(line=>(rating(line(0).toInt,line(1).toInt,line(2).toInt,line(3).toInt)))
    val user_split = user_text.map(line=>line.split("::")).map(line=>(user(line(0).toInt,line(1),line(2).toInt,line(3),line(4))))

    val rating_item = rating_split.map(x =>(x.UserID,x))
    val user_item = user_split.map(x =>(x.UserID,x))

    val join_table = rating_item.join(user_item)
    val joined_table = join_table.map(x=>(x._2._1.MovieID,x._2._2.Gender,x._2._1.Rating))

    val avgVal = joined_table.map{ case(key,key1,value) => ((key,key1),(value,1.toDouble))}
      .reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))
      .mapValues{ case(sum,value) => sum / value }

    new File("Output.txt" ).delete()
    val pw = new PrintWriter(new File("Output.txt" ))
    val data=avgVal.sortByKey().collect()

    data.foreach(x=>pw.println("%d,%s,%s" format (x._1._1,x._1._2,BigDecimal(x._2).setScale(11, BigDecimal.RoundingMode.CEILING).toDouble)))
    pw.close()

  }
}
