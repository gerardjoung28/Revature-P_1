import org.apache.spark.sql.SparkSession

object P1 {


  def main(args: Array[String]): Unit = {

   // System.setProperty("hadoop.home.dir", "C:\\winutils")
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    spark.sparkContext.setLogLevel("ERROR")

    val passwor = "temporal"
    var userinput = ""

    userinput = scala.io.StdIn.readLine("Enter password for Admin:")

    if (userinput == passwor) {

      // spark.sql("create table if not exists covid1999(location String,date String, vaccine String, total_vaccinations Int) ");
      // spark.sql("LOAD DATA LOCAL INPATH 'src/main/scala/input/country_vaccinations_by_manufacturer.csv' INTO TABLE covid1999")
      spark.sql("select * from covid199").show()


      //Query 1
      // What is the total of people vaccinated in the US

      println("What is the total of people vaccinated in the US  ")
      //spark.sql("create view temp0 as select * from covid199 where location = 'United States' ")
      spark.sql("select cast(sum(total_vaccinations)as int) as total_in_US from temp0 where total_vaccinations < 2000000").show()



      //Query 2,3
      // How many got the Moderna and Pfizer vaccine
      println(" How many people in the world got the Moderna and Pfizer vaccine ")
      //spark.sql("create view temp6 as select * from covid199 where vaccine = 'Moderna' ")
      spark.sql("SELECT cast(SUM(total_vaccinations) as int) as moderna from temp6 where total_vaccinations < 200000 ").show()
      //spark.sql("create view temp7 as select * from covid199 where vaccine = 'Pfizer/BioNTech' ")
      spark.sql("SELECT cast(SUM(total_vaccinations) as int) as pfizer from temp7 where total_vaccinations < 20000 ").show()

      // Query 4
      // How many people are vaccinated globally
      println("How many people are vaccinated globally")
      spark.sql("select cast(sum(total_vaccinations) as int) as total from covid199 where total_vaccinations < 200000").show()

      //Query 5


      //Future Query
      // How many people vaccinated in the next sixth month in the US

      println("How many people will be vaccinated in the next sixth month in the US")
      spark.sql("select * from temp2")
      //spark.sql("create view temp5 as select * from temp2 where date between '2021-01-12' and '2021-03-12'")
      spark.sql("select * from temp5")
      spark.sql("select cast(sum(total_vaccinations)*2 as int) as prediction from temp5 where total_vaccinations < 2000000").show()

    }
    else {println("Invalid input")
       0}

  }


}
