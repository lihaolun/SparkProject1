package business

import com.atguigu.sparkmall.common.util.ConfigUtil
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AreaTop3ProductApp {
  def main(args: Array[String]): Unit = {
    //需求6.各区域点击量Top3统计
    val sparkConf: SparkConf = new SparkConf().setAppName("area_top3").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    sparkSession.udf.register("city_remark", new CityRemarkUDAF())

    val config: FileBasedConfiguration = ConfigUtil("config.properties").config
    val databaseName: String = config.getString("hive.database")

    sparkSession.sql("use " + databaseName)

    sparkSession.sql(" select c.area,p.product_id,p.product_name,city_name from user_visit_action v  join  city_info c  on v.city_id=c.city_id   join  product_info p on p.product_id=v.click_product_id ").createOrReplaceTempView("area_product_click_detail")

    sparkSession.sql("select  area,product_id,product_name,count(*) clickcount,city_remark(city_name) cityremark  from   area_product_click_detail group by  area,product_id,product_name ").createOrReplaceTempView("area_product_click_count")
    // 利用开窗函数进行分组排序 =》 截取所有分组中前三名
    sparkSession.sql("select area,product_id,product_name,clickcount,cityremark from(select area,product_id,product_name,clickcount,rank() over(partition by area order by clickcount desc)rk,cityremark from area_product_click_count) where rk<=3").show(100,false)
  }
}
