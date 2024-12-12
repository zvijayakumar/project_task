from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, when, lit

class CouncilsJob:

    def __init__(self):
        self.spark_session = (SparkSession.builder
                                          .master("local[*]")
                                          .appName("EnglandCouncilsJob")
                                          .getOrCreate())
        self.input_directory = "data"

    def extract_councils(self):
        # path pattern to match all council files
        path_pattern = f"{self.input_directory}/england_councils/*.csv"

        # read all files in one operation
        councils_df = self.spark_session.read.csv(path_pattern, header=True ,       inferSchema=True)

        # extract the file name to infer the council type
        councils_df = councils_df.withColumn(
            "file_name", regexp_extract(input_file_name(), r"([^/]+)\.csv$", 1)
        )

        # map file names to their respective council types using `when`
        councils_df = councils_df.withColumn(
            "council_type",
            when(councils_df["file_name"] == "district_councils", "District Council")
            .when(councils_df["file_name"] == "london_boroughs", "London Borough")
            .when(councils_df["file_name"] == "metropolitan_districts", "Metropolitan District")
            .when(councils_df["file_name"] == "unitary_authorities", "Unitary Authority")
            .otherwise("Unknown")
        )

        councils_df = councils_df.drop("file_name")
        #print(combined_councils_df.count())
        return councils_df
     
    def extract_avg_price(self):
        avg_price_path = f"{self.input_directory}/property_avg_price.csv"
        avg_price_df = (self.spark_session.read.csv(avg_price_path, header=True,inferSchema=True).selectExpr("local_authority as council", 
                                    "avg_price_nov_2019") )
        #print("count in avg price df",avg_price_df.count())
        return avg_price_df

    def extract_sales_volume(self):
        sales_volume_path = f"{self.input_directory}/property_sales_volume.csv"
        sales_volume_df = (self.spark_session.read.csv(sales_volume_path, header=True, inferSchema=True).selectExpr("local_authority as council", "sales_volume_sep_2019"))
        #print("count in sales price df",sales_volume_df.count())
        return sales_volume_df

    def transform(self, councils_df, avg_price_df, sales_volume_df):
        
        result_df = councils_df.join(avg_price_df, on="council", how= "left")
        result_df = result_df.join(sales_volume_df, on="council", how = "left")
        
        # select final required columns for final 
        final_df = result_df.select("council", 
                                    "county", 
                                    "council_type", 
                                    "avg_price_nov_2019", 
                                    "sales_volume_sep_2019")
        #print("final_df count",final_df.count())
        return final_df

    def run(self):
        return self.transform(self.extract_councils(),
                              self.extract_avg_price(),
                              self.extract_sales_volume())
