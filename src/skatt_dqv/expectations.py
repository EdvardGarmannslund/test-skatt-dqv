#Local imports
from .dataQuality import expectation
#Pyspark imports
from pyspark.sql import functions as f
from pyspark.sql import DataFrame
#Databricks imports
#from databricks.sdk.runtime import * # Dette gjor at python kode i denne filen har tilgang til spark context. Dette er ikke ideelt, skal endres paa sikt.


def value_matches_regex(expectation:expectation,df:DataFrame,column:str) -> DataFrame:
    """Checks rows for data that does not match regex.

    Args:
        expectation (expectation): Expectation test of class expectation. This defines what and how the expectation test should work.
        df (DataFrame): Spark df with the data that is tested.
        column (str): String with column name that is tested.
        
    Returns:
        DataFrame: Spark df with rows that does not pass the expectation test.
    """
    df = df.filter(
        ~f.col(column).rlike(expectation.regex)
        )

    return df

def value_not_null(expectation,df,column):
    """Checks rows for data that is not null.

    Args:
        expectation (expectation): Expectation test of class expectation. This defines what and how the expectation test should work.
        df (DataFrame): Spark df with the data that is tested.
        column (str): String with column name that is tested.
        
    Returns:
        DataFrame: Spark df with rows that does not pass the expectation test.
    """
    df = df.filter(f.col(column).isNull())

    return df

def value_in_set(expectation,df,column):
    """Checks rows for data that is not in set.

    Args:
        expectation (expectation): Expectation test of class expectation. This defines what and how the expectation test should work.
        df (DataFrame): Spark df with the data that is tested.
        column (str): String with column name that is tested.
        
    Returns:
        DataFrame: Spark df with rows that does not pass the expectation test.
    """
    df = df.filter(
        ~f.col(column).isin(expectation.value_set)
    )

    return df

def value_is_bigger_then(expectation,df,column):
    """Checks rows for data that is bigger than set value.

    Args:
        expectation (expectation): Expectation test of class expectation. This defines what and how the expectation test should work.
        df (DataFrame): Spark df with the data that is tested.
        column (str): String with column name that is tested.
        
    Returns:
        DataFrame: Spark df with rows that does not pass the expectation test.
    """
    df = df.filter(
        f"{column}>{expectation.args}"
    )
    return df

def value_is_less_then(expectation,df,column):
    """Checks rows for data that is not in set.

    Args:
        expectation (expectation): Expectation test of class expectation. This defines what and how the expectation test should work.
        df (DataFrame): Spark df with the data that is tested.
        column (str): String with column name that is tested.
        
    Returns:
        DataFrame: Spark df with rows that does not pass the expectation test.
    """
    df = df.filter(
        f"{column}<{expectation.args}"
    )
    return df

def check_row_sql_filter(expectation,df,column):
    """Checks rows for data that does not match sql filter.

    Args:
        expectation (expectation): Expectation test of class expectation. This defines what and how the expectation test should work.
        df (DataFrame): Spark df with the data that is tested.
        column (str): String with column name that is tested.
        
    Returns:
        DataFrame: Spark df with rows that does not pass the expectation test.
    """
    df = df.filter(expectation.args)

    return df 

def check_delivery_for_valid_tax_year(expectation,df,column):
    """Checks rows for data that does not have delivery i time for valid tax year.

    Args:
        expectation (expectation): Expectation test of class expectation. This defines what and how the expectation test should work.
        df (DataFrame): Spark df with the data that is tested.
        column (str): String with column name that is tested.
        
    Returns:
        DataFrame: Spark df with rows that does not pass the expectation test.
    """
    # HUSK Å ENDRE header_gjelderInntektsaar til et innsendt arg
    filter_arg = f"TO_DATE({column}) > CONCAT(CAST(header_gjelderInntektsaar + 1 AS STRING),'{expectation.args}')"

    df = df.filter(filter_arg)

    return df

def check_if_value_is_distinct(expectation,df,column):
    """Checks if value is distinct. If args is included then assumes a tupple of columns that is included in distinct logic.
    
    Args:
        expectation (expectation): Expectation test of class expectation. This defines what and how the expectation test should work.
        df (DataFrame): Spark df with the data that is tested.
        column (str): String with column name that is tested.
        
    Returns:
        DataFrame: Spark df with rows that does not pass the expectation test.
    """
    df_counts = df.groupBy(column, *expectation.args).agg(f.count("*").alias("count"))

    df_non_distinct = df_counts.filter("count > 1").drop("count")

    df_filtered = df.join(df_non_distinct, on=[column, *expectation.args], how="inner")

    return df_filtered


def test_modulus_11_org(expectation,df,column):
    """
    Function to validate Norwegian organization numbers using modulus 11.
    
    Args:
        df (DataFrame): A PySpark DataFrame with columns 'id' and 'orgnr'.
        
    Returns:
        DataFrame: A filtered DataFrame where valid numbers pass the modulus 11 check.
    """
    # Keep only 9-digit orgnr rows
    df_not_9 = df.filter(f.expr(f"LENGTH({column}) != 9"))
    df = df.filter(f.expr(f"LENGTH({column}) = 9"))
    
    # Extract individual digits from orgnr
    for i in range(9):
        df = df.withColumn(f"val_{i}", f.col(column).substr(i+1, 1).cast("int"))
    
    # Define weights for the first 8 digits
    weights = [3, 2, 7, 6, 5, 4, 3, 2]
    
    # Multiply digits by weights
    for i in range(8):
        df = df.withColumn(f"val_{i}", f.col(f"val_{i}") * weights[i])
    
    # Calculate the sum of weighted values
    df = df.withColumn("sum", sum(f.col(f"val_{i}") for i in range(8)))
    
    # Compute rest (mod 11)
    df = df.withColumn("rest", f.col("sum") % 11)
    
    # Compute kontrol (11 - rest)
    df = df.withColumn("kontrol", (11 - f.col("rest")))
    df = df.withColumn("kontrol", f.when(f.col("kontrol") == 11, 0).otherwise(f.col("kontrol")))
    
    # Filter only valid rows where kontrol matches val_8
    df = df.filter(f.col("kontrol") != f.col("val_8"))
    
    df = df.unionByName(df_not_9,allowMissingColumns=True)
    
    return df

def test_modulus_11_fnr(expectation,df,column):
    """
    Function to validate Norwegian personal id numbers using modulus 11.
    
    Args:
        df (DataFrame): A PySpark DataFrame with columns 'id' and 'orgnr'.
        
    Returns:
        DataFrame: A filtered DataFrame where valid numbers pass the modulus 11 check.
    """
    # Keep only 9-digit orgnr rows

    df_not_11 = df.filter(f.expr(f"LENGTH({column}) != 11"))
    df = df.filter(f.expr(f"LENGTH({column}) = 11"))
    
    # Extract individual digits from orgnr
    for i in range(11):
        df = df.withColumn(f"val_{i}", f.col(column).substr(i+1, 1).cast("int"))
    
    # Define weights for the first 8 digits
    weights1 = [3,7,6,1,8,9,4,5,2]
    weights2 = [5,4,3,2,7,6,5,4,3,2]
    
    # Multiply digits by weights
    for i in range(9):
        df = df.withColumn(f"val1_{i}", f.col(f"val_{i}") * weights1[i])

    for i in range(10):
        df = df.withColumn(f"val2_{i}", f.col(f"val_{i}") * weights2[i])
    
    # Calculate the sum of weighted values
    df = df.withColumn("sum1", sum(f.col(f"val1_{i}") for i in range(9)))
    df = df.withColumn("sum2", sum(f.col(f"val2_{i}") for i in range(10)))
    # Compute rest (mod 11)
    df = df.withColumn("rest1", f.col("sum1") % 11)
    df = df.withColumn("rest2", f.col("sum2") % 11)
    # Compute kontrol (11 - rest)
    df = df.withColumn("kontrol1", (11 - f.col("rest1")))
    df = df.withColumn("kontrol1", f.when(f.col("kontrol1") == 11, 0).otherwise(f.col("kontrol1")))
    df = df.withColumn("kontrol2", (11 - f.col("rest2")))
    df = df.withColumn("kontrol2", f.when(f.col("kontrol2") == 11, 0).otherwise(f.col("kontrol2")))
    # Filter only valid rows where kontrol matches val_8
    df = df.filter(f.col("kontrol1") != f.col("val_9"))
    df = df.filter(f.col("kontrol1") != f.col("val_10"))
    
    df = df.unionByName(df_not_11,allowMissingColumns=True)
    
    return df


def check_if_id_in_master_data(expectation,df,column):
    """Function to check if id is in master data/population.
    
    Args:
        expectation (expectation): Expectation test of class expectation. This defines what and how the expectation test should work.
        df (DataFrame): Spark df with the data that is tested.
        column (str): String with column name that is tested.
        
    Returns:
        DataFrame: Spark df with rows that does not pass the expectation test.
    """
    
    df_pop = spark.read.table(expectation.path)
    df_pop = df_pop.withColumnRenamed("primaeridentifikator_verdi", f"{expectation.args}")

    df = df.join(df_pop,on=expectation.args,how="left_anti")

    return df


def check_if_data_keeps_coming_after_close(expectation, df, column):
    """Function to check if data keeps coming after a unit should be shut down.
    
    Args:
        expectation (expectation): Expectation test of class expectation. This defines what and how the expectation test should work.
        df (DataFrame): Spark df with the data that is tested.
        column (str): String with column name that is tested.
        
    Returns:
        DataFrame: Spark df with rows that does not pass the expectation test.
    """
    df.createOrReplaceTempView("temp")

    df = spark.sql(
        f"""
        select a.*
        from temp as a
        where a.{column} = true
        and exists (
            select 1
            from temp as b
            where b.{expectation.args['id']} = a.{expectation.args['id']}
            and b.{expectation.args['date']} > a.{expectation.args['date']}

        )
        """
    )

    return df

