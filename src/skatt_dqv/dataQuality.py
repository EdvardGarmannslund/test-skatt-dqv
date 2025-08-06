#Python imports
from dataclasses import dataclass
from typing import Callable, Any
from datetime import datetime
#Pyspark imports
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
#Eksterne imports
import pandas as pd


@dataclass
class expectation:
    """Class for expectations.
    
    This class allows you to define an expectation for a column. The expectation can be reused across data products. The idea is that you can define column and rows of data that should be tested. What testing logic should apply and optional args.

    Attributes:
        expectation_name: String containing expectation name.
        expectation_test: Callable function containing test logic. This is seperate from a expectation. This could be logic for a regex check, the args is sent in seperately.
        expecatation_id: String for expectation id. Ideally unique so that metadata about an expectation can be stored seperately and be queried.
        regex: String for regex.
        sql_filter: SQL filter for fitlering which rows of data should be tested. Has to follow common sql syntax from where statement or pyspark.sql syntax.
        args: optional Args like a value to match.
        path: Optional path, if test requires seperate data.
        value_set: List with values that a column can contain.
    """
    expectation_name: str
    expectation_test: Callable
    expectation_id: str
    expectation_description: str = None
    quality_dimension: str = None
    regex: str = None
    sql_filter: str = None
    args: str = None
    path: str = None
    value_set: list[str] = None

    def test_expectation(self, df: DataFrame, uuid_columns: list[str], column: str)->DataFrame:
        """Function to test spark df.
        
        Args:
            df: Spark sql dataframe.
            uuid_columns: List with columns that should be kept from the original df in the result df.
            column: Column that should be tested for the expectation.

        Returns:
            DataFrame: Spark sql dataframe that
            """

        if self.sql_filter is not None:
            df = df.filter(f.expr(self.sql_filter))

        rows_bef = df.count()

        df = self.expectation_test(self,df,column)
        
        df = df.withColumn("testet_verdi",f.col(column).cast("string"))

        df = df.select(*uuid_columns)
        df = df.withColumn("forventning_id",f.lit(self.expectation_id))
        df = df.withColumn("kolonne",f.lit(column))

        rows_aft = df.count()

        if rows_bef == 0:
            result_agg = {
                "forventning_id":self.expectation_id,
                "kolonne":column,
                "resultat":1,
                "dimensjon":self.quality_dimension
                }
            print(f"Rader som blir testet for test {self.expectation_id} er null, dette boor dobbeltsjekkes.")
        else:
            result_agg = {
                "forventning_id":self.expectation_id,
                "kolonne":column,
                "resultat":1-(rows_aft/rows_bef),
                "dimensjon":self.quality_dimension
                }

        return df, result_agg


@dataclass
class data_quality_test:
    """Dataklasse for data validerings kjøring av et dataprodukt.
    
    Args:
        spark_df (DataFrame): Hvor man skal hente dataprodukt fra.
        spark_df_result (DataFrame): Hvor man skal lagre resultat av kjøring.
        column_expectations (dict[str,Callable]): Ordbok med kolonnenavn og hvilken forventning som skal testes.
        uuid_columns (list[str]): List with column that should be kept in result table.
        spark: Spark context.
    """
    spark_df: DataFrame
    column_expectations: dict[str,Callable]
    uuid_columns: list[str]

    def run_validation(self):
        """Method to run data validation."""
        #Setter resultat df til None.
        reference_df = None
        validation_df = None
        result_agg = []
        #Går gjennom kollonner og tester som kollonnene skal testes ppå.

        for column in self.column_expectations.keys():
            for expectation_def in self.column_expectations[column]:

                
                reference_row = pd.DataFrame(
                    {
                        "forventning_id":[expectation_def.expectation_id],
                        "kolonne":[column],
                        "dimensjon":[expectation_def.quality_dimension],
                        "forventning":[expectation_def.expectation_description]
                    }
                )
                #Create reference table.
                if reference_df is None:
                    reference_df = reference_row
                else:
                    reference_df = pd.concat([reference_df,reference_row])
                



                
                #Får tilbake rader som er ugyldige
                tested_df, result_dict = expectation_def.test_expectation(self.spark_df,self.uuid_columns,column)

                #Resultat aggregert.
                result_agg.append(result_dict)

                #Alle rader som feilet.
                if validation_df is None:
                    validation_df = tested_df
                #Dersom resultat df eksisterer legg til nye rader med data for annen test.
                else:
                    validation_df = validation_df.unionByName(tested_df, allowMissingColumns=True)
        #Lager pandas dataramme av aggregerte testresultater.
        agg_df = pd.DataFrame(result_agg)
        #Setter inn timestamp for datakvalitetskjoringen.
        now = datetime.now()
        timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
        agg_df["testkjoering_tidspunkt"] = timestamp

        return validation_df, agg_df, reference_df

