import unittest
from PracticeRepo.pyspark_practice.util import *

class MyTestCase(unittest.TestCase):
    spark = create_SparkSession()
    def test_something(self):
        schema = StructType([
            StructField('Id', IntegerType(), True),
            StructField('Name', StringType(), True),
            StructField('Department', StringType(), True),
            StructField('Salary', IntegerType(), True)
        ])

        data = [(1, 'ra', 'sales', 6000),
                (2, 'Ki', 'finance', 7500),
                (3, 'Um', 'finance', 6900),
                (4, 'Aj', 'Marketing', 8100),
                (5, 'Lo', 'sales', 9600),
                (6, 'Ga', 'Marketing', 7800)]
        employee_df = create_dataframe(self.spark,data, schema)

        #salary_increment
        expected_schema=StructType([
            StructField('Id', IntegerType(), True),
            StructField('Name', StringType(), True),
            StructField('Department', StringType(), True),
            StructField('Salary', IntegerType(), True),
            StructField('Bonus',IntegerType(),True)
        ])

        expected_data=[(1, 'ra', 'sales', 6000,60000),
                (2, 'Ki', 'finance', 7500,75000),
                (3, 'Um', 'finance', 6900,69000),
                (4, 'Aj', 'Marketing', 8100,81000),
                (5, 'Lo', 'sales', 9600,96000),
                (6, 'Ga', 'Marketing', 7800,78000)]

        expected_employee_df=create_dataframe(self.spark,data,schema)
        actual_employee_df=select(employee_df,employee_df.Id,employee_df.Name,employee_df.Department,employee_df.Salary,employee_df.Bonus)

        self.assertEqual(actual_employee_df.collect(),expected_employee_df.collect())  # add assertion here


if __name__ == '__main__':
    unittest.main()
