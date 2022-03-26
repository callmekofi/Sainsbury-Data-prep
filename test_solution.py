import unittest
import pandas as pd
from solution_start import *


class TestFunctions(unittest.TestCase):

    def test_number_of_customers(self):
        filepath = "/Users/kofijackson/Desktop/Git_Projects/input_data_generator/input_data/starter/customers.csv"
        customer_data = read_csv(filepath)
        total_customers = get_number_customers(customer_data)

        return self.assertEqual(total_customers, 137)


    def test_number_of_products(self):
        filepath = "/Users/kofijackson/Desktop/Git_Projects/input_data_generator/input_data/starter/products.csv"
        products_data = read_csv(filepath)
        total_products = get_number_products(products_data)

        return self.assertEqual(total_products, 64)


    def test_get_latest_date(self):
        transactions_df = pd.concat(
            [
                pd.read_json(
                    "{\"customer_id\": \"C6\", \"basket\": [{\"product_id\": \"P53\", \"price\": 476}, {\"product_id\": \"P42\", \"price\": 1937}, {\"product_id\": \"P43\", \"price\": 1019}], \"date_of_purchase\": \"2018-12-03 17:52:00\"}"),
                pd.read_json(
                    "{\"customer_id\": \"C125\", \"basket\": [{\"product_id\": \"P28\", \"price\": 1752}], \"date_of_purchase\": \"2019-01-27 08:23:00\"}"),
                pd.read_json(
                    "{\"customer_id\": \"C76\", \"basket\": [{\"product_id\": \"P39\", \"price\": 1033}], \"date_of_purchase\": \"2019-02-27 13:55:00\"}")
            ]
        )

        result = get_latest_transaction_date(transactions_df)
        assert result.date_of_purchase[0] == "2019-02-27 13:55:00"


if __name__ == "__main__":
        unittest.main()
