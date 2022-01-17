import argparse
import glob
import pandas as pd
from pathlib import Path


# Read csv files
def read_csv(csv_filepath: str):
    return pd.read_csv(csv_filepath, header=0)

# Read json files
def read_json_folder(json_filepath: str):
    transaction_files = Path(json_filepath).glob('**/*.json')

    return pd.concat(pd.read_json(f, lines=True) for f in transaction_files)


def run_transformations(customers_filepath: str, products_filepath: str, transactions_filepath: str, output_location: str):
    customers_df = read_csv(customers_filepath)

    # Modify products_df
    products_df = read_csv(products_filepath)
    product_list = []
    for row in products_df.iterrows():
        value = row[1][0], row[1][1], row[1][2], row[1][2][0].upper()
        product_list.append(value)

    # Convert list to dataframe and rename columns
    products_df = pd.DataFrame(product_list)
    products_df.columns = ('product_id', 'product_description', 'product_category', 'product_cat')


    # Modify transactions_df to 1NF
    transactions_df = read_json_folder(transactions_filepath)
    transactions_list = []
    for row in transactions_df.iterrows():
        for i in row[1][1]:
            value = row[1][0], i['product_id'], i['price'], row[1][2]
            transactions_list.append(value)

    # Convert list to dataframe and rename columns
    transactions_df = pd.DataFrame(transactions_list)
    transactions_df.columns = ('customer_id', 'product_id', 'price', 'date_of_purchase')

    #customer_groupby function
    customer_group = transactions_df.groupby(['customer_id'])

    # Create dataframe from grouping product_id count on customer
    transaction_history = pd.DataFrame(customer_group['product_id'].value_counts())
    transactions_list = []
    for row in transaction_history.iterrows():
        value = row[0][0], row[0][1], row[1][0]
        transactions_list.append(value)

    transaction_history = pd.DataFrame(transactions_list)
    transaction_history.columns = ('customer_id', 'product_id', 'total_purchase')

    # Merge transactions_history with customers_df and products_df
    transaction_history = pd.merge(transaction_history, customers_df, on='customer_id')
    transaction_history = pd.merge(transaction_history, products_df, on='product_id')

    # Retrieve relevant rows and sort by customer_id and product_id
    transaction_history = transaction_history[['customer_id', 'loyalty_score', 'product_id', 'product_cat', 'total_purchase']].sort_values(by=['customer_id', 'product_id'])
    transaction_history['index'] = range(1, len(transaction_history.index) + 1)
    transaction_history.set_index('index', inplace = True)

    # Save transaction_history to output file
    transaction_history.to_csv(output_location+'transaction_history', header=True, index=None)

    return get_latest_transaction_date(transactions_df), get_number_customers(customers_df), get_number_products(products_df)


def get_latest_transaction_date(transactions):
    latest_purchase = transactions.date_of_purchase.max()
    latest_transaction = transactions[transactions.date_of_purchase == latest_purchase]
    return latest_transaction


def get_number_customers(customer_df):
    total_customers = customer_df.count()
    return total_customers[0]


def get_number_products(product_df):
    total_products = product_df.count()
    return total_products[0]


def to_canonical_date_str(date_to_transform):
    return date_to_transform.strftime('%Y-%m-%d')


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Transations_dataset')
    parser.add_argument('--customers_filepath', required=False, default="/Users/emmanuelsifah/Desktop/Git_Projects/input_data_generator/input_data/starter/customers.csv")
    parser.add_argument('--products_filepath', required=False, default="/Users/emmanuelsifah/Desktop/Git_Projects/input_data_generator/input_data/starter/products.csv")
    parser.add_argument('--transactions_filepath', required=False, default="/Users/emmanuelsifah/Desktop/Git_Projects/input_data_generator/input_data/starter/transactions")
    parser.add_argument('--output_location', required=False, default="/Users/emmanuelsifah/Desktop/Git_Projects/input_data_generator/output")
    args = vars(parser.parse_args())

    run_transformations(args['customers_filepath'], args['products_filepath'], args['transactions_filepath'], args['output_location'])
