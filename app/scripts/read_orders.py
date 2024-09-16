import pandas as pd


def read_orders(file_path):
    # Read the Excel file
    orders_df = pd.read_excel(file_path, engine='openpyxl')

    # Display the first few rows to understand the structure
    print(orders_df.head())

    # Perform any necessary preprocessing steps
    # Example: Handling missing values
    orders_df.fillna(0, inplace=True)

    # Example: Converting date columns to datetime format
    if 'Order Date' in orders_df.columns:
        orders_df['Order Date'] = pd.to_datetime(orders_df['Order Date'])

    # Example: Extracting useful features
    if 'Order Date' in orders_df.columns:
        orders_df['Year'] = orders_df['Order Date'].dt.year
        orders_df['Month'] = orders_df['Order Date'].dt.month

    return orders_df


if __name__ == "__main__":
    order_details_path = 'data/OrderData.xlsx'
    orders_data_df = read_orders(order_details_path)
    print(orders_data_df.head())