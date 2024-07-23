import os
import pandas as pd

def process_menu_data():
    INPUT_FILE = 'input/refined_Menu.csv'
    OUTPUT_FILE = 'output/cleaned_Menu.csv'

    print("Processing menu data...")

    menu_df = pd.read_csv(INPUT_FILE)

    # Find the most common values for `currency` and `currency_symbol`.
    most_common_currency = menu_df['currency'].mode()[0] if not menu_df['currency'].mode().empty else 'DOLLARS'
    most_common_currency_symbol = menu_df['currency_symbol'].mode()[0] if not menu_df['currency_symbol'].mode().empty else '$'
    
    # Replace blank values in currency and currency_symbol with the most common values.
    menu_df['currency'] = menu_df['currency'].fillna(most_common_currency)
    menu_df['currency_symbol'] = menu_df['currency_symbol'].fillna(most_common_currency_symbol)

    # Remove rows where all crucial columns are missing.
    crucial_columns = ['sponsor', 'event', 'venue', 'place']
    menu_df = menu_df.dropna(subset=crucial_columns, how='all')

    # Remove duplicate rows.
    menu_df.drop_duplicates(inplace=True)

    # Save the finalized dataset to a new file, or replace it if it already exists.
    if os.path.exists(OUTPUT_FILE):
        print(f"Removing existing file: {OUTPUT_FILE}")
        os.remove(OUTPUT_FILE)
    menu_df.to_csv(OUTPUT_FILE, index=False)

    print("Menu data processing complete.\n")

if __name__ == "__main__":
    process_menu_data()
