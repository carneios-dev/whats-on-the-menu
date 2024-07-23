import os
import pandas as pd

def clean_menuitem_dataframe(menuitem_df):
    # Trim and collapse whitespace in all columns.
    for col in menuitem_df.columns:
        menuitem_df[col] = menuitem_df[col].astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)

    # Convert `created_at` and `updated_at` columns to ISO 8601 datetime format.
    menuitem_df['created_at'] = pd.to_datetime(menuitem_df['created_at'], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S')
    menuitem_df['updated_at'] = pd.to_datetime(menuitem_df['updated_at'], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S')

    # Convert `price`, `high_price`, `dish_id` columns to numeric.
    menuitem_df['price'] = pd.to_numeric(menuitem_df['price'], errors='coerce')
    menuitem_df['high_price'] = pd.to_numeric(menuitem_df['high_price'], errors='coerce')
    menuitem_df['dish_id'] = pd.to_numeric(menuitem_df['dish_id'], errors='coerce')

    return menuitem_df

def clean_dish_dataframe(dish_df):
    # Convert `id`, `lowest_price`, and `highest_price` in the Dish DataFrame to numeric.
    dish_df['id'] = pd.to_numeric(dish_df['id'], errors='coerce')
    dish_df['lowest_price'] = pd.to_numeric(dish_df['lowest_price'], errors='coerce')
    dish_df['highest_price'] = pd.to_numeric(dish_df['highest_price'], errors='coerce')

    return dish_df

def merge_dataframes(menuitem_df, dish_df):
    # Merge the MenuItem DataFrame with the Dish DataFrame on `dish_id` and `id`, respectively.
    merged_df = menuitem_df.merge(dish_df[['id', 'lowest_price', 'highest_price']], left_on='dish_id', right_on='id', how='left')

    # Rename `id_x` (`id` from the MenuItem DataFrame) to `id` and drop `id_y` (`id` from the Dish DataFrame), from our result set.
    merged_df.rename(columns={'id_x': 'id'}, inplace=True)
    merged_df.drop(columns=['id_y'], inplace=True)

    # Remove rows where `dish_id` is empty or `NaN`.
    merged_df = merged_df.dropna(subset=['dish_id'])

    return merged_df

def process_menuitem_data():
    INPUT_FILE_MENUITEM = 'input/raw_MenuItem.csv'
    INPUT_FILE_DISH = 'output/cleaned_Dish.csv'
    OUTPUT_FILE = 'output/cleaned_MenuItem.csv'

    print("Processing menu item data...")

    menuitem_df = pd.read_csv(INPUT_FILE_MENUITEM)
    dish_df = pd.read_csv(INPUT_FILE_DISH)

    # Clean the MenuItem and Dish DataFrames and then merge them.
    menuitem_df = clean_menuitem_dataframe(menuitem_df)
    dish_df = clean_dish_dataframe(dish_df)
    merged_df = merge_dataframes(menuitem_df, dish_df)

    def impute_prices(row):
        # When `price` and `high_price` both exist, keep them as is.
        if pd.notna(row['price']) and pd.notna(row['high_price']):
            return row['price'], row['high_price']
        # When `high_price` is missing and `price` exists, set `high_price` to the `highest_price` value from `Dish`.
        if pd.notna(row['price']) and pd.isna(row['high_price']):
            return row['price'], row['highest_price']
        # When `price` is missing and `high_price` exists, set `price` to the value of `high_price`.
        if pd.isna(row['price']) and pd.notna(row['high_price']):
            return row['high_price'], row['high_price']
        # When both `price` and `high_price` are missing, set `price` to the median of `lowest_price` and `highest_price` from `Dish`, and set `high_price` to `highest_price`.
        if pd.isna(row['price']) and pd.isna(row['high_price']):
            median_price = (row['lowest_price'] + row['highest_price']) / 2
            return median_price, row['highest_price']
        return row['price'], row['high_price']
    
    def fix_high_price(row):
        # When `price` is higher than `high_price`, set `high_price` to the value of `price`.
        if row['price'] > row['high_price']:
            return row['price'], row['price']
        return row['price'], row['high_price']

    # Use the `impute_prices()` function to fill in missing values for `price` and `high_price`.
    merged_df['price'], merged_df['high_price'] = zip(*merged_df.apply(impute_prices, axis=1))

    # Use the `fix_price()` function to correct any instances where `price` is higher than `high_price`.
    merged_df['price'], merged_df['high_price'] = zip(*merged_df.apply(fix_high_price, axis=1))

    # Drop the `lowest_price` and `highest_price` columns from our result set, if they exist, from our Dish DataFrame.
    columns_to_drop = ['lowest_price', 'highest_price']
    columns_to_drop = [col for col in columns_to_drop if col in merged_df.columns]
    merged_df.drop(columns=columns_to_drop, inplace=True)

    # Remove duplicate rows.
    merged_df.drop_duplicates(inplace=True)

    # Save the finalized dataset to a new file, or replace it if it already exists.
    if os.path.exists(OUTPUT_FILE):
        print(f"Removing existing file: {OUTPUT_FILE}")
        os.remove(OUTPUT_FILE)
    merged_df.to_csv(OUTPUT_FILE, index=False)

    print("Menu item data processing complete. Inspection slice created.\n")

if __name__ == "__main__":
    process_menuitem_data()
