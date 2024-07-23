import os
import pandas as pd

def process_dish_data():
    INPUT_FILE = 'input/refined_Dish.csv'
    OUTPUT_FILE = 'output/cleaned_Dish.csv'

    print("Processing dish data...")

    dish_df = pd.read_csv(INPUT_FILE)

    # Calculate mean and round to two decimal places.
    mean_lowest_price = round(dish_df['lowest_price'].mean(), 2)
    mean_highest_price = round(dish_df['highest_price'].mean(), 2)

    # Fill in missing values with the mean of the column.
    dish_df['lowest_price'] = dish_df['lowest_price'].fillna(mean_lowest_price)
    dish_df['highest_price'] = dish_df['highest_price'].fillna(mean_highest_price)

    # Remove duplicate rows.
    dish_df.drop_duplicates(inplace=True)

    # Save the finalized dataset to a new file, or replace it if it already exists.
    if os.path.exists(OUTPUT_FILE):
        print(f"Removing existing file: {OUTPUT_FILE}")
        os.remove(OUTPUT_FILE)
    dish_df.to_csv(OUTPUT_FILE, index=False)

    print("Dish data processing complete.\n")

if __name__ == "__main__":
    process_dish_data()
