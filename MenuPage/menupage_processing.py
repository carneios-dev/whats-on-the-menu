import os
import pandas as pd

def process_menupage_data():
    INPUT_FILE = 'input/refined_MenuPage.csv'
    OUTPUT_FILE = 'output/cleaned_MenuPage.csv'

    print("Processing menu page data...")

    # Load the dataset.
    menupage_df = pd.read_csv(INPUT_FILE)

    # Remove duplicate rows.
    menupage_df.drop_duplicates(inplace=True)

    # Save the finalized dataset to a new file, or replace it if it already exists.
    if os.path.exists(OUTPUT_FILE):
        print(f"Removing existing file: {OUTPUT_FILE}")
        os.remove(OUTPUT_FILE)
    menupage_df.to_csv(OUTPUT_FILE, index=False)

    print("Menu page data processing complete.\n")

if __name__ == "__main__":
    process_menupage_data()
