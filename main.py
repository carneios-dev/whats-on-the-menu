# import os
import pandas as pd

from Dish.dish_processing import process_dish_data
from Menu.menu_processing import process_menu_data
from MenuItem.menuitem_processing import process_menuitem_data
from MenuPage.menupage_processing import process_menupage_data
from use_case import run_use_case

def check_referential_integrity():
    menuitem_df = pd.read_csv('output/cleaned_MenuItem.csv')
    dish_df = pd.read_csv('output/cleaned_Dish.csv')
    menupage_df = pd.read_csv('output/cleaned_MenuPage.csv')
    menu_df = pd.read_csv('output/cleaned_Menu.csv')

    # Convert IDs to numeric to handle any potential non-numeric values gracefully.
    menuitem_df['dish_id'] = pd.to_numeric(menuitem_df['dish_id'], errors='coerce')
    menuitem_df['menu_page_id'] = pd.to_numeric(menuitem_df['menu_page_id'], errors='coerce')
    dish_df['id'] = pd.to_numeric(dish_df['id'], errors='coerce')
    menupage_df['menu_id'] = pd.to_numeric(menupage_df['menu_id'], errors='coerce')
    menupage_df['id'] = pd.to_numeric(menupage_df['id'], errors='coerce')
    menu_df['id'] = pd.to_numeric(menu_df['id'], errors='coerce')

    # Check foreign key constraints for `MenuItem` -> `Dish`.
    invalid_menuitem_dish = menuitem_df[~menuitem_df['dish_id'].isin(dish_df['id'])]
    if not invalid_menuitem_dish.empty:
        print("Removing invalid dish_id in MenuItem:")
        print(invalid_menuitem_dish)
        menuitem_df = menuitem_df[menuitem_df['dish_id'].isin(dish_df['id'])]
    print("") # Add a newline for better readability.

    # Check foreign key constraints for `MenuItem` -> `MenuPage`.
    invalid_menuitem_menupage = menuitem_df[~menuitem_df['menu_page_id'].isin(menupage_df['id'])]
    if not invalid_menuitem_menupage.empty:
        print("Removing invalid page_id in MenuItem:")
        print(invalid_menuitem_menupage)
        menuitem_df = menuitem_df[menuitem_df['menu_page_id'].isin(menupage_df['id'])]
    print("") # Add a newline for better readability.

    # Check foreign key constraints for `MenuPage` -> `Menu`.
    invalid_menupage_menu = menupage_df[~menupage_df['menu_id'].isin(menu_df['id'])]
    if not invalid_menupage_menu.empty:
        print("Removing invalid menu_id in MenuPage:")
        print(invalid_menupage_menu)
        menupage_df = menupage_df[menupage_df['menu_id'].isin(menu_df['id'])]
    print("") # Add a newline for better readability.

    print("Saving cleaned dataframes back to their respective files...\n")

    # Save the cleaned dataframes back to their respective files.
    menuitem_df.to_csv('MenuItem/cleaned_MenuItem.csv', index=False)
    menupage_df.to_csv('MenuPage/cleaned_MenuPage.csv', index=False)

# def create_test_files():
#     DATASETS = {
#         'output/cleaned_Dish.csv': 'output/test_Dish.csv',
#         'output/cleaned_Menu.csv': 'output/test_Menu.csv',
#         'output/cleaned_MenuItem.csv': 'output/test_MenuItem.csv',
#         'output/cleaned_MenuPage.csv': 'output/test_MenuPage.csv'
#     }
    
#     for input_file, output_file in DATASETS.items():
#         df = pd.read_csv(input_file)

#         if os.path.exists(output_file):
#             print(f"Removing existing file: {output_file}")
#             os.remove(output_file)

#         sample_df = df.head(1000)
#         sample_df.to_csv(output_file, index=False)
#         print(f"Test file created: {output_file}")
    
#     print("") # Add a newline for better readability.

def main():
    print("Starting data processing...\n")
    process_dish_data()
    process_menu_data()
    process_menuitem_data()
    process_menupage_data()
    print("Data processing complete.\n")
    
    print("Starting referential integrity checks...\n")
    check_referential_integrity()
    print("Referential integrity checks complete.")

    # print("Creating test files...\n")
    # create_test_files()
    # print("Test files created.\n")

if __name__ == "__main__":
    main()
    run_use_case()
