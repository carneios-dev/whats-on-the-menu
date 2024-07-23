import os

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def convert_to_usd(price, currency):
    if currency == "CENTS":
        return np.ceil(price / 100)
    return price

# Identify the top 10 most frequently appearing dishes using the times_appeared column.
def popular_dishes(dish_df):
    return dish_df.nlargest(10, 'times_appeared')

# Identify the price changes over time for the top 10 most popular dishes, by decade.
def price_changes_over_time(dish_df, menu_df, menuitem_df, menupage_df, popular_dishes_df):
    # Merge dataframes to get complete information
    merged_df = menuitem_df.merge(dish_df, left_on='dish_id', right_on='id', suffixes=('', '_dish'))
    merged_df = merged_df.merge(menupage_df, left_on='menu_page_id', right_on='id', suffixes=('', '_page'))
    merged_df = merged_df.merge(menu_df, left_on='menu_id', right_on='id', suffixes=('', '_menu'))

    # Filter for popular dishes in USD.
    filtered_df = merged_df[(merged_df['dish_id'].isin(popular_dishes_df['id'])) & 
                            (merged_df['currency'].isin(['DOLLARS', 'CENTS']))].copy()

    # Convert prices to USD.
    filtered_df['price'] = filtered_df.apply(lambda row: convert_to_usd(row['price'], row['currency']), axis=1)
    filtered_df['high_price'] = filtered_df.apply(lambda row: convert_to_usd(row['high_price'], row['currency']), axis=1)

    filtered_df['date'] = pd.to_datetime(filtered_df['date'], errors='coerce', format='%Y-%m-%dT%H:%M:%SZ')

    # Extract the decade from the date.
    filtered_df['decade'] = (filtered_df['date'].dt.year // 10) * 10

    # Track price changes over the decades.
    filtered_df = filtered_df[['dish_id', 'name', 'price', 'high_price', 'decade', 'location', 'sponsor']]

    price_summary = filtered_df.groupby(['dish_id', 'name', 'decade']).agg(
        avg_price=('price', 'mean'),
        min_price=('price', 'min'),
        max_price=('price', 'max')
    ).reset_index()

    return price_summary.sort_values(by=['dish_id', 'decade'])

def format_table(df, title):
    # Get the column widths
    widths = [max(len(str(value)) for value in df[col].values) for col in df.columns]
    widths = [max(width, len(col)) for width, col in zip(widths, df.columns)]
    
    # Calculate the total width of the table
    total_width = sum(widths) + len(widths) * 3 - 1
    
    # Create the header row
    header = ' | '.join(f"{col:{width}}" for col, width in zip(df.columns, widths))
    separator = '-+-'.join('-' * width for width in widths)
    
    # Create the data rows
    rows = [header, separator]
    for row in df.itertuples(index=False):
        rows.append(' | '.join(f"{str(value):{width}}" for value, width in zip(row, widths)))
    
    # Create the formatted title
    title_line = '=' * total_width
    centered_title = f"| {title.center(total_width - 4)} |"
    title_section = f"{title_line}\n{centered_title}\n{title_line}"
    
    table = '\n'.join(rows)

    footer = '=' * total_width
    
    return f"{title_section}\n{table}\n{footer}"

def plot_price_trends(price_summary_df):
    plt.figure(figsize=(14, 8))

    for dish_id, group in price_summary_df.groupby('dish_id'):
        plt.plot(group['decade'], group['avg_price'], marker='o', label=group['name'].iloc[0])

    plt.xlabel('Decade')
    plt.ylabel('Average Price (USD)')
    plt.title('Price Trends of Popular Dishes Over Decades')
    plt.legend()
    plt.grid(True)

    # Save the visualization to a file, or replace it if it already exists.
    OUT_IMAGE = 'output/popular_dish_price_trends_by_decade.png'
    if os.path.exists(OUT_IMAGE):
        print(f"Removing existing visualization: {OUT_IMAGE}")
        os.remove(OUT_IMAGE)
    plt.savefig(OUT_IMAGE)
    
    plt.show() # Show the visualization.

def run_use_case():
    # Load the datasets.
    dish_df = pd.read_csv('output/cleaned_Dish.csv')
    menu_df = pd.read_csv('output/cleaned_Menu.csv')
    menupage_df = pd.read_csv('output/cleaned_MenuPage.csv')
    menuitem_df = pd.read_csv('output/cleaned_MenuItem.csv')

    popular_dishes_df = popular_dishes(dish_df)
    print(format_table(popular_dishes_df, "Popular Dishes"))

    # Track price changes over time
    price_changes_df = price_changes_over_time(dish_df, menu_df, menuitem_df, menupage_df, popular_dishes_df)
    print("\nCreating and displaying price trends over time visualization...")
    plot_price_trends(price_changes_df)
