import pandas as pd

df = pd.read_csv("warehouse_messy_data.csv")

# Normalize columns: lowercase and replace spaces with underscores
df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

# Convert quantity to numeric (coerce errors to NaN, then fill with 0)
# If you want to manually map words to numbers, you can do so here (optional)
word_to_num = {
    'two hundred': 200,
    # add more if needed
}
df['quantity'] = df['quantity'].replace(word_to_num)
df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)

# Convert price to numeric, fill missing with 0.0
df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0.0)

# Fill missing values
df['category'] = df['category'].fillna('Unknown')
df['supplier'] = df['supplier'].fillna('Unknown')
df['status'] = df['status'].fillna('Unknown')

# Convert last_restocked to datetime
df['last_restocked'] = pd.to_datetime(df['last_restocked'], errors='coerce')

# Remove duplicates
df = df.drop_duplicates()

# Standardize string columns
df['category'] = df['category'].str.lower().str.strip()
df['status'] = df['status'].str.lower().str.strip()
df['warehouse'] = df['warehouse'].str.upper().str.strip()
df['product_name'] = df['product_name'].str.strip()
df['supplier'] = df['supplier'].str.strip()
df['location'] = df['location'].str.strip()

df.to_csv("warehouse_clean_data.csv", index=False)
