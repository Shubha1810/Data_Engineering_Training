import pandas as pd
import numpy as np

# Load CSV file
df = pd.read_csv('week2_expenses.csv')

# Clean 'amount' column (remove $ and convert to float)
df['amount'] = df['amount'].replace('[\$,]', '', regex=True).astype(float)

# Convert 'date' column to datetime
df['date'] = pd.to_datetime(df['date'])

# Extract month in YYYY-MM format
df['month'] = df['date'].dt.to_period('M')

# Monthly total & average expense
monthly_totals = df.groupby('month')['amount'].sum()
monthly_averages = df.groupby('month')['amount'].mean()

# Category-wise monthly breakdown
monthly_expense = df.groupby(['month', 'category'])['amount'].sum().unstack().fillna(0)

# Display results
print("==== Monthly Totals ====")
print(monthly_totals)
print("\n==== Monthly Averages ====")
print(monthly_averages)
print("\n==== Monthly Expense Breakdown ====")
print(monthly_expense)

# Save cleaned dataset & breakdown
df.to_csv('week2_cleaned_expenses.csv', index=False)
monthly_expense.to_csv('week2_monthly_expense_breakdown.csv')
