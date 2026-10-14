import os
import pandas as pd

def profile_tables():
    data_dir = "data"
    tables = [
        "lead_logs.csv", "paid_transactions.csv", "referral_rewards.csv",
        "user_logs.csv", "user_referral_logs.csv", "user_referral_statuses.csv", "user_referrals.csv"
    ]
    
    os.makedirs("output/profiling", exist_ok=True)
    
    for table in tables:
        path = os.path.join(data_dir, table)
        if os.path.exists(path):
            df = pd.read_csv(path)
            profile = pd.DataFrame({
                "Column Name": df.columns,
                "Data Type": df.dtypes.astype(str),
                "Null Count": df.isnull().sum(),
                "Percentage Populated": ((df.notnull().sum() / len(df)) * 100).round(2).astype(str) + '%',
                "Distinct Value Count": df.nunique()
            })
            output_file = f"output/profiling/{table.replace('.csv', '_profile.csv')}"
            profile.to_csv(output_file, index=False)
            print(f"Profiled {table} -> Saved to {output_file}")

if __name__ == "__main__":
    profile_tables()
