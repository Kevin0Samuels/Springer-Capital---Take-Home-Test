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

data = [
    ["referral_details_id", "INTEGER", "Unique tracking ID for the specific referral log entry"],
    ["referral_id", "STRING", "Unique system-generated identifier for the referral event"],
    ["referral_source", "STRING", "Channel through which the referral originated (User Sign Up, Draft Transaction, Lead)"],
    ["referral_source_category", "STRING", "High-level grouping of the source channel (Online, Offline, or lead category)"],
    ["referral_at", "DATETIME", "Exact timestamp when the referral was initiated"],
    ["referrer_id", "STRING", "Unique ID of the existing member who made the referral"],
    ["referrer_name", "STRING", "Full name of the referrer (Initcap applied)"],
    ["referrer_phone_number", "STRING", "Contact phone number of the referrer"],
    ["referrer_homeclub", "STRING", "Designated home fitness club location of the referrer"],
    ["referee_id", "STRING", "Unique ID assigned to the newly referred user"],
    ["referee_name", "STRING", "Full name of the newly referred user (Initcap applied)"],
    ["referee_phone", "STRING", "Contact phone number of the new referee"],
    ["referral_status", "STRING", "Current workflow state of the referral (Berhasil, Menunggu, Tidak Berhasil)"],
    ["num_reward_days", "INT", "Standard duration window (in days) applied to reward validity tracking"],
    ["transaction_id", "STRING", "Unique ID of the financial transaction linked to the referral"],
    ["transaction_status", "STRING", "Current payment status of the transaction (Paid)"],
    ["transaction_at", "DATETIME", "Timestamp when the transaction was completed"],
    ["transaction_location", "STRING", "Physical or digital branch location where the transaction occurred"],
    ["transaction_type", "STRING", "Classification of the transaction type (New)"],
    ["updated_at", "DATETIME", "Timestamp of the last update made to the referral record"],
    ["reward_granted_at", "DATETIME", "Timestamp when the incentive reward was officially credited to the user"],
    ["is_business_logic_valid", "BOOLEAN", "Fraud check flag indicating whether the referral reward complies with business rules"]
]

df_dict = pd.DataFrame(data, columns=["Column Name", "Data Type", "Description / Business Meaning"])
df_dict.to_excel("data_dictionary.xlsx", index=False)
print("Saved data_dictionary.xlsx")
