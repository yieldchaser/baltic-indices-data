import pandas as pd

# Load unmasked forward curves
fwd_df = pd.read_csv('data/bunkers/bunker_forward_curves_12m.csv')
print("Unmasked hubs in forward curve:", fwd_df['port'].unique())

# Load spot prices from master
master_df = pd.read_csv('bunker_master_historical.csv')
latest_spots = master_df.sort_values('observation_date').groupby(['port_code', 'grade']).last().reset_index()

for p in ['SG SIN', 'NL RTM', 'US HOU', 'US NYC', 'GI GIB', 'CN ZOS', 'PA BLB']:
    sub = latest_spots[latest_spots['port_code'] == p]
    for _, row in sub.iterrows():
        print(f"{p} ({row['port_name']}) - {row['grade']}: ${float(row['price_usd']):.2f}")
