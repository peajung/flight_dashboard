import pandas as pd
import numpy as np
# import tgfr
# from scipy.stats import norm, t, sem
import tgfrStat
import tgfr_prelim_data

# Load data
eofp_df_merged = tgfr_prelim_data.load_merged_df()
report_df = eofp_df_merged[[
    'flight_date',
    'flight_number',
    'departure_aerodrome',
]]
zfw_plan = tgfr_prelim_data.plan_zfw(eofp_df_merged)
zfw_actual = tgfr_prelim_data.actual_zfw(eofp_df_merged)

# Merge all data
report_df = pd.concat([report_df,zfw_plan,zfw_actual],axis=1)

# Calculate for ZFW values
report_df['zfw_diff'] = report_df.zfw_actual - report_df.zfw_plan

# Create dataframe for zfw data only
zfw_df = report_df[['flight_date', 'flight_number','departure_aerodrome', 'zfw_diff']]

# Calculate total flights & drop flight with less than 10 datas
flight_dep = report_df.groupby(['flight_number','departure_aerodrome'])['flight_date'].count()
total_flight = flight_dep.reset_index()

zfw_df = zfw_df.merge(total_flight, on=['departure_aerodrome','flight_number']).rename(columns = {'flight_date_y':'total_flight'})
zfw_df = zfw_df[zfw_df.total_flight >= 10]
zfw_df.dropna(inplace=True)

flight_list = zfw_df[['flight_number','departure_aerodrome']].drop_duplicates()

# Calculate statistical value
report_df = pd.DataFrame(columns=['low','mean', 'high'],index=flight_list)
for ind, i in enumerate(report_df.index):
    flt = i[0]
    dep = i[1]
    data = zfw_df[(zfw_df['flight_number'] == flt) & (zfw_df['departure_aerodrome'] == dep)]['zfw_diff']
    pct5, pct95 = np.percentile(data ,[5,95])
    low, mean, high = tgfrStat.mean_confidence_interval(data)
    if np.absolute(pct5/low - 1) > 0.1:
        low = pct5
    if np.absolute(pct95/high -1) > 0.1:
        high = pct95
    report_df.iloc[ind, 0] = low
    report_df.iloc[ind, 1] = mean
    report_df.iloc[ind, 2] = high

tgfr_prelim_data.export_csv(report_df, name='rpt_zfw')