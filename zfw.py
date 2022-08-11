import pandas as pd
import numpy as np
import tgfr
from scipy.stats import norm, t, sem
import tgfrStat

#output_path = "C:\\Users\\peaju\\THAI AIRWAYS INTERNATIONAL PUBLIC CO.,LTD\\DP - PC - Team 1 - Documents\\PC - Team 1\\2022\\python\\dash_board\\output\\"

# Load data
#eofp_df_merged = pd.read_json(output_path+'merged_data.json')
eofp_df_merged = tgfrStat.load_merged_df()
report_df = eofp_df_merged[[
    'flight_date',
    'flight_number',
    'aircraft_registration',
    'aircraft_type',
    'arrival_aerodrome',
    'departure_aerodrome',
    'alternate_aerodrome',
    'flight_plan_id',
    'planned_zfw',
    'dow'
]]

# Calculate for ZFW values
# Plan ZFW

report_df['zfw_plan_fuel_report'] = eofp_df_merged.fuelreport.apply(pd.Series)['plan_zfw'].astype(float)
report_df['zfw_est_eofp'] = eofp_df_merged.userInput.apply(pd.Series)['est_zfw'].astype(float)

zfw_plan_list = []
for i, row in report_df[['zfw_plan_fuel_report', 'planned_zfw']].iterrows():
    fuel_report_zfw = row['zfw_plan_fuel_report']
    planned_zfw = row['planned_zfw']
    if pd.isna(fuel_report_zfw) and pd.isna(planned_zfw):
        zfw_plan_list.append(np.nan)
    elif pd.isna(fuel_report_zfw):
        zfw_plan_list.append(planned_zfw)
    else:
        zfw_plan_list.append(fuel_report_zfw)

report_df['zfw_plan'] = zfw_plan_list

# Actual ZFW
zfw_actual_df = pd.DataFrame(
    {
        'fuel_report' : eofp_df_merged.fuelreport.apply(pd.Series)['actual_zfw'].astype(float),
        'eofp' : eofp_df_merged.userInput.apply(pd.Series)['actual_zfw'],
        'qar' : eofp_df_merged.qar.apply(pd.Series)['zero_fuel_weight']
    },index = eofp_df_merged.index
)
zfw_actual_df.replace(0, np.nan, inplace=True)
zfw_actual_df['mean_value'] =zfw_actual_df.mean(axis=1)

zfw_actual_list = []
for i, row in zfw_actual_df.iterrows():
    values_list = [row['fuel_report'], row['eofp'], row['qar']]
    mean_value = row['mean_value']
    zfw_actual_list.append(tgfr.compare_and_choose(values_list, mean_value))

report_df['zfw_actual'] = zfw_actual_list
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

tgfrStat.export_csv(report_df, name='rpt_zfw')