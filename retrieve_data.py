# import libraries
from datetime import date, timedelta
import pandas as pd
import numpy as np
import tgfr

today = date.today()
output_path = "C:\\Users\\peaju\\THAI AIRWAYS INTERNATIONAL PUBLIC CO.,LTD\\DP - PC - Team 1 - Documents\\PC - Team 1\\2022\\python\\dash_board\\output\\"

# Adjust to/from date
from_param = str(today - timedelta(days=90))
to_param = str(today)

denorm_df = tgfr.get_denorm_data(from_param, to_param)

# rename columns
columns_name = {
    'arrival_aerodrome_icao_code' : 'arrival_aerodrome',
    'departure_aerodrome_icao_code' : 'departure_aerodrome',
    'alternate_aerodromes_icao_code' : 'alternate_aerodrome',
    'aircraft_aircraft_config_aircraft_version' : 'aircraft_type',
    'aircraft_aircraft_config_cabin_version' : 'aircraft_cabin_version',
    'aircraft_aircraft_config_fleet_name' : 'aircraft_fleet_name'
}
denorm_df.rename(columns=columns_name, inplace=True)

# initialize number or eOFP report to be downloaded
total_data = len(denorm_df.fuelreport)
total_loop = total_data // 500 + 1

# load first chunk of eOFP data
eofp_df = tgfr.get_oefp_user_input()

for i in np.arange(total_loop):
    limit = 500
    skip = 500 + 500 * i
    chunk = tgfr.get_oefp_user_input(skip=skip, limit=limit)
    eofp_df = pd.concat([eofp_df, chunk], ignore_index= True)

# merged eofp to denorm df
eofp_df_merged = tgfr.merge_flightPlan_eofp(eofp_df,denorm_df)

# Create report_df to from eofp_merge_df to rcfp_df
report_df = eofp_df_merged.copy()
#report_df = report_df.join(special_plan_df[['plan_type']])

eofp_df_merged.to_json(output_path+'merged_data.json')