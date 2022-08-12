import numpy as np
import pandas as pd
from datetime import date, timedelta
import tgfr

output_path = "C:\\Users\\peaju\\THAI AIRWAYS INTERNATIONAL PUBLIC CO.,LTD\\DP - PC - Team 1 - Documents\\PC - Team 1\\2022\\python\\dash_board\\output\\"
data_path = "C:\\Users\\peaju\\THAI AIRWAYS INTERNATIONAL PUBLIC CO.,LTD\\DP - PC - Team 1 - Documents\\PC - Team 1\\2022\\python\\dash_board\\data\\"
today = date.today()


def load_denorm(days = 90):
    """Load denorm data from TGFR server from previous specified days"""
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
    return denorm_df

def load_eofp(n):
    """Load eofp from most recent up to n loop. Each loop will download 500 data"""
    eofp_df = pd.DataFrame()
    for i in np.arange(n):
        limit = 500
        skip = 500 * i
        chunk = tgfr.get_oefp_user_input(skip=skip, limit=limit)
        eofp_df = pd.concat([eofp_df, chunk], ignore_index= True)
    return eofp_df

def load_merged_df():
    return pd.read_json(data_path + 'merged_data.json')

def plan_zfw(eofp_df_merged):
    """extract planned zfw value from merged data frame

    Return
    ------
    df[['zfw_plan','zfw_est_eofp']]
    """
    # Extract two data frame from merged df
    fuel_report = tgfr.expand_columns(eofp_df_merged,'fuelreport')
    eofp = tgfr.expand_columns(eofp_df_merged,'userInput')

    df = eofp_df_merged[['planned_zfw']]
    df['zfw_plan_fuel_report'] = tgfr.expand_columns(fuel_report,'plan_zfw').astype(float)
    df['zfw_est_eofp'] = tgfr.expand_columns(eofp,'est_zfw').astype(float)

    # Use plan ZFW data from fuel report 
    zfw_plan_list = []
    for i, row in df[['zfw_plan_fuel_report', 'planned_zfw']].iterrows():
        fuel_report_zfw = row['zfw_plan_fuel_report']
        planned_zfw = row['planned_zfw']
        if pd.isna(fuel_report_zfw) and pd.isna(planned_zfw):
            zfw_plan_list.append(np.nan)
        elif pd.isna(fuel_report_zfw):
            zfw_plan_list.append(planned_zfw)
        else:
            zfw_plan_list.append(fuel_report_zfw)

    df['zfw_plan'] = zfw_plan_list
    return df[['zfw_plan', 'zfw_est_eofp']]

def actual_zfw(eofp_df_merged):
    """extract actual zfw value from merged data frame

    Return
    ------
    df[['zfw_plan','zfw_est_eofp']]
    """
    # Extract three data frames from merged df
    fuel_report = tgfr.expand_columns(eofp_df_merged,'fuelreport')
    eofp = tgfr.expand_columns(eofp_df_merged,'userInput')
    qar = tgfr.expand_columns(eofp_df_merged,'qar')

    # Create dataframe to compare value
    zfw_actual_df = pd.DataFrame(
    {
        'fuel_report' : fuel_report['actual_zfw'].astype(float),
        'eofp' : eofp['actual_zfw'],
        'qar' : qar['zero_fuel_weight']
    } ,index = eofp_df_merged.index
    )

    # Replace 0 value to nan the calculate Mean value
    zfw_actual_df.replace(0, np.nan, inplace=True)
    zfw_actual_df['mean_value'] =zfw_actual_df.mean(axis=1)

    zfw_actual_list = []
    for i, row in zfw_actual_df.iterrows():
        values_list = [row['fuel_report'], row['eofp'], row['qar']]
        mean_value = row['mean_value']
        zfw_actual_list.append(tgfr.compare_and_choose(values_list, mean_value))
    return pd.DataFrame({'zfw_actual':zfw_actual_list},index=eofp_df_merged.index)

def export_csv(df, name):
    df.to_csv(output_path + name +'.csv')

def export_json(df, name):
    df.to_json(output_path + name + '.json')

