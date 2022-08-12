# import libraries
import pandas as pd
import numpy as np
import tgfr

# Adjust to/from date
from_param = input("Enter start date (YYYY-MM-DDTHH:MM:SS.SSS): ")
to_param = input("Enter end date (YYYY-MM-DDTHH:MM:SS.SSS): ")

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

# Create special_df
special_columns = tgfr.expend_columns(eofp_df_merged, 'special_planning')

# initiate empty dict
special_plan_dict = {
    'decision_point_name':[],
    'enroute_alternate_aerodrome':[],
    'fuel_to_dp':[],
    'min_fuel_dp':[],
    'plan_type':[]
}
for i, row in special_columns.iterrows():
    try:
        special_plan_dict['decision_point_name'].append(row['decision_point']['name'])
    except TypeError:
        special_plan_dict['decision_point_name'].append(np.nan)
    try:
        special_plan_dict['enroute_alternate_aerodrome'].append(row['enroute_alternate']['icao_id'])
    except TypeError:
        special_plan_dict['enroute_alternate_aerodrome'].append(np.nan)
    try:
        special_plan_dict['fuel_to_dp'].append(row['to_dp']['fuel']['value'])
    except TypeError:
        special_plan_dict['fuel_to_dp'].append(np.nan)
    try:
        special_plan_dict['min_fuel_dp'].append(row['min_fuel_to_proceed'])
    except TypeError:
        special_plan_dict['min_fuel_dp'].append(np.nan)
    try:
        special_plan_dict['plan_type'].append(row['_type'])
    except TypeError:
        special_plan_dict['plan_type'].append(np.nan)

special_plan_df = pd.DataFrame(special_plan_dict, index=eofp_df_merged.index)
special_plan_df['plan_type'].replace({np.nan:'Normal','FEW':'CF3', 'PBR':'RCFP'},inplace= True)

# Create report_df to from eofp_merge_df to rcfp_df
report_df = eofp_df_merged.copy()
#report_df = report_df.join(special_plan_df[['plan_type']])

eofp_df_merged.to_json('.\\output\\merged_data.json')

# create header_columns and select only rcfp
report_df = report_df[[
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
report_df = report_df.join(special_plan_df)

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
report_df['pay_load_actual'] = report_df.zfw_actual - report_df.dow

# plan flight time
report_df['flight_time_plan'] = eofp_df_merged.fuelreport.apply(pd.Series)['plan_flt_time']

flight_time_list = []
for item in report_df['flight_time_plan']:
    if pd.isna(item):
        flight_time_list.append(np.nan)
    else:
        x = item.split(':')
        flight_time_list.append(int(x[0]) + int(x[1])/60)
        
report_df['flight_time_plan_hours'] = flight_time_list

# actual flight time
# actual flight time from fuel report
actual_flight_time_fuel_report = eofp_df_merged.fuelreport.apply(pd.Series)['actual_flt_time']
act_flt_time_fuel_rpt_list = []
for item in actual_flight_time_fuel_report:
    if pd.isna(item):
        act_flt_time_fuel_rpt_list.append(np.nan)
    else:
        try:
            x = item.split(':')
            act_flt_time_fuel_rpt_list.append(int(x[0]) + int(x[1])/60)
        except ValueError:
            act_flt_time_fuel_rpt_list.append(np.nan)

# actual flight time from eofp
actual_flight_time_eofp = eofp_df_merged.userInput.apply(pd.Series)['flight_time']
act_flt_time_eofp_list = []
for duration in actual_flight_time_eofp:
    if pd.isna(duration):
        act_flt_time_eofp_list.append(np.nan)
    else:
        hr = int(duration[0:2])
        min = int(duration[2:4])
        act_flt_time_eofp_list.append(hr + min/60)

# actual flight time from qar
actual_flight_time_qar = tgfr.expend_columns(eofp_df_merged,'qar')['actual_flight_time']/3600

act_flt_time_df = pd.DataFrame({
    'fuel_report': pd.Series(act_flt_time_fuel_rpt_list, index=eofp_df_merged.index),
    'eoft':pd.Series(act_flt_time_eofp_list,index=eofp_df_merged.index),
    'qar':actual_flight_time_qar
    }, index=eofp_df_merged.index)

report_df['flight_time_actual'] = act_flt_time_df.median(axis=1)

# fill null and 0 value with plan flight time
report_df['flight_time_actual'] = report_df['flight_time_actual'].fillna(report_df['flight_time_plan_hours'])
report_df.loc[report_df.flight_time_actual == 0, 'flight_time_actual']  = report_df['flight_time_plan_hours']

report_df = report_df[[
       # General
       'flight_date', 'flight_number', 'flight_plan_id',
       # Aircraft
       'aircraft_registration','aircraft_type',
       # Aerodrome
       'arrival_aerodrome', 'departure_aerodrome', 'alternate_aerodrome', 
       # ZFW
       'zfw_plan', 'zfw_actual','zfw_diff', 'pay_load_actual',
       # Flight time
       'flight_time_plan_hours','flight_time_actual',
       # Special Ops
       'decision_point_name', 'enroute_alternate_aerodrome', 'fuel_to_dp','min_fuel_dp', 'plan_type']]

report_df['flight_time_delta'] = report_df.flight_time_actual - report_df.flight_time_plan_hours

# fuel planning
ramp_fuel_plan_fuel_report = eofp_df_merged['fuelreport'].apply(pd.Series)[[
    'plan_block_fuel',
    'plan_burn',
    'plan_taxi_fuel',
    'company_fuel']].astype(float)
ramp_fuel_plan_fuel_report['trip_fuel_plan'] = ramp_fuel_plan_fuel_report.plan_burn - ramp_fuel_plan_fuel_report.plan_taxi_fuel

plan_fuel_df = eofp_df_merged.planned_fuel.apply(pd.Series)[[
    'contingency_fuel',
    'alternate_fuel',
    'final_reserve_fuel',
    'additional_fuel'
]]

fuel_plan_df = ramp_fuel_plan_fuel_report.join(plan_fuel_df)
fuel_plan_df['cf3'] = fuel_plan_df.trip_fuel_plan *0.03

report_df = report_df.join(fuel_plan_df)
report_df.rename(columns= {'plan_block_fuel': 'ramp_fuel_plan', 'plan_burn': 'fuel_burn_plan', 'plan_taxi_fuel': 'taxi_fuel_plan'}, inplace=True)

# Acutal Ramp Fuel
ramp_fuel_actual_df = pd.DataFrame(
    {
        'fuel_report' : eofp_df_merged.fuelreport.apply(pd.Series)['actual_block_fuel'].astype(float),
        'eofp' : eofp_df_merged.userInput.apply(pd.Series)['offblock_fuel'],
        'qar' : eofp_df_merged.qar.apply(pd.Series)['ramp_fuel']
    },
    index=eofp_df_merged.index
)

#ramp_fuel_actual_df['median'] = ramp_fuel_actual_df.median(axis=1)
ramp_fuel_actual_df.replace(0.0, np.nan, inplace = True)
ramp_fuel_actual_df = ramp_fuel_actual_df.applymap(tgfr.nonnumeric_to_nan)

ramp_fuel_actual_list = []
for i, row in ramp_fuel_actual_df.iterrows(): 
    values_list = [row['fuel_report'], row['eofp'], row['qar']]
    mean_value = report_df.loc[i,'ramp_fuel_plan']
    if np.count_nonzero(~np.isnan(values_list)) == 1:
        ramp_fuel_actual_list.append(np.nansum(values_list))
    else:
        ramp_fuel_actual_list.append(tgfr.compare_and_choose(values_list, mean_value))

report_df['ramp_fuel_actual'] = ramp_fuel_actual_list

# Actual Burn
fuel_burn_actual_df = pd.DataFrame(
    {
        'fuel_report' : tgfr.columns_extract(eofp_df_merged,'fuelreport')['actual_burn'],
        'qar' : tgfr.columns_extract(eofp_df_merged,'qar')['overall_fuel_used'],
        'eofp' : tgfr.columns_extract(eofp_df_merged,'userInput')['actual_burn_fuel']
    }, index = eofp_df_merged.index
)
fuel_burn_actual_df.replace(0.0, np.nan, inplace = True)
fuel_burn_actual_df = fuel_burn_actual_df.applymap(tgfr.nonnumeric_to_nan)
report_df['fuel_burn_actual'] = fuel_burn_actual_df.median(axis=1)

correction_factor_df = pd.DataFrame(
    {
        'aircraft_type' : ['B777-300ER', 'A350-900','B787-8','B777-200ER','B787-9'],
        'correction_factor' : [35, 30, 25, 35, 25],
        'max_cf' : [3400, 2500, 2100, 2500, 2300],
        'min_cf' : [1300, 800, 700, 1000, 800]
    }
)


report_df = report_df.merge(correction_factor_df, on = 'aircraft_type', how='left')
report_df.set_index(eofp_df_merged.index,inplace=True)

# extra fuel

extra_fuel_list = []
for i, row in report_df.iterrows():
    min_fuel = tgfr.extra_fuel(row['correction_factor'], row['ramp_fuel_plan'], row['flight_time_plan_hours'], row['zfw_diff'])
    # min_fuel_list.append(min_fuel)
    extra_fuel_list.append(row['ramp_fuel_actual'] - min_fuel)

# report_df['ramp_fuel_corr'] = min_fuel_list, 
report_df['extra_fuel'] = extra_fuel_list
report_df['ramp_fuel_corr'] = report_df.ramp_fuel_actual - report_df.extra_fuel

# add trip_fuel_correction_from extrafuel and zfw
additional_weight = report_df['zfw_diff'] + report_df['extra_fuel']
trip_fuel_plan_corr_extraFuel = report_df['trip_fuel_plan'] + report_df['correction_factor'] * additional_weight /1000 *report_df['flight_time_plan_hours']
report_df['trip_fuel_plan_corr_extra_fuel'] = trip_fuel_plan_corr_extraFuel

# clean cf3 values
cf3_clean = []
for i, dat in report_df.iterrows():
    cf3_clean.append(tgfr.check_cf(dat['cf3'],dat['min_cf'], dat['max_cf']))

report_df['cf3'] = cf3_clean

# fill na value with min cf
report_df['contingency_fuel'] = report_df['contingency_fuel'].fillna(report_df['min_cf'])

# sort value and remove dubplicate
report_df.sort_values(by = ['flight_date', 'flight_number','aircraft_registration', 'flight_plan_id'], ascending=True, inplace=True)
keys = ['flight_date',	'flight_number','aircraft_registration','departure_aerodrome']
report_df.drop_duplicates(subset=keys, keep='last', inplace=True)

report_df.to_json('.\output\general_report.json')

# additional_weight = report_df['zfw_diff'] + report_df['extra_fuel']
# trip_fuel_plan_corr_extraFuel = report_df['trip_fuel_plan'] + report_df['correction_factor'] * additional_weight /1000 *report_df['flight_time_plan_hours']
# report_df['trip_fuel_plan_corr_extra_fuel'] = trip_fuel_plan_corr_extraFuel