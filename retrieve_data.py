import tgfr
import tgfr_prelim_data

# Load denorm data
denorm_df = tgfr_prelim_data.load_denorm()

# initialize number or eOFP report to be downloaded
total_data = len(denorm_df.fuelreport)
total_loop = total_data // 500 + 1

# load eofp
eofp_df = tgfr_prelim_data.load_eofp(total_loop)

# merged eofp to denorm df
eofp_df_merged = tgfr.merge_flightPlan_eofp(eofp_df,denorm_df)

# export merged data to json file
tgfr_prelim_data.export_json(eofp_df_merged, name='merged_data')