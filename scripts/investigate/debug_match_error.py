#!/usr/bin/env python3
"""
debug_match_error.py — Reproduce SQL ที่ vrh_chv_match_v2 จะรันสำหรับ devtest

Usage:
    source /home/khaw/ClaudeCode/vrh_cdmq_dev/venv/bin/activate
    python3 scripts/investigate/debug_match_error.py
"""
import os, sys
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from databricks.connect import DatabricksSession
from pyspark.sql.functions import lower, col
import re

spark = DatabricksSession.builder.getOrCreate()

catalog     = 'viriyah_cdqm_poc'
fw_schema   = 'control_fw'
table       = 'viriyah_cdqm_poc.silver.source_motor_devtest'
data_dt     = '2025-01-01'
ld_id       = '1'
updt_prcs_nm = 'EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2025-01-01'
updt_ld_id   = '1'
env = 'dev'

def custom_aggregation(group):
    if any(group['ORDER'].astype(int) > 1):
        key = [f"COALESCE({i},'')" for i in group['PK']]
        return ', '.join(key)
    else:
        return f"COALESCE({group['PK'].iloc[0]},'')"

config_pk_pd = spark.sql(f"SELECT * FROM {catalog}.{fw_schema}.CHV_CONFIG_PK_V2 ORDER BY `ORDER`").toPandas()
config_pk_agg = config_pk_pd.groupby('TABLE', group_keys=False).apply(custom_aggregation).reset_index(name='PK_CONCATENATED')

config_matching_pd = spark.sql(f"SELECT * FROM {catalog}.{fw_schema}.chv_config_matching_v2 WHERE act_f = '1' ORDER BY TIER, MATCHING_RULES").toPandas()
config_check_pre_val_pd = spark.sql(f"SELECT * FROM {catalog}.{fw_schema}.chv_config_check_pre_validation_v2").toPandas()
general_param = spark.sql(f"SELECT * FROM {catalog}.{fw_schema}.CHV_PARAM_GENERAL_V2")

config = config_matching_pd.merge(config_pk_agg, left_on='MAIN_TABLE', right_on='TABLE', how='left')
config = config.merge(config_pk_agg, left_on='MATCHING_TABLE', right_on='TABLE', how='left')
config = config[(config["ACT_F"] == 1) & (config["MAIN_TABLE"].str.lower() == table.lower())]

print(f"Config rows: {len(config)}")

all_parameter = []
for idx, i in config.iterrows():
    parameter = re.findall(r'MATCH\.([a-zA-Z0-9_]+)', i.MATCH_CONDITION)
    all_parameter += parameter
all_parameter = list(set(all_parameter))
print(f"all_parameter: {all_parameter}")

matched_keys = set()
tier_values = sorted(config['TIER'].unique())

for tier_idx, tier in enumerate(tier_values):
    tier_config = config[config['TIER'] == tier].copy()
    print(f"\n=== Tier {tier} ({len(tier_config)} rules) ===")

    inner_sql = []
    join_sql = []
    from_sql_parts = []

    for idx, i in tier_config.iterrows():
        sub_sql = f"SELECT '{i.MATCHING_RULES}' AS RULES,"
        parameter = re.findall(r'MATCH\.([a-zA-Z0-9_]+)', i.MATCH_CONDITION)
        main_parameter = re.findall(r'MAIN\.([a-zA-Z0-9_]+)', i.MATCH_CONDITION)

        for param in all_parameter:
            sub_sql += f"{param} AS {param}," if param in parameter else f"NULL AS {param},"

        sub_main_sql = f"SELECT CAST('{table.lower()}' AS STRING) AS MAIN_TABLE,CAST('{i.MATCHING_TABLE.lower()}' AS STRING) AS MATCHING_TABLE,CONCAT_WS(',',{config.iloc[0]['PK_CONCATENATED_x']}) AS KEY,"
        for param in all_parameter:
            sub_main_sql += f"{param} AS {param}," if param in main_parameter else f"NULL AS {param},"

        sub_main_sql += f"'{i.SUBJECT}' AS SUBJECT,'{i.MATCHING_RULES}' AS RULES\nFROM {table} SRC\n"
        sub_main_sql += f"LEFT JOIN {catalog}.{fw_schema}.CHV_PARAM_GENERAL_V2 PARM ON LOWER(PARM.PARAM_NAME) = '{table.lower()}' AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'"
        sub_main_sql += f"INNER JOIN {catalog}.{fw_schema}.CHV_PRE_VALIDATION_RESULT_V2 VLD_RESULT\nON VLD_RESULT.DATA_DT = '{data_dt}' AND LOWER(VLD_RESULT.TABLE) = '{i.MAIN_TABLE.lower()}' AND CONCAT_WS(',',{config.iloc[0]['PK_CONCATENATED_x']}) = VLD_RESULT.KEY AND VLD_RESULT.RESULT = 'PASSED'\n"

        match_key = i.PK_CONCATENATED_y
        main_key = i.PK_CONCATENATED_x
        sub_sql += f"CONCAT_WS(',',{match_key}) AS KEY,'{i.MATCHING_TABLE.lower()}' AS TABLE\nFROM {i.MATCHING_TABLE} SRC\n"
        sub_sql += f"LEFT JOIN {catalog}.{fw_schema}.CHV_PARAM_GENERAL_V2 PARM ON LOWER(PARM.PARAM_NAME) = '{i.MATCHING_TABLE.lower()}' AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'"
        sub_sql += f"INNER JOIN {catalog}.{fw_schema}.CHV_PRE_VALIDATION_RESULT_V2 VLD_RESULT\nON VLD_RESULT.DATA_DT = '{data_dt}' AND LOWER(VLD_RESULT.TABLE) = '{i.MATCHING_TABLE.lower()}' AND CONCAT_WS(',',{match_key}) = VLD_RESULT.KEY AND VLD_RESULT.RESULT = 'PASSED'"

        _cpv = config_check_pre_val_pd[config_check_pre_val_pd['MATCHING_RULES'] == i.MATCHING_RULES]
        sub_vld_sql, sub_main_vld_sql = [], []
        for j in range(len(_cpv)):
            if _cpv.iloc[j]['TABLE'] == 'MATCH':
                sub_vld_sql.append(f"(VLD_RESULT.RULES = '{_cpv.iloc[j]['RULES_CHECK']}' AND COLUMN = '{_cpv.iloc[j]['COLUMN']}' AND RESULT = 'PASSED')")
            else:
                sub_main_vld_sql.append(f"(VLD_RESULT.RULES = '{_cpv.iloc[j]['RULES_CHECK']}' AND COLUMN = '{_cpv.iloc[j]['COLUMN']}' AND RESULT = 'PASSED')")

        gp_match = general_param.filter(lower(col('PARAM_NAME')) == i.MATCHING_TABLE.lower()).collect()
        gp_main  = general_param.filter(lower(col('PARAM_NAME')) == table.lower()).collect()

        sub_sql      += 'AND (' + 'or'.join(sub_vld_sql) + ')'
        sub_sql      += f"\nWHERE SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('%DATA_DT%','yyyy-MM-dd'),{gp_match[0].PARAM_VAL_NUMBER}),'yyyy-MM-dd')"
        sub_main_sql += 'AND (' + 'or'.join(sub_main_vld_sql) + ')'
        sub_main_sql += f"\nWHERE SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('%DATA_DT%','yyyy-MM-dd'),{gp_main[0].PARAM_VAL_NUMBER}),'yyyy-MM-dd')"

        if tier_idx > 0 and matched_keys:
            keys_str = "','".join(str(k) for k in matched_keys)
            sub_main_sql += f"\nAND CONCAT_WS(',',{main_key}) NOT IN ('{keys_str}')"

        from_sql_parts.append(sub_main_sql)
        inner_sql.append(sub_sql)

        if i.MAIN_TABLE != i.MATCHING_TABLE:
            sub_sql2 = f"({i.MATCH_CONDITION.replace('LEFT JOIN MATCH ON','').strip()} AND MAIN.RULES = '{i.MATCHING_RULES}' AND MATCH.RULES = '{i.MATCHING_RULES}')".replace('MAIN.%PK_MAIN%', f"CONCAT_WS(',',{config.iloc[0]['PK_CONCATENATED_x']})").replace('%PK_MATCH%', 'KEY')
        else:
            sub_sql2 = f"({i.MATCH_CONDITION.replace('LEFT JOIN MATCH ON','').strip()} AND MAIN.RULES = '{i.MATCHING_RULES}' AND MATCH.RULES = '{i.MATCHING_RULES}' AND MAIN.KEY <> MATCH.KEY)".replace('MAIN.%PK_MAIN%', f"CONCAT_WS(',',{config.iloc[0]['PK_CONCATENATED_x']})").replace('%PK_MATCH%', 'KEY')
        join_sql.append(sub_sql2)

    tier_sql  = f"SELECT MAIN.MAIN_TABLE,MATCHING_TABLE AS MATCHING_TABLE,MAIN.RULES AS MATCHING_RULES,MAIN.KEY AS KEY_MAIN,MATCH.KEY AS KEY_MATCH,CASE WHEN MATCH.RULES IS NULL THEN 'FAILED' ELSE 'PASSED' END AS RESULT,'{ld_id}' AS LD_ID,'{updt_prcs_nm}' AS UPDT_PRCS_NM,'{updt_ld_id}' AS UPDT_LD_ID,MAIN.SUBJECT AS SUBJECT\n"
    tier_sql += f"FROM ({chr(10).join(f for f in from_sql_parts)}) AS MAIN\n"
    tier_sql += f"LEFT JOIN (" + '\n UNION ALL \n'.join(inner_sql) + ') MATCH\n'
    tier_sql += f"ON {'OR'.join(join_sql)}"
    tier_sql  = tier_sql.replace('%DATA_DT%', data_dt).replace('%ENV%', env)

    print(f"\n--- Tier {tier} SQL (first 2000 chars) ---")
    print(tier_sql[:2000])

    try:
        tier_df = spark.sql(tier_sql)
        cnt = tier_df.count()
        print(f"Tier {tier} result: {cnt} rows")
        tier_passed = tier_df.filter("RESULT = 'PASSED'")
        matched_this_tier = set(row['KEY_MAIN'] for row in tier_passed.select('KEY_MAIN').distinct().collect())
        print(f"Tier {tier}: {len(matched_this_tier)} keys matched")
        matched_keys |= matched_this_tier
    except Exception as e:
        print(f"ERROR in tier {tier}: {e}")
        break

print("\n=== DONE ===")
