import sys
import traceback
import linecache
import os
from datetime import datetime, timedelta
import logging

import pandas as pd
import sqlalchemy.exc

from App.etl.extract.data_from_mdc import dt_data_extract, prod_data_extract, runtime_data_extract, brandcode_data_extract
from App.etl.transform.mes_etl import mes_etl_main
from App.utils.time_utils import get_analysis_time_bounds


def append_data_to_sql(db_connection, new_rows: pd.DataFrame, table_name: str) -> str:
    """
    Function to append data to existing SQL tables. It looks at the column data types in SQL and revises the data types and then performs the appending.

    :params db_connection:
    :params new_rows:
    :params table_name:
    :returns: message:
    """
    # todo write append data to existing data tables
    query = 'SELECT column_name, data_type ' \
            'FROM information_schema.columns ' \
            'WHERE table_name=?'
    result = db_connection.execute(query, table_name).fetchall()
    columns_in_sql = pd.DataFrame(data=result, columns=['COLUMN_NAME', 'DATA_TYPE'])
    new_table = pd.DataFrame(columns=list(columns_in_sql['COLUMN_NAME']))
    new_rows.columns = new_rows.columns.str.lower()
    new_table.columns = new_table.columns.str.lower()
    for column in new_table.columns:
        if column in new_rows.columns:
            new_table[column] = new_rows[column]
        else:
            new_table[column] = pd.NA

    try:
        result = new_table.to_sql(table_name, db_connection, if_exists='append', index=False)
    except sqlalchemy.exc.DBAPIError as e:
        logging.exception(f'Error while appending to {table_name}: {e}', exc_info=True)
        return True

    return False


def data_type_replace(data_to_be_replaced,data_to_be_used):
    # todo: no idea if this will be necessary in the python. R lines 79-116
    print('pretending to replace datatypes while code is being written')
    return 'temporary list of data to be replaced, data to be used'


def convert_to_utf16(string: str) -> list:
    return list(max(hex, list(string.encode('UTF-16LE'))))


def get_exception() -> str:
    '''
    A function which returns Exception details in the format
    'Failure at {file_name}: {line_number} [{line}] - {exception_message}'
    '''
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    line_no = tb.tb_lineno
    file_name = f.f_code.co_filename
    linecache.checkcache(file_name)
    line = linecache.getline(file_name, line_no, f.f_globals)
    return f'Failure at {file_name}: {line_no} [{line.strip()}] - {exc_obj}'


def site_server_overall_etl(mdc_header: dict, params: dict, line_params: pd.DataFrame, code_params: dict, db_connection) -> bool:
    """
    Runs the logic from RCO_Overall_Orchestrator.R
    Line numbers unless otherwise noted are referring to lines in that R script

    :param mdc_header:
    :param params:
    :param line_params:
    :param db_connection:
    :return:
    """

    query_start_marker = datetime.now()
    start_marker = query_start_marker

    try:

        ###########
        # EXTRACT #
        ###########
        print(params['SiteServer'], params['SiteMDCName'])

        # Line 56 - 76 RCO_Overall_Orchestrator
        # Line 74 - 129 RCO_Overall_Orchestrator New
        start_time, end_time, update_brandcode_data, only_modify_new_or_deleted_cos = get_analysis_time_bounds(db_connection, params)
        params['UpdateBrandcode'] = True if update_brandcode_data == 'yes' else False
        params['ModifyNewDeletedCOs'] = True if only_modify_new_or_deleted_cos == 'yes' else False

        # entire contents of RCO_ProficyiODS_Orchestrator - they are the 'extract' portion of ETL, lines 68-179
        line_dt, line_dt_full, machine_dt, machine_dt_full = dt_data_extract(mdc_header, params, line_params, start_time, end_time)
        prod_data = prod_data_extract(mdc_header, params, line_params, start_time, end_time)
        ## FOR TESTING
        runtime_per_day_data, day_starttime_per_line = runtime_data_extract(mdc_header, params, line_params, code_params, start_time, end_time)
        brandcode_data = brandcode_data_extract(mdc_header, params, code_params)
        # runtime_per_day_data, day_starttime_per_line = (None,) * 2
        # brandcode_data = None


        #############
        # TRANSFORM #
        #############

        # RCO_MES_ETL.R and all of its functionality will go in this area - in the R script, the Maple or Proficy orchestator
        # calls RCO_MES_ETL.R directly

        logging.info('ETL Started')

        co_aggregated_data, \
        co_event_log, \
        first_stop_after_co_data,\
        gantt_data, \
        event_log_for_gantt = mes_etl_main(params, line_params, code_params, line_dt, line_dt_full, machine_dt)

        # The output of [these scripts] gives out the data frames [CO_Aggregated_Data], [CO_Event_Log], [Runtime_per_Day_data], [BRANDCODE_data], [First_Stop_after_CO_Data], [Gantt_Data] and [Event_Log_for_Gantt].
        # The next section of this script mainly performs appending this new data to historical data already stored in Transformed Data Storage.

        '''
        if co_event_log:  # based on line 73, 74 of rco_mes_etl.R - if nothing is found, should be None and this will not execute
            print('this is what happens if etl returns changeover events')
    
            # script branches like the following analysis probably go here
            if params['first_stop_after_CO_analysis']:
                first_stop_analysis(line_dt)
    
            if params['SUDSpecific'] and machine_dt:
                sud_first_stop(line_dt, machine_dt)
    
        else:
            print('this is what happens if no changeovers are found')
        '''

        logging.info('ETL Completed')

        ########
        # LOAD #
        ########

        # todo: Define SQL table names to be used in Transformed Data Storage - can do this in the .env file, lines 16-26
        sql_tablename_co_aggregated_data = os.getenv('sql_tablename_co_aggregated_data')
        sql_tablename_co_event_log = os.getenv('sql_tablename_co_event_log')
        sql_tablename_script_data = os.getenv('sql_tablename_script_data')
        sql_tablename_runtime_per_day_data = os.getenv('sql_tablename_runtime_per_day_data')
        sql_tablename_brandcode_data = os.getenv('sql_tablename_brandcode_data')
        sql_tablename_gantt_data = os.getenv('sql_tablename_gantt_data')
        sql_tablename_event_log_for_gantt = os.getenv('sql_tablename_event_log_for_gantt')
        sql_tablename_first_stop_after_co_data = os.getenv('sql_tablename_first_stop_after_co_data')

        # todo: determine if converting non-latin characters to utf-16 is needed (lines 4-8, 43-53).
        #  Hoshin does not have this issue as far as I can tell.

        # todo: if there is at least one CO available in the data, perform few post-treatment steps.
        co_flag = False
        if len(co_event_log):
            co_flag = True
            co_event_log_full = co_event_log
            co_aggregated_data_full = co_aggregated_data
            co_aggregated_data_full['Brandcode_Status'].fillna('Unknown', inplace=True)

            # substitute character "'" which creates issues when writing/reading data to SQL.
            if params['MachineLevel']:
                gantt_data_full = gantt_data
                event_log_for_gantt_full = event_log_for_gantt
                event_log_for_gantt_full['OPERATOR_COMMENT'] = event_log_for_gantt_full['OPERATOR_COMMENT'].str.replace("'", " ", regex=True)
                event_log_for_gantt_full['CAUSE_LEVELS_3_NAME'] = event_log_for_gantt_full['CAUSE_LEVELS_3_NAME'].str.replace("'", " ", regex=True)
                event_log_for_gantt_full['CAUSE_LEVELS_4_NAME'] = event_log_for_gantt_full['CAUSE_LEVELS_4_NAME'].str.replace("'", " ", regex=True)
            if params['FirstStop']:
                first_stop_after_co_data_full = first_stop_after_co_data
                first_stop_after_co_data_full['OPERATOR_COMMENT'] = first_stop_after_co_data_full['OPERATOR_COMMENT']
                first_stop_after_co_data_full['CAUSE_LEVELS_3_NAME'] = first_stop_after_co_data_full['CAUSE_LEVELS_3_NAME']
                first_stop_after_co_data_full['CAUSE_LEVELS_4_NAME'] = first_stop_after_co_data_full['CAUSE_LEVELS_4_NAME']
                first_stop_after_co_data_full.dropna(subset=['START_TIME'], inplace=True)
            if len(co_event_log_full) > 0:
                co_event_log_full['OPERATOR_COMMENT'] = co_event_log_full['OPERATOR_COMMENT'].str.replace("'", " ", regex=True)
                co_event_log_full['CAUSE_LEVELS_3_NAME'] = co_event_log_full['CAUSE_LEVELS_3_NAME'].str.replace("'", " ", regex=True)
                co_event_log_full['CAUSE_LEVELS_4_NAME'] = co_event_log_full['CAUSE_LEVELS_4_NAME'].str.replace("'", " ", regex=True)
            if brandcode_data is not None and len(brandcode_data) > 0:      ## FOR TESTING
                brandcode_data['BRANDNAME'] = brandcode_data['BRANDNAME'].str.replace("'", " ", regex=True)

            # add blank column [Total_Uptime_till_Next_CO] if the First Stop after CO sub-RTL is not enabled.
            if 'Total_Uptime_till_Next_CO' not in co_aggregated_data_full.columns:
                co_aggregated_data_full['Total_Uptime_till_Next_CO'] = None

        if runtime_per_day_data is not None:        ## TEMPORARY MEASURE
            runtime_per_day_data['Server'] = params['SiteServer']
            runtime_per_day_data['Runtime'] = round(runtime_per_day_data['Runtime'].astype(float), 1)
            runtime_per_day_data_full = runtime_per_day_data

        time_pass = round((datetime.now() - start_marker).total_seconds() / 60, 1)
        logging.info('Time passed for MES data extraction & ETL: {} min'.format(time_pass))

        # NUMBER OF CONSTRAINTS DATA
        if line_dt is not None and len(line_dt) > 0:
            number_of_constraints_data = line_dt.groupby(by=['LINE', 'MACHINE'], as_index=False).agg(UPTIME=('UPTIME', sum))
            number_of_constraints_data = number_of_constraints_data.groupby(by='LINE', as_index=False) \
                                                               .agg(Number_of_Constraints=('LINE', 'count'))

        # NON-LATIN DATA MARKER
        # non_latin_servers = code_params['non_latin_servers']
        # write_to_sql_via_dbi = any(x in params['SiteServer'] for x in non_latin_servers)
        # write_to_sql_via_dbi = params['SiteServer'] in non_latin_servers
        write_to_sql_via_dbi = False

        # todo: run Transformed Data Storage appending per line
        for index in line_params.index:
            start_marker = datetime.now()
            system = line_params['System'][index]
            line_name = line_params['MDC_Line_Name'][index]
            logging.info('Equipment #: {} ({}) started'.format(index, system))

            # check if this line is already available in [Script_Data] and if not, add it.
            query = "SELECT * " \
                    f"FROM {sql_tablename_script_data} " \
                    "WHERE MES_Line_Name=? AND Server=?"
            new_rows = db_connection.execute(query, line_name, params['SiteServer']).fetchall()
            if len(new_rows) == 0:
                query = 'SELECT TOP 1 * ' \
                        f'FROM {sql_tablename_script_data}'
                ## FOR LOCAL DATABASE
                new_rows = db_connection.execute(query).fetchone()
                new_rows = pd.DataFrame(columns=['System', 'Data_Update_Time', 'First_Available_Data_Point', 'Last_Available_Data_Point',
                                    'MES_Line_Name', 'Server', 'Day_Start_hours', 'BU', 'Number_of_Constraints'])

                query = "SELECT min(CO_StartTime) as Min_time, max(CO_StartTime) as Max_time " \
                        f"FROM {sql_tablename_co_aggregated_data} " \
                        "WHERE Line=? AND Server=?"
                result = db_connection.execute(query, line_name, params['SiteServer']).first()

                new_rows = new_rows.append({'System': system, 'MES_Line_Name': line_name, 'Server': params['SiteServer'], 'BU': 'FHC', 'Data_Update_Time': datetime.now(), 'First_Available_Data_Point': result[0], 'Last_Available_Data_Point': result[1]}, ignore_index=True)

                # day_start_hours = day_starttime_per_line[day_starttime_per_line['LINE'] == line_name]['Day_Start_hours'].values
                day_start_hours = []        ## FOR TESTING
                if len(day_start_hours) > 0:
                    new_rows['Day_Start_hours'].iloc[0] = day_start_hours[0]
                else:
                    new_rows['Day_Start_hours'] = 6

                if pd.isna(new_rows['Number_of_Constraints'].iloc[0]):
                    new_rows['Number_of_Constraints'].iloc[0] = 1

                if params['MultiConstraint']:
                    temp = number_of_constraints_data[number_of_constraints_data['LINE'] == line_name]
                    if len(temp) > 0:
                        if temp['Number_of_Constraints'].iloc[0] > new_rows['Number_of_Constraints'].iloc[0]:
                            new_rows['Number_of_Constraints'].iloc[0] = temp['Number_of_Constraints'].iloc[0]
                    new_rows = new_rows.astype({'Number_of_Constraints': 'int'})

                append_result = append_data_to_sql(db_connection, new_rows, sql_tablename_script_data)

            # run appending CO data to Transformed Data Storage only if a CO for the specific Line is available.
            if co_flag:
                starttime = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')

                # filter data for the specific line
                co_aggregated_data = co_aggregated_data_full[co_aggregated_data_full['LINE'] == line_name]
                co_event_log = co_event_log_full[co_event_log_full['LINE'] == line_name]
                if params['MachineLevel']:
                    gantt_data = gantt_data_full[gantt_data_full['Line'] == line_name]
                    event_log_for_gantt = event_log_for_gantt_full[event_log_for_gantt_full['LINE'] == line_name]
                if params['FirstStop']:
                    first_stop_after_co_data = first_stop_after_co_data_full[first_stop_after_co_data_full['LINE'] == line_name]

                if len(co_aggregated_data) > 0:
                    if params['ModifyNewDeletedCOs']:
                        # extract the COs already available in the Transformed Data Storage for the timespan data re-taken from MES
                        query = "SELECT * " \
                                f"FROM {sql_tablename_co_aggregated_data} " \
                                f"WHERE Line=? AND Server=? AND CO_StartTime>=CONVERT(datetime, ?)"
                        already_available_cos = pd.read_sql(query, db_connection, params=[line_name, params['SiteServer'], f'{(starttime - timedelta(seconds=60)):%Y-%m-%d %H:%M:%S}'])
                        already_available_cos.columns = already_available_cos.columns.str.lower()

                        query = 'SELECT * ' \
                                f'FROM {sql_tablename_co_event_log} ' \
                                f'WHERE Line=? AND Server=? AND START_TIME>=CONVERT(datetime, ?)'
                        already_available_co_events = pd.read_sql(query, db_connection, params=[line_name, params['SiteServer'], f'{(starttime - timedelta(seconds=60)):%Y-%m-%d %H:%M:%S}'])
                        already_available_co_events.columns = already_available_co_events.columns.str.lower()

                        if len(already_available_cos) > 0:
                            # eliminate the COs in [Gantt_Data] and [Event_Log_for_Gantt] which are already having identical data available in Transformed Data Storage, to prevent time loss due to re-writing them
                            already_available_cos['co_downtime'] = round(already_available_cos['co_downtime'].astype(float), 2)
                            already_available_cos['total_uptime_till_next_co'] = round(already_available_cos['total_uptime_till_next_co'].astype(float), 2)
                            events_list = already_available_co_events.groupby(by='co_identifier', as_index=False)\
                                                                     .agg(number_of_events=('co_identifier', 'count'),
                                                                          event_downtime=('downtime', sum))
                            events_list['event_downtime'] = round(events_list['event_downtime'].astype(float), 1)
                            already_available_cos = already_available_cos.merge(events_list, on='co_identifier', how='left')
                            already_available_cos['number_of_events'].fillna(0, inplace=True)
                            already_available_cos['event_downtime'].fillna(0, inplace=True)
                            already_available_cos['temp'] = already_available_cos['co_starttime'].astype(str) + '_' + already_available_cos['co_downtime'].astype(str) + '_' + \
                                                            already_available_cos['co_identifier'].astype(str) + '_' + already_available_cos['number_of_events'].astype(str) + '_ ' + \
                                                            already_available_cos['event_downtime'].astype(str) + '_' + already_available_cos['total_uptime_till_next_co'].astype(str)
                            co_aggregated_data_temp = co_aggregated_data
                            co_aggregated_data_temp['CO_DOWNTIME'] = round(co_aggregated_data_temp['CO_DOWNTIME'], 2)
                            co_aggregated_data_temp['Total_Uptime_till_Next_CO'] = round(co_aggregated_data_temp['Total_Uptime_till_Next_CO'], 2)
                            number_of_events_list = co_event_log.groupby(by='CO_Identifier', as_index=False)\
                                                                .agg(number_of_events=('CO_Identifier', 'count'),
                                                                     event_downtime=('DOWNTIME', sum))
                            number_of_events_list['event_downtime'] = round(number_of_events_list['event_downtime'], 1)
                            co_aggregated_data_temp = co_aggregated_data_temp.merge(number_of_events_list, on='CO_Identifier', how='left')
                            co_aggregated_data_temp['temp'] = co_aggregated_data_temp['CO_StartTime'].astype(str) + '_' + co_aggregated_data_temp['CO_DOWNTIME'].astype(str) + '_' + \
                                                              co_aggregated_data_temp['CO_Identifier'].astype(str) + '_' + co_aggregated_data_temp['number_of_events'].astype(str) + '_' + \
                                                              co_aggregated_data_temp['event_downtime'].astype(str) + '_' + co_aggregated_data_temp['Total_Uptime_till_Next_CO'].astype(str)
                            new_cos = co_aggregated_data_temp[~co_aggregated_data_temp['temp'].isin(pd.unique(already_available_cos['temp']))]
                            deleted_cos = already_available_cos[~already_available_cos['temp'].isin(pd.unique(co_aggregated_data_temp['temp']))]

                            if len(deleted_cos) > 0:
                                for i in range(len(deleted_cos)):
                                    co_identifier = deleted_cos['co_identifier'].iloc[i]
                                    query = 'DELETE ' \
                                            f'FROM {sql_tablename_co_aggregated_data} ' \
                                            'WHERE CO_Identifier=? AND Server=?'
                                    db_connection.execute(query, co_identifier, params['SiteServer'])

                                    query = 'DELETE ' \
                                            f'FROM {sql_tablename_co_event_log} ' \
                                            'WHERE CO_Identifier=? AND Server=?'
                                    db_connection.execute(query, co_identifier, params['SiteServer'])

                                    if params['MachineLevel']:
                                        query = 'DELETE ' \
                                                f'FROM {sql_tablename_gantt_data} ' \
                                                'WHERE CO_Identifier=? AND Server=?'
                                        db_connection.execute(query, co_identifier, params['SiteServer'])

                                        query = 'DELETE ' \
                                                f'FROM {sql_tablename_event_log_for_gantt} ' \
                                                'WHERE CO_Identifier=? AND Server=?'
                                        db_connection.execute(query, co_identifier, params['SiteServer'])

                                    if params['FirstStop']:
                                        query = 'DELETE ' \
                                                f'FROM {sql_tablename_first_stop_after_co_data} ' \
                                                'WHERE CO_Identifier=? AND Server=?'
                                        db_connection.execute(query, co_identifier, params['SiteServer'])

                                logging.info(f'Number of Deleted COs: {len(deleted_cos)}')

                            co_aggregated_data = co_aggregated_data[co_aggregated_data['CO_Identifier'].isin(pd.unique(new_cos['CO_Identifier']))]
                            co_event_log = co_event_log[co_event_log['CO_Identifier'].isin(pd.unique(new_cos['CO_Identifier']))]
                            if params['MachineLevel']:
                                gantt_data = gantt_data[gantt_data['CO_Identifier'].isin(pd.unique(new_cos['CO_Identifier']))]
                                event_log_for_gantt = event_log_for_gantt[event_log_for_gantt['CO_Identifier'].isin(pd.unique(new_cos['CO_Identifier']))]
                            if params['FirstStop']:
                                first_stop_after_co_data = first_stop_after_co_data[first_stop_after_co_data['CO_Identifier'].isin(pd.unique(new_cos['CO_Identifier']))]

                else:
                    logging.info('CO-related data NOT updated due to no new COs.')

                if len(co_aggregated_data) > 0:
                    # create queries to take number of rows before/after data appending
                    query_co_aggregated_data = 'SELECT COUNT(*) ' \
                                               f'FROM {sql_tablename_co_aggregated_data} ' \
                                               'WHERE LINE=? AND Server=?'
                    init_co_aggregated_data = db_connection.execute(query_co_aggregated_data, line_name, params['SiteServer']).fetchone()[0]   # these count variables are generated to compare the number of rows in data before/after to check how many new entries are generated.

                    query_co_event_log = 'SELECT COUNT(*) ' \
                                         f'FROM {sql_tablename_co_event_log} ' \
                                         'WHERE LINE=? AND Server=?'
                    init_co_event_log = db_connection.execute(query_co_event_log, line_name, params['SiteServer']).fetchone()[0]

                    if params['MachineLevel']:
                        query_gantt_data = 'SELECT COUNT(*) ' \
                                           f'FROM {sql_tablename_gantt_data} ' \
                                           'WHERE Line=? AND Server=?'
                        init_gantt_data = db_connection.execute(query_gantt_data, line_name, params['SiteServer']).fetchone()[0]

                        query_event_log_for_gantt = 'SELECT COUNT(*) ' \
                                                    f'FROM {sql_tablename_event_log_for_gantt} ' \
                                                    'WHERE Line=? AND Server=?'
                        init_event_log_for_gantt = db_connection.execute(query_event_log_for_gantt, line_name, params['SiteServer']).fetchone()[0]

                    if params['FirstStop']:
                        query_first_stop_after_co_data = 'SELECT COUNT(*) ' \
                                                         f'FROM {sql_tablename_first_stop_after_co_data} ' \
                                                         'WHERE LINE=? AND Server=?'
                        init_first_stop_after_co_data = db_connection.execute(query_first_stop_after_co_data, line_name, params['SiteServer']).fetchone()[0]

                    if params['ModifyNewDeletedCOs']:
                        # remove all COs from [CO_Aggregated_Data] which happened after the StartTime of newly taken data.
                        temp2 = co_aggregated_data['CO_StartTime'].min().tz_localize(None).to_pydatetime()
                        temp3 = starttime
                        if temp3 < temp2:
                            temp2 = temp3
                        temp2 = temp2 - timedelta(seconds=10)   # to accomodate for also removing historical COs whose StartTime moved couple of seconds.
                        temp2_backup = temp2
                        query = 'DELETE ' \
                                f'FROM {sql_tablename_co_aggregated_data} ' \
                                'WHERE Line=? AND Server=? AND CO_StartTime>=CONVERT(datetime, ?)'
                        db_connection.execute(query, line_name, params['SiteServer'], f'{temp2:%Y-%m-%d %H:%M:%S}')

                        # after deleting the historical COs in Transformed Data Storage which are also available in newly extracted data, get the last CO available in Transformed Data Storage.
                        query = 'SELECT TOP 1 CO_Identifier ' \
                                f'FROM {sql_tablename_co_aggregated_data} ' \
                                'WHERE Line=? AND Server=? ' \
                                'ORDER BY CO_StartTime DESC'        # MODIFIED QUERY TO MAKE IT EASIER TO EXTRACT DATA
                        temp = db_connection.execute(query, line_name, params['SiteServer']).fetchone()
                        if temp is not None:
                            temp = temp[0]

                        # get the last [START_TIME] of last event of last CO available in Transformed Data Storage.
                        query = 'SELECT MAX(START_TIME) ' \
                                f'FROM {sql_tablename_co_event_log} ' \
                                'WHERE CO_Identifier=? and Server=?'
                        temp2 = db_connection.execute(query, temp, params['SiteServer']).fetchone()[0]

                        if temp2 is None:       # if that CO is not found, use the [CO_EndTime] of the last CO.
                            query = 'SELECT TOP 1 CO_EndTime ' \
                                    f'FROM {sql_tablename_co_aggregated_data} ' \
                                    'WHERE Line=? and Server=? ' \
                                    'ORDER BY CO_StartTime DESC'    # MODIFIED QUERY TO MAKE IT EASIER TO EXTRACT DATA
                            temp = db_connection.execute(query, line_name, params['SiteServer']).fetchone()
                            if temp is not None:
                                temp = temp[0]
                        if temp2 is None:
                            temp2 = temp2_backup
                        # the variable is temp2 is set to the [CO_EndTime] is the last available CO for the given line after above delete operation.
                        # This variable is then used to delete the rows from other relevant table in next section.

                        # delete entries from other tables similar to COs deleted from [CO_Aggregated_Data]
                        query = 'DELETE ' \
                                f'FROM {sql_tablename_co_event_log} ' \
                                'WHERE Line=? AND Server=? AND START_TIME>CONVERT(datetime, ?)'
                        db_connection.execute(query, line_name, params['SiteServer'], f'{temp2:%Y-%m-%d %H:%M:%S}')

                        if params['MachineLevel']:
                            temp3 = temp2 + timedelta(minutes=20)
                            query = 'DELETE ' \
                                    f'FROM {sql_tablename_gantt_data} ' \
                                    'WHERE Line=? AND Server=? and StartTime>CONVERT(datetime, ?)'
                            db_connection.execute(query, line_name, params['SiteServer'], f'{temp3:%Y-%m-%d %H:%M:%S}')

                            query = 'DELETE ' \
                                    f'FROM {sql_tablename_event_log_for_gantt} ' \
                                    'WHERE LINE=? AND Server=? AND START_TIME>CONVERT(datetime, ?)'
                            db_connection.execute(query, line_name, params['SiteServer'], f'{temp3:%Y-%m-%d %H:%M:%S}')

                        if params['FirstStop']:
                            if len(first_stop_after_co_data) > 0:
                                temp2 = co_aggregated_data['CO_StartTime'].min()
                                query = 'DELETE ' \
                                        f'FROM {sql_tablename_first_stop_after_co_data} ' \
                                        'WHERE LINE=? AND Server=? AND START_TIME>CONVERT(datetime, ?)'
                                db_connection.execute(query, line_name, params['SiteServer'], f'{temp2:%Y-%m-%d %H:%M:%S}')

                    # append new COs to [CO_Aggregate_Data]
                    # TEMPORARY FIX BY CHANGING DOWNTIME_ID TO DOWNTIME_PK
                    co_aggregated_data.rename(columns={'downtime_id_of_First_CO_Event': 'downtime_pk_of_first_co_event',
                                                       'downtime_id_of_Last_CO_Event': 'downtime_pk_of_last_co_event'},
                                              inplace=True)
                    append_result = append_data_to_sql(db_connection, co_aggregated_data, sql_tablename_co_aggregated_data)

                    # append new CO events to [CO_Event_Log] - note that for non-Latin data sources, different library is used, as this table may include non-Latin characters.
                    if write_to_sql_via_dbi:
                        co_event_log['CAUSE_LEVELS_1_NAME'] = co_event_log['CAUSE_LEVELS_1_NAME'].apply(lambda x: convert_to_utf16(x))
                        co_event_log['CAUSE_LEVELS_2_NAME'] = co_event_log['CAUSE_LEVELS_2_NAME'].apply(lambda x: convert_to_utf16(x))
                        co_event_log['CAUSE_LEVELS_3_NAME'] = co_event_log['CAUSE_LEVELS_3_NAME'].apply(lambda x: convert_to_utf16(x))
                        co_event_log['CAUSE_LEVELS_4_NAME'] = co_event_log['CAUSE_LEVELS_4_NAME'].apply(lambda x: convert_to_utf16(x))
                        co_event_log['OPERATOR_COMMENT'] = co_event_log['OPERATOR_COMMENT'].apply(lambda x: convert_to_utf16(x))
                        if 'ProdDesc' in co_event_log.columns:
                            co_event_log['ProdDesc'] = co_event_log['ProdDesc'].apply(lambda x: convert_to_utf16(x))

                    # TEMPORARY FIX BY CHANGING DOWNTIME_ID TO DOWNTIME_PK
                    co_event_log.rename(columns={'downtime_id': 'downtime_pk'}, inplace=True)
                    append_result = append_data_to_sql(db_connection, co_event_log, sql_tablename_co_event_log)

                    logging.info(f"Delta rows in CO_Aggregated_Data: {db_connection.execute(query_co_aggregated_data, line_name, params['SiteServer']).fetchone()[0] - init_co_aggregated_data}")
                    logging.info(f"Delta rows in CO_Event_Log: {db_connection.execute(query_co_event_log, line_name, params['SiteServer']).fetchone()[0] - init_co_event_log}")

                    # append new machine level data to [Gantt_Data]
                    # append new machine level data to [Event_Log_for_Gantt] - note that for non-latin data sources, different library is used, as this table may inclide non-Latin characters.
                    if params['MachineLevel']:
                        if len(gantt_data) > 0:
                            # TEMPORARY FIX BY CHANGING DOWNTIME_ID TO DOWNTIME_PK
                            gantt_data.rename(columns={'downtime_id': 'downtime_pk'}, inplace=True)
                            append_result = append_data_to_sql(db_connection, gantt_data, sql_tablename_gantt_data)
                            if append_result:
                                logging.error(f'Error appending data to {sql_tablename_gantt_data}: {append_result}')

                            logging.info(f"Delta rows in Gantt_Data: {db_connection.execute(query_gantt_data, line_name, params['SiteServer']).fetchone()[0] - init_gantt_data}")

                        if len(event_log_for_gantt) > 0:
                            if write_to_sql_via_dbi:
                                event_log_for_gantt['CAUSE_LEVELS_1_NAME'] = event_log_for_gantt['CAUSE_LEVELS_1_NAME'].apply(lambda x: convert_to_utf16(x))
                                event_log_for_gantt['CAUSE_LEVELS_2_NAME'] = event_log_for_gantt['CAUSE_LEVELS_2_NAME'].apply(lambda x: convert_to_utf16(x))
                                event_log_for_gantt['CAUSE_LEVELS_3_NAME'] = event_log_for_gantt['CAUSE_LEVELS_3_NAME'].apply(lambda x: convert_to_utf16(x))
                                event_log_for_gantt['CAUSE_LEVELS_4_NAME'] = event_log_for_gantt['CAUSE_LEVELS_4_NAME'].apply(lambda x: convert_to_utf16(x))
                                event_log_for_gantt['OPERATOR_COMMENT'] = event_log_for_gantt['OPERATOR_COMMENT'].apply(lambda x: convert_to_utf16(x))
                                if 'Fault' in event_log_for_gantt.columns:
                                    event_log_for_gantt['Fault'] = event_log_for_gantt['Fault'].apply(lambda x: convert_to_utf16(x))

                            # TEMPORARY FIX BY CHANGING DOWNTIME_ID TO DOWNTIME_PK
                            event_log_for_gantt.rename(columns={'downtime_id': 'downtime_pk'}, inplace=True)
                            append_result = append_data_to_sql(db_connection, event_log_for_gantt, sql_tablename_event_log_for_gantt)

                            logging.info(f"Delta rows in Event_Log_for_Gantt: {db_connection.execute(query_event_log_for_gantt, line_name, params['SiteServer']).fetchone()[0] - init_event_log_for_gantt}")

                    # append new machine level data to ['First_Stop_after_CO_Data] - note that for non-Latin data sources, different library is used, as this table may include non-Latin characters.
                    if params['FirstStop']:
                        if len(first_stop_after_co_data) > 0:
                            if write_to_sql_via_dbi:
                                first_stop_after_co_data['CAUSE_LEVELS_1_NAME'] = first_stop_after_co_data['CAUSE_LEVELS_1_NAME'].apply(lambda x: convert_to_utf16(x))
                                first_stop_after_co_data['CAUSE_LEVELS_2_NAME'] = first_stop_after_co_data['CAUSE_LEVELS_2_NAME'].apply(lambda x: convert_to_utf16(x))
                                first_stop_after_co_data['CAUSE_LEVELS_3_NAME'] = first_stop_after_co_data['CAUSE_LEVELS_3_NAME'].apply(lambda x: convert_to_utf16(x))
                                first_stop_after_co_data['CAUSE_LEVELS_4_NAME'] = first_stop_after_co_data['CAUSE_LEVELS_4_NAME'].apply(lambda x: convert_to_utf16(x))
                                first_stop_after_co_data['OPERATOR_COMMENT'] = first_stop_after_co_data['OPERATOR_COMMENT'].apply(lambda x: convert_to_utf16(x))

                                if 'Fault' in first_stop_after_co_data.columns:
                                    first_stop_after_co_data['Fault'] = first_stop_after_co_data['Fault'].apply(lambda x: convert_to_utf16(x))

                            # TEMPORARY FIX BY CHANGING DOWNTIME_ID TO DOWNTIME_PK
                            first_stop_after_co_data = first_stop_after_co_data.astype({'downtime_id': 'str'})
                            first_stop_after_co_data.rename(columns={'downtime_id': 'downtime_pk'}, inplace=True)
                            append_result = append_data_to_sql(db_connection, first_stop_after_co_data, sql_tablename_first_stop_after_co_data)

                            logging.info(f"Delta rows in First_Stop_after_CO_Data: {db_connection.execute(query_first_stop_after_co_data, line_name, params['SiteServer']).fetchone()[0] - init_first_stop_after_co_data}")

                else:
                    logging.info('CO-related data NOT updated due to no new COs.')

            else:
                logging.info('CO-related data NOT updated due to no new COs.')

            # Appending new Runtime data to historical data stored in Transformed Data Storage.
            # Note that this part runs even if no new COs are in place.
            if runtime_per_day_data is not None:        ## TEMPORARY MEASURE
                runtime_per_day_data = runtime_per_day_data_full[runtime_per_day_data_full['LINE'] == line_name]
                if len(runtime_per_day_data) > 0:
                    query_runtime_per_day_data = 'SELECT COUNT(*) ' \
                                                 f'FROM {sql_tablename_runtime_per_day_data} ' \
                                                 'WHERE LINE=? AND Server=?'
                    init_runtime_per_day_data = db_connection.execute(query_runtime_per_day_data, line_name, params['SiteServer']).fetchone()[0]

                    temp2 = runtime_per_day_data['Date'].min()
                    query = 'DELETE ' \
                            f'FROM {sql_tablename_runtime_per_day_data} ' \
                            'WHERE Line=? AND Server=? AND Date>CONVERT(datetime, ?)'
                    db_connection.execute(query, line_name, params['SiteServer'], f'{temp2:%Y-%m-%d}')

                    append_result = append_data_to_sql(db_connection, runtime_per_day_data, sql_tablename_runtime_per_day_data)

                    logging.info(f"Delta rows in Runtime_per_Day_data: {db_connection.execute(query_runtime_per_day_data, line_name, params['SiteServer']).fetchone()[0] - init_runtime_per_day_data}")

            query = 'SELECT * ' \
                    f'FROM {sql_tablename_script_data} ' \
                    'WHERE MES_Line_Name=? AND Server=?'
            existing_rows = pd.read_sql(query, db_connection, params=[line_name, params['SiteServer']])
            existing_rows.columns = existing_rows.columns.str.lower()

            data_update_time = start_marker
            query = 'SELECT MIN(CO_StartTime) as Min_time, MAX(CO_StartTime) AS Max_time ' \
                    f'FROM {sql_tablename_co_aggregated_data} ' \
                    'WHERE Line=? AND Server=?'
            temp = db_connection.execute(query, line_name, params['SiteServer']).first()
            first_available_data_point = temp[0]
            last_available_data_point = temp[1]

            day_start_hours = day_starttime_per_line[day_starttime_per_line['LINE'] == line_name]['Day_Start_hours']
            # day_start_hours = []        ## FOR TESTING
            if len(day_start_hours) == 0:
                if len(existing_rows) > 0:
                    day_start_hours = float(existing_rows['day_start_hours'].iloc[0])
                else:
                    day_start_hours = None
            else:
                day_start_hours = day_start_hours.iloc[0]

            if len(existing_rows) > 0:
                if pd.isna(existing_rows['number_of_constraints'].iloc[0]):
                    number_of_constraints = 1
                else:
                    number_of_constraints = existing_rows['number_of_constraints'].iloc[0]
            else:
                number_of_constraints = 1

            if params['MultiConstraint']:
                temp = number_of_constraints_data[number_of_constraints_data['LINE'] == line_name]
                if len(temp) > 0:
                    if temp['Number_of_Constraints'].iloc[0] > number_of_constraints:
                        number_of_constraints = temp['Number_of_Constraints'].iloc[0]

            query = f'UPDATE {sql_tablename_script_data} ' \
                    'SET Data_Update_Time=CONVERT(datetime, ?), First_Available_Data_Point=CONVERT(datetime, ?), ' \
                    'Last_Available_Data_Point=CONVERT(datetime, ?), Day_Start_hours=?, Number_of_Constraints=? ' \
                    'WHERE MES_Line_Name=? AND Server=?'
            db_connection.execute(query, f'{data_update_time:%Y-%m-%d %H:%M:%S}', (f'{first_available_data_point:%Y-%m-%d %H:%M:%S}' if first_available_data_point else None),
                                  (f'{last_available_data_point:%Y-%m-%d %H:%M:%S}' if last_available_data_point else None), (round(day_start_hours, 2) if day_start_hours else None),
                                  int(number_of_constraints), line_name, params['SiteServer'])

            time_pass = round((datetime.now() - start_marker).total_seconds() / 60, 1)
            logging.info(f'Time passed for data appending to Transformed Data Storage for {system}: {time_pass} min')

        # save non-line dedicated MES tables. ([BRANDCODE_DATA])
        # note that all historical data stored in Transformed Data Storage is extracted and combined with new data, removing duplicates.
        # Them this append data is re-wrote back in Transformed Data Storage. This logic is needed for Proficy iODS sites.
        if params['UpdateBrandcode'] and brandcode_data is not None:        ## TEMPORARY MEASURE
            start_marker = datetime.now()
            query_brandcode_data = 'SELECT COUNT(*) ' \
                                   f'FROM {sql_tablename_brandcode_data} ' \
                                   'WHERE Server=?'
            init_brandcode_data = db_connection.execute(query_brandcode_data, params['SiteServer']).fetchone()[0]  # these count variables are generated to compare the number of rows in data before/after to check how many new entries are generated.

            query = 'SELECT * ' \
                    f'FROM {sql_tablename_brandcode_data} ' \
                    'WHERE Server=? '
            temp = pd.read_sql(query, params=[params['SiteServer']])
            temp.columns = temp.columns.str.lower()

            temp = temp.astype({'brandcode': 'str'})
            temp = temp[~(temp['brandcode'].isin(pd.unique(brandcode_data['BRANDCODE'])))]

            brandcode_data.columns = brandcode_data.columns.str.lower()
            brandcode_data = brandcode_data.append(temp, ignore_index=True)

            query = 'DELETE ' \
                    f'FROM {sql_tablename_brandcode_data} ' \
                    'WHERE Server=?'
            db_connection.execute(query, params['SiteServer'])

            if write_to_sql_via_dbi:
                brandcode_data['brandname'] = brandcode_data['brandname'].apple(lambda x: convert_to_utf16(x))
            append_result = append_data_to_sql(db_connection, brandcode_data, sql_tablename_brandcode_data)

            logging.info(f"Delta rows in Brandcode_Data: {db_connection.execute(query_brandcode_data, params['SiteServer']).fetchone()[0] - init_brandcode_data}")

            time_pass = round((datetime.now() - start_marker).total_seconds() / 60, 1)
            logging.info(f'Time passed for data appending to Transformed Data Storage for Brandcode Data: {time_pass} min')

        else:
            logging.info('Brandcode data NOT updated.')
        # todo: change this to not always be true
        return 'Success'

    except Exception as e:
        logging.exception(f'SCRIPT FAILED: {e}', exc_info=True)
        return get_exception()