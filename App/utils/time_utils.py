from datetime import datetime
from dateutil import relativedelta as rd
import logging


def get_analysis_time_bounds(engine, site_params: dict):
    # Get current time
    sys_time = datetime.now()
    current_hour = sys_time.hour
    # todo: perhaps un-hardcode the table name
    time_query = "SELECT MAX(Data_Update_Time) FROM rco_v1_script_data WHERE Server=?"
    last_update = engine.execute(time_query, site_params['SiteServer']).first()[0]
    if not last_update:
        last_update = sys_time  # if we have nothing for this Site, get last X days from today.
        # todo make this longer - like 3 months?.

    """
    COMMENTING AND CHANGING IT TO MATCH CURRENT R SCRIPT
    if current_hour != 3:  # if we're running this in the hour of 3am, get last 7 days instead of 3.
        # todo there has to be a better way to do accomplish this.
        start_time = last_update + rd.relativedelta(days=-3)
    else:
        start_time = last_update + rd.relativedelta(days=-7)
    """
    if current_hour == 3:
        number_of_days_to_look_back = 7
        site_params['MachineLevel'] = False
        update_brandcode_data = 'yes'
        only_modify_new_or_deleted_cos = 'no'
    elif current_hour == 20:
        number_of_days_to_look_back = 2
        update_brandcode_data = 'no'
        only_modify_new_or_deleted_cos = 'no'
    elif current_hour == 21:
        number_of_days_to_look_back = 14
        site_params['MachineLevel'] = False
        update_brandcode_data = 'yes'
        only_modify_new_or_deleted_cos = 'no'
    elif current_hour == 22:
        number_of_days_to_look_back = 7
        update_brandcode_data = 'yes'
        only_modify_new_or_deleted_cos = 'no'
    else:
        number_of_days_to_look_back = 3
        update_brandcode_data = 'no'
        only_modify_new_or_deleted_cos = 'yes'

    logging.info(f'Number of days data extracted: {number_of_days_to_look_back}')
    logging.info(f'Brandcode Data Updating Active Status: {update_brandcode_data}')
    logging.info(f'Modification of Only New or Deleted COs Active Status: {only_modify_new_or_deleted_cos}')

    start_time = last_update - rd.relativedelta(days=number_of_days_to_look_back)
    start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_time = sys_time + rd.relativedelta(days=1)
    end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')
    return start_time, end_time, update_brandcode_data, only_modify_new_or_deleted_cos
