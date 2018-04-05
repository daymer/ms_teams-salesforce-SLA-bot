from simple_salesforce import Salesforce
import simple_salesforce
from configuration import SFConfig, SFQueues, SQLConfig, TeamsChannels, Integration
import logging
from datetime import datetime
import custom_logic
import sys
import time as other_time
from logger_init import logging_config

##############################################################
#                        variables                           #
MaxAllowedSLA = 61
Query_Delay = 60


#                                                            #
##############################################################

configuration_inst = Integration()
MainLogger = logging_config(integration_config=configuration_inst, logging_mode='INFO', log_to_file=True, executable_path = __file__)


def initialize(sf_config_ins_func: SFConfig):
    logger_inst = logging.getLogger()
    try:
        sf_connection = Salesforce(username=sf_config_ins_func.user, password=sf_config_ins_func.password,
                                   security_token=sf_config_ins_func.token)
    except simple_salesforce.exceptions.SalesforceAuthenticationFailed as error:
        logger_inst.error('Failed to connect to SalesForce due to the following error:\n' + str(error))
        sf_connection = None
    return logger_inst, sf_connection


sf_config_instance = SFConfig()
sf_queues_instance = SFQueues()
sql_config_instance = SQLConfig()
teams_channels_inst = TeamsChannels()
sql_connector_instance = custom_logic.SQLConnector(sql_config_instance)
SF_connection = initialize(sf_config_ins_func=sf_config_instance)
MainLogger.info('Main process has been initialized')


##############################################################


def a_rule(rule_start: int, rule_end: int, sf_connection: Salesforce):
    main_logger = logging.getLogger()
    main_logger.info(
        'A1: Searching for new potential SLA violations by Rule:' + str(rule_end) + '<SLA<' + str(rule_start))
    found_cases_list = custom_logic.find_cases_with_potential_sla(sf_connection=sf_connection,
                                                                  max_allowed_sla=rule_start, min_allowed_sla=rule_end)

    if len(found_cases_list) == 0:
        main_logger.info('Done, no threats were found')
    else:
        main_logger.info('Done, found ' + str(len(found_cases_list)) + ' case(s)')

    for case_dict in found_cases_list:
        case_dict['target_notification_channel'] = custom_logic.find_target_teams_channel(case_dict['OwnerId'],
                                                                                          case_dict[
                                                                                              'Previous_Owner__c'],
                                                                                          case_dict['Product__c'])
        result = sql_connector_instance.insert_into_dbo_cases(case_dict=case_dict, rule=str(rule_start))
        if result is not False:
            pass
        else:
            main_logger.error('Some error has occurred, braking execution and notifying an admin')
            if isinstance(main_logger.root.handlers[0], logging.FileHandler):
                main_logger.error('Log name: ' + main_logger.root.handlers[0].baseFilename)
                exit(1)


Global_UTC_current_time_hour = datetime.utcnow().hour


def main_execution():
    global Global_UTC_current_time_hour
    Global_UTC_current_time_hour = datetime.now().hour
    if 4 < Global_UTC_current_time_hour < 17:
        sf_config_inst_2 = SFConfig()
        s_f_connection = Salesforce(username=sf_config_inst_2.user, password=sf_config_inst_2.password,
                                   security_token=sf_config_inst_2.token)
        # Block A: loading source threats and uploading them to DB
        #   A1: Loading SLA cases from all Tier 1 Queues with potentially broken SLA: RULE 60
        rule_a1 = MaxAllowedSLA
        rule_a1_end = 30
        a_rule(rule_a1, rule_a1_end, s_f_connection)
        # A2: Loading SLA cases from all Tier 1 Queues with potentially broken SLA: RULE 30
        rule_a2 = 31
        rule_a2_end = 0
        a_rule(rule_a2, rule_a2_end, s_f_connection)
        # A3: Loading SLA cases from all Tier 1 Queues with potentially broken SLA: RULE 1
        rule_a3 = 1
        rule_a3_end = 0
        a_rule(rule_a3, rule_a3_end, s_f_connection)
        # Block B: loading threats
        MainLogger.info('Loading threats')

        threats = []
        #   B1:
        MainLogger.info('Loading cases with bad SLA')
        try:
            threats += sql_connector_instance.select_all_unanswered_threats_from_cases()
        except custom_logic.NoThreadsFound as error:
            logging.info(error)

        if len(threats) > 0:
            MainLogger.info('threats loaded, processing')
        else:
            MainLogger.debug('no threats found, skipping')
        # Block C: reacting on threats

        for Threat in threats:
            if not isinstance(Threat, custom_logic.CaseSLA):
                MainLogger.debug('Case' + str(Threat.case_info_tuple[2]) + 'Unsupported threat type, skipping')
                continue
            try:
                # In order to save the info regarding currently unsupported target_notification_channels,
                # we should resolve CO and pCO:
                if Threat.target_notification_channel == 'undefined':
                    MainLogger.error('Case ' + str(
                        Threat.case_info_tuple[2]) + ': unsupported target_notification_channel type, skipping')
                    co = Threat.case_info_tuple[3]
                    p_co = Threat.case_info_tuple[11]
                    # both could be a user or a group
                    try:
                        MainLogger.debug('Trying to locate CO')
                        co = custom_logic.sf_get_user_or_group(sf_connection=s_f_connection, user_or_group_id=co)[0]
                    except custom_logic.SFGetUserNameError:
                        MainLogger.debug('Failed to locate CO, skipping')
                        pass
                    try:
                        MainLogger.debug('Trying to locate pCOQ')
                        p_co = custom_logic.sf_get_user_or_group(sf_connection=s_f_connection, user_or_group_id=p_co)[0]
                    except custom_logic.SFGetUserNameError:
                        MainLogger.debug('Failed to locate pCOQ, skipping')
                        pass
                    MainLogger.info(
                        'Unsupported target notification channel, CO of the case is:' + str(co) + ' and pCOQ is:' + str(
                            p_co))
                    sql_connector_instance.update_dbo_cases_after_notification_sent(row_id=Threat.case_info_tuple[1])
                elif Threat.target_notification_channel.startswith('https://outlook.office.com/webhook/'):
                    MainLogger.info('Reacting on threat: case ' + str(Threat.case_info_tuple[2]))
                    Threat.current_SLA = custom_logic.get_current_case_sla(sf_connection=s_f_connection,
                                                                           case_id=Threat.case_info_tuple[4])

                    # Step 1: looking for an appropriate A rule:
                    # A rule is already broken
                    if Threat.current_SLA is None:
                        sql_connector_instance.update_dbo_cases_after_notification_sent(
                            row_id=Threat.case_info_tuple[1])
                        MainLogger.debug('Threat.current_SLA:' + str(Threat.current_SLA))
                        MainLogger.info('Threat neutralized, but not processed: it\'s too late')
                        continue
                    # A rule is not violated, looking for a channel
                    MainLogger.info('Looking for an appropriate A rule, current case SLA:' + str(Threat.current_SLA))
                    Threat.current_SLA = int(Threat.current_SLA)
                    if Threat.current_SLA > rule_a2 and Threat.case_info_tuple[1]:
                        # Adding a special forwarding rule A1_1-4 for "Tier 1 - Europe OR Tier Russian OR Tier Portuguese"
                        # Testing CO and PCOQ:
                        co = custom_logic.sf_get_user_or_group(sf_connection=s_f_connection,
                                                               user_or_group_id=Threat.case_info_tuple[3])[0]
                        if co is None:
                            exc_tuple = sys.exc_info()
                            raise custom_logic.SFGetUserNameError('Testing CO and PCOQ has failed',
                                                                  {'user_or_group_id': Threat.case_info_tuple[3],
                                                                   'exception': exc_tuple[1]})
                        PCOQ = Threat.case_info_tuple[11]
                        MainLogger.info(
                            'CO:' + str(co) + ' with ID: ' + str(Threat.case_info_tuple[3]) + ', PCOQ: ' + str(PCOQ))
                        USE_A1_SHIFT = False
                        if PCOQ is not None:
                            if PCOQ in ['Tier 1 - Europe',
                                        'Tier Russian',
                                        'Tier Portuguese',
                                        'Tier 1 - APAC',
                                        'Tier 1 - North America',
                                        'Tier 1 - South America',
                                        'Tier 1 - US Federal',
                                        'Tier Chinese',
                                        'Tier Dutch',
                                        'Tier Japanese']:
                                USE_A1_SHIFT = True
                        elif co in ['Tier 1 - Europe',
                                    'Tier Russian',
                                    'Tier Portuguese',
                                    'Tier 1 - APAC',
                                    'Tier 1 - North America',
                                    'Tier 1 - South America',
                                    'Tier 1 - US Federal',
                                    'Tier Chinese',
                                    'Tier Dutch',
                                    'Tier Japanese']:
                            USE_A1_SHIFT = True
                        if USE_A1_SHIFT is True:
                            # Testing time:
                            current_time_hour_utc = datetime.utcnow()
                            today_shift1_start = current_time_hour_utc.replace(hour=4, minute=30, second=0,
                                                                               microsecond=0)
                            today_shift1_end = today_shift1_start.replace(hour=8, minute=30)
                            today_shift2_start = today_shift1_start.replace(hour=8, minute=30)
                            today_shift2_end = today_shift1_start.replace(hour=10, minute=30)
                            today_shift3_start = today_shift1_start.replace(hour=10, minute=30)
                            today_shift3_end = today_shift1_start.replace(hour=12, minute=30)
                            today_shift4_start = today_shift1_start.replace(hour=12, minute=30)
                            today_shift4_end = today_shift1_start.replace(hour=16, minute=30)

                            if today_shift1_start <= current_time_hour_utc < today_shift1_end:
                                # 7:30 - 11:30 am GMT+3, 4:30 - 8:30 UTC
                                Threat.target_notification_channel = teams_channels_inst.webhooks_dict['Case shift 1']
                            elif today_shift2_start <= current_time_hour_utc < today_shift2_end:
                                # 11:30am - 1:30pm GMT+3, 8:30 - 10:30 UTC
                                Threat.target_notification_channel = teams_channels_inst.webhooks_dict['Case shift 2']
                            elif today_shift3_start <= current_time_hour_utc < today_shift3_end:
                                # 1:30pm - 3:30pm GMT+3, 10:30 - 12:30 UTC
                                Threat.target_notification_channel = teams_channels_inst.webhooks_dict['Case shift 3']
                            elif today_shift4_start <= current_time_hour_utc < today_shift4_end:
                                # 3:30pm - 7:30pm GMT+3, 12:30 - 16:30 UTC
                                Threat.target_notification_channel = teams_channels_inst.webhooks_dict['Case shift 4']
                            else:
                                MainLogger.info(
                                    'A1_n rule by source is ok, but now is not a right time, notifying a previously selected channel: ' + str(
                                        Threat.target_notification_channel))
                                USE_A1_SHIFT = False
                        else:
                            MainLogger.info(
                                'A1 rule, notifying a previously selected channel: ' + str(
                                    Threat.target_notification_channel))
                        if USE_A1_SHIFT is True:
                            MainLogger.info(
                                'A1_n rule, notifying a new selected Case Shift channel: ' + str(
                                    Threat.target_notification_channel))
                        result = custom_logic.send_notification_to_web_hook(
                            web_hook_url=Threat.target_notification_channel,
                            threat=Threat,
                            max_allowed_sla=Threat.current_SLA)
                    elif Threat.current_SLA <= rule_a2 and Threat.current_SLA > rule_a3:
                        RuleA2_notification_target_channel = teams_channels_inst.webhooks_dict['Tier 1 EMEA > General']
                        result = custom_logic.send_notification_to_web_hook(
                            web_hook_url=RuleA2_notification_target_channel,
                            threat=Threat,
                            max_allowed_sla=Threat.current_SLA)
                    elif Threat.current_SLA <= rule_a3:
                        RuleA3_notification_target_channel = teams_channels_inst.webhooks_dict['Tier 1 EMEA > General']
                        result = custom_logic.send_notification_to_web_hook(
                            web_hook_url=RuleA3_notification_target_channel,
                            threat=Threat,
                            max_allowed_sla=Threat.current_SLA)
                    else:
                        result = False
                    if result is not True:
                        MainLogger.error('Failed to send notification to ' + str(Threat.target_notification_channel))
                    else:
                        result = sql_connector_instance.update_dbo_cases_after_notification_sent(
                            row_id=Threat.case_info_tuple[1])
                        if result is not True:
                            MainLogger.critical('Failed to update DB around row:' + str(Threat.case_info_tuple[1]))
                        elif result is True:
                            MainLogger.info('Threat neutralized and processed')
                else:
                    pass

            except Exception as error:
                MainLogger.error('Some unknown error has occurred: \n' + str(error))
                exit()
        other_time.sleep(Query_Delay)
    else:
        other_time.sleep(Query_Delay)


try:
    while True:
        main_execution()
except simple_salesforce.exceptions.SalesforceExpiredSession:
    sf_config_inst = SFConfig()
    SF_connection = Salesforce(username=sf_config_inst.user, password=sf_config_inst.password,
                               security_token=sf_config_inst.token)
