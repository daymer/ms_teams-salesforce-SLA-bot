from simple_salesforce import Salesforce
import simple_salesforce
from configuration import SFConfig, SFQueues, SQLConfigELISADB, TeamsChannels, Integration, SQLConfigKARMADB
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
#

USE_TEST_VARS = False
PROCEED_WITH_SLA_RULES = True
if PROCEED_WITH_SLA_RULES is False:
    proceed_with_a1_rule = False
    proceed_with_a2_rule = False
    proceed_with_a3_rule = False
    proceed_with_b1_rule = False
else:
    proceed_with_a1_rule = True
    proceed_with_a2_rule = True
    proceed_with_a3_rule = True
    proceed_with_b1_rule = True

PROCEED_WITH_KARMA_EVENTS_RULES = True
#                                                            #
##############################################################

configuration_inst = Integration()
if USE_TEST_VARS is False:
    MainLogger = logging_config(integration_config=configuration_inst, logging_mode='DEBUG', log_to_file=True,
                                executable_path=__file__)
else:
    MainLogger = logging_config(integration_config=configuration_inst, logging_mode='DEBUG', log_to_file=False,
                                executable_path=__file__)


def initialize(sf_config_ins_func: SFConfig):
    logger_inst = logging.getLogger()
    try:
        sf_connection = Salesforce(username=sf_config_ins_func.user, password=sf_config_ins_func.password,
                                   security_token=sf_config_ins_func.token)
    except simple_salesforce.exceptions.SalesforceAuthenticationFailed as error:
        logger_inst.error('Failed to connect to SalesForce due to the following error:\n' + str(error))
        sf_connection = None
    return sf_connection


sf_config_instance = SFConfig()
sf_queues_instance = SFQueues()
sql_config_instance_elisa_db = SQLConfigELISADB()
sql_config_instance_karma_db = SQLConfigKARMADB()
teams_channels_inst = TeamsChannels(use_test_channels=USE_TEST_VARS)
sql_connector_instance_elisa_db = custom_logic.SQLConnectorELISADB(sql_config_instance_elisa_db, use_test_instance=USE_TEST_VARS)
sql_connector_instance_karma_db = custom_logic.SQLConnectorKARMADB(sql_config_instance_karma_db)
SF_connection = initialize(sf_config_ins_func=sf_config_instance)
MainLogger.info('Main process has been initialized')


##############################################################


def a_rule(rule_start: int, rule_end: int, sf_connection: Salesforce, teams_channels_inst: TeamsChannels):
    #   loads SF Cases by various filters
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
        case_dict['target_notification_channel'] = custom_logic.find_target_teams_channel_for_case_sla(case_dict['OwnerId'],
                                                                                                       case_dict[
                                                                                              'Previous_Owner__c'],
                                                                                                       case_dict['Product__c'], teams_channels_inst)
        result = sql_connector_instance_elisa_db.insert_into_dbo_cases(case_dict=case_dict, rule=str(rule_start))
        if result is not False:
            pass
        else:
            main_logger.error('Some error has occurred, braking execution and notifying an admin')
            if isinstance(main_logger.root.handlers[0], logging.FileHandler):
                main_logger.error('Log name: ' + main_logger.root.handlers[0].baseFilename)
                exit(1)


def a_backlog_rule(sf_connection: Salesforce, teams_channels_inst: TeamsChannels, team: str):
    main_logger = logging.getLogger()
    main_logger.info(
        'A backlog: Checking if there is any backlog for team: ' + str(team))


def a_karma_event_rule(sql_connector_instance_karma_db_func: custom_logic.SQLConnectorKARMADB, event_type: str):
    #   loads Karma events
    main_logger = logging.getLogger()
    main_logger.info('Ax: Searching for new Karma Events by type: ' + event_type)
    found_events_list = sql_connector_instance_karma_db_func.find_karma_events(event_type)
    if len(found_events_list) == 0:
        main_logger.info('Done, no Karma ' + event_type + ' events were found')
    else:
        main_logger.info('Done, found ' + str(len(found_events_list)) + ' event(s)')
    for event_dict in found_events_list:
        event_dict['target_notification_channel'] = custom_logic.find_target_teams_channel_for_karma_event(event_type, teams_channels_inst, event_dict)
        result = sql_connector_instance_elisa_db.insert_into_dbo_karma_events(event_dict=event_dict, event_type=event_type)
        if result is not False:
            pass
        else:
            main_logger.error('Some error has occurred, braking execution and notifying admin')
            if isinstance(main_logger.root.handlers[0], logging.FileHandler):
                main_logger.error('Log name: ' + main_logger.root.handlers[0].baseFilename)
                exit(1)


def main_execution(sql_connector_instance_func, teams_channels_inst_func):
    c_rule_logic_style = None
    if PROCEED_WITH_SLA_RULES is True:
        current_day_of_week = datetime.utcnow().date().weekday()
        if current_day_of_week in (5, 6):  # weekend shifts
            utc_current_date = datetime.utcnow().date()
            utc_11_march = datetime.utcnow().date().replace(month=3, day=11)
            utc_04_november = datetime.utcnow().date().replace(month=11, day=4)
            utc_current_time = datetime.utcnow()
            if utc_04_november <= utc_current_date < utc_11_march:
                MainLogger.debug('Checking time, now is after 04.11 and before 11.03, dls = winter')
                weekend_emea_shift_start = utc_current_time.replace(hour=4, minute=30)
                weekend_emea_shift_end = utc_current_time.replace(hour=16, minute=30)
            elif utc_11_march <= utc_current_date < utc_04_november:
                MainLogger.debug('Checking time, now is after 11.03 and before 04.11, dls = summer')
                weekend_emea_shift_start = utc_current_time.replace(hour=5, minute=00)
                weekend_emea_shift_end = utc_current_time.replace(hour=17, minute=00)
            else:
                #  just in case...
                weekend_emea_shift_start = utc_current_time.replace(hour=5, minute=00)
                weekend_emea_shift_end = utc_current_time.replace(hour=17, minute=00)
            if weekend_emea_shift_start <= utc_current_time < weekend_emea_shift_end:
                c_rule_logic_style = 'Weekend EMEA'
            else:
                c_rule_logic_style = 'Weekend US'
        else:
            utc_current_time_hour = datetime.utcnow().hour
            if 5 <= utc_current_time_hour < 7:  # t0,  APJ + EMEA
                c_rule_logic_style = 'APJ + EMEA'
            elif 7 <= utc_current_time_hour < 12:  # t1,  EMEA
                c_rule_logic_style = 'EMEA'
            elif 12 <= utc_current_time_hour < 17:  # t2,  EMEA + US
                c_rule_logic_style = 'EMEA + US'
            elif 17 <= utc_current_time_hour < 23:  # t3,  US
                c_rule_logic_style = 'US'
            elif 23 <= utc_current_time_hour:  # t4,  US + APJ
                c_rule_logic_style = 'US + APJ'
            elif 0 <= utc_current_time_hour < 5:  # t5,  APJ
                c_rule_logic_style = 'APJ'
            else:
                c_rule_logic_style = None
                MainLogger.critical('c_rule_logic_style cannot be None, what time is it?')
                exit(1)
    sf_config_inst_2 = SFConfig()
    s_f_connection = Salesforce(username=sf_config_inst_2.user, password=sf_config_inst_2.password,
                                security_token=sf_config_inst_2.token)
    # Block A: loading source threats and uploading them to DB
    #   A1: Loading SLA cases from all Tier 1 Queues with potentially broken SLA: RULE 60
    if proceed_with_a1_rule is True:
        rule_a1 = MaxAllowedSLA
        rule_a1_end = 30
        a_rule(rule_a1, rule_a1_end, s_f_connection, teams_channels_inst_func)
    #   A2: Loading SLA cases from all Tier 1 Queues with potentially broken SLA: RULE 30
    if proceed_with_a2_rule is True:
        rule_a2 = 31
        rule_a2_end = 0
        a_rule(rule_a2, rule_a2_end, s_f_connection, teams_channels_inst_func)
    #   A3: Loading SLA cases from all Tier 1 Queues with potentially broken SLA: RULE 10
    if proceed_with_a3_rule is True:
        rule_a3 = 10
        rule_a3_end = 0
        a_rule(rule_a3, rule_a3_end, s_f_connection, teams_channels_inst_func)
    #   Ax_karma_event_rule
    if PROCEED_WITH_KARMA_EVENTS_RULES is True:
        #   WebRequests_delete_page_by_XWD_FULLNAME
        a_karma_event_rule(sql_connector_instance_karma_db, 'delete')
        #   WebRequests_reindex_page_by_XWD_FULLNAME
        a_karma_event_rule(sql_connector_instance_karma_db, 'reindex')
        #   WebRequests_vote_for_page_as_user
        a_karma_event_rule(sql_connector_instance_karma_db, 'vote')

    # Block B: loading threats
    MainLogger.info('Loading threats')
    threats = []
    #   B1: [dbo].[Cases]
    if proceed_with_b1_rule is True:
        MainLogger.info('Loading cases with bad SLA')
        try:
            threats += sql_connector_instance_func.select_all_unanswered_threats_from_cases()
        except custom_logic.NoThreadsFound as error:
            logging.info(error)
    #   B2: [dbo].[Karma_events]
    MainLogger.info('Loading Karma Events')
    try:
        threats += sql_connector_instance_func.select_all_unanswered_threats_from_karma_events()
    except custom_logic.NoThreadsFound as error:
        logging.info(error)

    if len(threats) > 0:
        MainLogger.info('threats loaded, processing')
    else:
        MainLogger.debug('no threats found, skipping')

    # Block C: reacting on threats
    for Threat in threats:
        if not isinstance(Threat, custom_logic.CaseSLA) and not isinstance(Threat, custom_logic.KarmaEvent):
            MainLogger.info('Unsupported threat type, skipping: ' + str(type(Threat)) + ' ' + str(Threat.info_tuple[2]))
            continue
        try:
            if isinstance(Threat, custom_logic.CaseSLA):
                # In order to save the info regarding currently unsupported target_notification_channels,
                # we should resolve CO and pCO:
                if Threat.target_notification_channel == 'undefined':
                    MainLogger.error('Case ' + str(
                        Threat.info_tuple[2]) + ': unsupported target_notification_channel type, skipping')
                    co = Threat.info_tuple[3]
                    p_co = Threat.info_tuple[11]
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
                    sql_connector_instance_func.update_dbo_cases_after_notification_sent(row_id=Threat.info_tuple[1])
                elif Threat.target_notification_channel.startswith('https://outlook.office.com/webhook/'):
                    MainLogger.info('Reacting on threat: case ' + str(Threat.info_tuple[2]))
                    Threat.current_SLA = custom_logic.get_current_case_sla(sf_connection=s_f_connection,
                                                                           case_id=Threat.info_tuple[4])
                    # Step 1: looking for an appropriate A rule:
                    # A rule is already broken
                    if Threat.current_SLA is None:
                        sql_connector_instance_func.update_dbo_cases_after_notification_sent(
                            row_id=Threat.info_tuple[1])
                        MainLogger.debug('Threat.current_SLA:' + str(Threat.current_SLA))
                        MainLogger.info('Threat neutralized, but not processed: it\'s too late')
                        continue
                    # A rule is not violated, looking for a channel
                        # Testing CO (Threat.info_tuple[3]) and PCOQ (Threat.info_tuple[11]):
                    co = custom_logic.sf_get_user_or_group(sf_connection=s_f_connection,
                                                           user_or_group_id=Threat.info_tuple[3])[0]
                    if co is None:
                        exc_tuple = sys.exc_info()
                        raise custom_logic.SFGetUserNameError('Testing of CO and PCOQ has failed',
                                                              {'user_or_group_id': Threat.info_tuple[3],
                                                               'exception': exc_tuple[1]})
                    PCOQ = Threat.info_tuple[11]
                    MainLogger.info(
                        'CO: ' + str(co) + ' with ID: ' + str(Threat.info_tuple[3]) + ', PCOQ: ' + str(PCOQ))

                    MainLogger.info('Looking for an appropriate A rule, current case SLA:' + str(Threat.current_SLA) + ', c_rule_logic_style: ' + str(c_rule_logic_style))
                    Threat.current_SLA = int(Threat.current_SLA)
                    if Threat.current_SLA > rule_a2 and Threat.info_tuple[1]:  # A1
                        if c_rule_logic_style == 'APJ + EMEA':
                            # Adding a special forwarding rule to notify in a frontier case
                            # APJ
                            notify_APJ = False
                            if PCOQ is not None:
                                if PCOQ not in ['Tier 1 - Europe',
                                            'Tier Russian',
                                            'Tier 2 - EM:Europe',
                                            'Tier 3 - EM: EMEA']:
                                    notify_APJ = True
                            elif co not in ['Tier 1 - Europe',
                                        'Tier Russian',
                                        'Tier 2 - EM:Europe',
                                        'Tier 3 - EM: EMEA']:
                                notify_APJ = True
                            # EMEA: NOTE: Threat.target_notification_channel also contains cloud and agent logic
                            notify_EMEA = False
                            if PCOQ is not None:
                                if PCOQ not in ['Tier 1 - APAC',
                                            'Tier Chinese',
                                            'Tier Japanese']:
                                    notify_EMEA = True
                            elif co not in ['Tier 1 - APAC',
                                            'Tier Chinese',
                                            'Tier Japanese']:
                                notify_EMEA = True
                            if notify_APJ is True:
                                MainLogger.debug('Sending notification to: Support.Worldwide / APAC - Cases and Calls')
                                channel_notification_sequence(teams_channels_inst_func.webhooks_dict['Support.Worldwide / APAC - Cases and Calls'],
                                                              sql_connector_instance_func, Threat)
                            if notify_EMEA is True:
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
                                    MainLogger.debug(
                                        'Sending notification to: Case shift 1')
                                    channel_notification_sequence(
                                        teams_channels_inst_func.webhooks_dict['Case shift 1'],
                                        sql_connector_instance_func, Threat)
                                else:
                                    MainLogger.debug(
                                        'Sending notification to: default channel')
                                    channel_notification_sequence(Threat.target_notification_channel,
                                                                  sql_connector_instance_func, Threat)
                        elif c_rule_logic_style == 'EMEA':
                            # Adding a special forwarding rule A1_1-4 for the list mentioned below
                            # Testing CO and PCOQ
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
                                    RuleA1_notification_target_channel = teams_channels_inst_func.webhooks_dict[
                                        'Case shift 1']
                                    MainLogger.debug(
                                        'Sending notification to: Case shift 1')
                                elif today_shift2_start <= current_time_hour_utc < today_shift2_end:
                                    # 11:30am - 1:30pm GMT+3, 8:30 - 10:30 UTC
                                    RuleA1_notification_target_channel = teams_channels_inst_func.webhooks_dict[
                                        'Case shift 2']
                                    MainLogger.debug(
                                        'Sending notification to: Case shift 2')
                                elif today_shift3_start <= current_time_hour_utc < today_shift3_end:
                                    # 1:30pm - 3:30pm GMT+3, 10:30 - 12:30 UTC
                                    RuleA1_notification_target_channel = teams_channels_inst_func.webhooks_dict[
                                        'Case shift 3']
                                    MainLogger.debug(
                                        'Sending notification to: Case shift 3')
                                elif today_shift4_start <= current_time_hour_utc < today_shift4_end:
                                    # 3:30pm - 7:30pm GMT+3, 12:30 - 16:30 UTC
                                    RuleA1_notification_target_channel = teams_channels_inst_func.webhooks_dict[
                                        'Case shift 4']
                                    MainLogger.debug(
                                        'Sending notification to: Case shift 4')
                                else:
                                    MainLogger.info(
                                        'A1_n rule by source is ok, but now is not a right time, notifying a default channel by type: ' + str(
                                            Threat.target_notification_channel))
                                    RuleA1_notification_target_channel = Threat.target_notification_channel
                                channel_notification_sequence(RuleA1_notification_target_channel,
                                                              sql_connector_instance_func, Threat)
                            else:
                                # it means that it's a special or language channel and a default target_notification_channel should be used
                                MainLogger.debug(
                                    'Sending notification to: default channel')
                                channel_notification_sequence(Threat.target_notification_channel,
                                                              sql_connector_instance_func, Threat)
                        elif c_rule_logic_style == 'EMEA + US':
                            # EMEA:
                            notify_EMEA = False
                            if PCOQ is not None:
                                if PCOQ not in ['Tier 1 - North America',
                                                'Tier 1 - South America',
                                                'Tier 1 - US Federal',
                                                'Tier 2 - EM:Americas',
                                                'Tier 3 - EM:Americas']:
                                    notify_EMEA = True
                            elif co not in ['Tier 1 - North America',
                                                'Tier 1 - South America',
                                                'Tier 1 - US Federal',
                                                'Tier 2 - EM:Americas',
                                                'Tier 3 - EM:Americas']:
                                notify_EMEA = True
                            # US
                            notify_US = False
                            if PCOQ is not None:
                                if PCOQ not in ['Tier 1 - Europe',
                                                'Tier Russian',
                                                'Tier 2 - EM:Europe',
                                                'Tier 3 - EM: EMEA']:
                                    notify_US = True
                            elif co not in ['Tier 1 - Europe',
                                            'Tier Russian',
                                            'Tier 2 - EM:Europe',
                                            'Tier 3 - EM: EMEA']:
                                notify_US = True
                            if notify_US is True:
                                MainLogger.debug(
                                    'Sending notification to: Support.Worldwide / NA - Cases')
                                channel_notification_sequence(teams_channels_inst_func.webhooks_dict[
                                                                  'Support.Worldwide / NA - Cases'],
                                                              sql_connector_instance_func, Threat)
                            if notify_EMEA is True:
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
                                        RuleA1_notification_target_channel = teams_channels_inst_func.webhooks_dict[
                                            'Case shift 1']
                                        MainLogger.debug(
                                            'Sending notification to: Case shift 1')
                                    elif today_shift2_start <= current_time_hour_utc < today_shift2_end:
                                        # 11:30am - 1:30pm GMT+3, 8:30 - 10:30 UTC
                                        RuleA1_notification_target_channel = teams_channels_inst_func.webhooks_dict[
                                            'Case shift 2']
                                        MainLogger.debug(
                                            'Sending notification to: Case shift 2')
                                    elif today_shift3_start <= current_time_hour_utc < today_shift3_end:
                                        # 1:30pm - 3:30pm GMT+3, 10:30 - 12:30 UTC
                                        RuleA1_notification_target_channel = teams_channels_inst_func.webhooks_dict[
                                            'Case shift 3']
                                        MainLogger.debug(
                                            'Sending notification to: Case shift 3')
                                    elif today_shift4_start <= current_time_hour_utc < today_shift4_end:
                                        # 3:30pm - 7:30pm GMT+3, 12:30 - 16:30 UTC
                                        RuleA1_notification_target_channel = teams_channels_inst_func.webhooks_dict[
                                            'Case shift 4']
                                        MainLogger.debug(
                                            'Sending notification to: Case shift 4')
                                    else:
                                        MainLogger.info(
                                            'A1_n rule by source is ok, but now is not a right time, notifying a default channel by type: ' + str(
                                                Threat.target_notification_channel))
                                        RuleA1_notification_target_channel = Threat.target_notification_channel
                                    channel_notification_sequence(RuleA1_notification_target_channel,
                                                                  sql_connector_instance_func, Threat)
                                else:
                                    # it means that it's a special or language channel and a default target_notification_channel should be used
                                    MainLogger.debug(
                                        'Sending notification to: default channel')
                                    channel_notification_sequence(Threat.target_notification_channel,
                                                                  sql_connector_instance_func, Threat)
                        elif c_rule_logic_style == 'US':
                            MainLogger.debug(
                                'Sending notification to: Support.Worldwide / NA - Cases')
                            channel_notification_sequence(teams_channels_inst_func.webhooks_dict['Support.Worldwide / NA - Cases'],
                                                          sql_connector_instance_func, Threat)
                        elif c_rule_logic_style == 'US + APJ':
                            # Adding a special forwarding rule to notify in a frontier case
                            notify_US = False
                            if PCOQ is not None:
                                if PCOQ not in ['Tier 1 - APAC',
                                                'Tier Chinese',
                                                'Tier Japanese']:
                                    notify_US = True
                            elif co not in ['Tier 1 - APAC',
                                                'Tier Chinese',
                                                'Tier Japanese']:
                                notify_US = True
                            notify_APJ = False
                            if PCOQ is not None:
                                if PCOQ not in ['Tier 1 - North America',
                                                'Tier 1 - South America',
                                                'Tier 1 - US Federal',
                                                'Tier 2 - EM:Americas',
                                                'Tier 3 - EM:Americas']:
                                    notify_APJ = True
                            elif co not in ['Tier 1 - North America',
                                                'Tier 1 - South America',
                                                'Tier 1 - US Federal',
                                                'Tier 2 - EM:Americas',
                                                'Tier 3 - EM:Americas']:
                                notify_APJ = True
                            if notify_US is True:
                                MainLogger.debug(
                                    'Sending notification to: Support.Worldwide / NA - Cases')
                                channel_notification_sequence(teams_channels_inst_func.webhooks_dict[
                                                                  'Support.Worldwide / NA - Cases'],
                                                              sql_connector_instance_func, Threat)
                            if notify_APJ is True:
                                MainLogger.debug(
                                    'Sending notification to: Support.Worldwide / APAC - Cases and Calls')
                                channel_notification_sequence(teams_channels_inst_func.webhooks_dict[
                                                                  'Support.Worldwide / APAC - Cases and Calls'],
                                                              sql_connector_instance_func, Threat)
                        elif c_rule_logic_style == 'APJ':
                            MainLogger.debug(
                                'Sending notification to: Support.Worldwide / APAC - Cases and Calls')
                            channel_notification_sequence(teams_channels_inst_func.webhooks_dict['Support.Worldwide / APAC - Cases and Calls'],
                                                          sql_connector_instance_func, Threat)
                        elif c_rule_logic_style == 'Weekend EMEA':
                            MainLogger.debug(
                                'Sending notification to: Tier1 EMEA / Weekend channel')
                            channel_notification_sequence(
                                teams_channels_inst_func.webhooks_dict['Tier1 EMEA / Weekend channel'],
                                sql_connector_instance_func, Threat)
                        elif c_rule_logic_style == 'Weekend US':
                            MainLogger.debug(
                                'Sending notification to: Support.Worldwide / NA - Cases')
                            channel_notification_sequence(
                                teams_channels_inst_func.webhooks_dict['Support.Worldwide / NA - Cases'],
                                sql_connector_instance_func, Threat)
                        else:
                            MainLogger.critial(
                                'Failed to locate an appropriate channel to notify about A1 rule event, using "Test channel"')
                            RuleA1_notification_target_channel = teams_channels_inst_func.webhooks_dict['Test channel']
                            channel_notification_sequence(RuleA1_notification_target_channel,
                                                          sql_connector_instance_func, Threat)
                    elif rule_a2 >= Threat.current_SLA > rule_a3:  # A2
                        if c_rule_logic_style == 'APJ + EMEA':
                            # Adding a special forwarding rule to notify in a frontier case
                            # APJ
                            notify_APJ = False
                            if PCOQ is not None:
                                if PCOQ not in ['Tier 1 - Europe',
                                                'Tier Russian',
                                                'Tier 2 - EM:Europe',
                                                'Tier 3 - EM: EMEA']:
                                    notify_APJ = True
                            elif co not in ['Tier 1 - Europe',
                                            'Tier Russian',
                                            'Tier 2 - EM:Europe',
                                            'Tier 3 - EM: EMEA']:
                                notify_APJ = True
                            # EMEA: NOTE: Threat.target_notification_channel also contains cloud and agent logic
                            notify_EMEA = False
                            if PCOQ is not None:
                                if PCOQ not in ['Tier 1 - APAC',
                                                'Tier Chinese',
                                                'Tier Portuguese',
                                                'Tier Japanese']:
                                    notify_EMEA = True
                            elif co not in ['Tier 1 - APAC',
                                            'Tier Chinese',
                                            'Tier Portuguese',
                                            'Tier Japanese']:
                                notify_EMEA = True
                            if notify_APJ is True:
                                MainLogger.debug(
                                    'Sending notification to: Support.Worldwide / APAC - Cases and Calls')
                                channel_notification_sequence(teams_channels_inst_func.webhooks_dict[
                                                                  'Support.Worldwide / APAC - Cases and Calls'],
                                                              sql_connector_instance_func, Threat)
                            if notify_EMEA is True:
                                MainLogger.debug(
                                    'Sending notification to: Tier 1 EMEA / Administrative')
                                channel_notification_sequence(teams_channels_inst_func.webhooks_dict[
                                                                  'Tier 1 EMEA / Administrative'],
                                                              sql_connector_instance_func, Threat)
                        elif c_rule_logic_style == 'EMEA':
                            MainLogger.debug(
                                'Sending notification to: Tier 1 EMEA / Administrative')
                            channel_notification_sequence(teams_channels_inst_func.webhooks_dict[
                                                              'Tier 1 EMEA / Administrative'],
                                                          sql_connector_instance_func, Threat)
                        elif c_rule_logic_style == 'EMEA + US':
                            # US
                            notify_US = False
                            if PCOQ is not None:
                                if PCOQ not in ['Tier 1 - Europe',
                                                'Tier Russian',
                                                'Tier 2 - EM:Europe',
                                                'Tier 3 - EM: EMEA']:
                                    notify_US = True
                            elif co not in ['Tier 1 - Europe',
                                            'Tier Russian',
                                            'Tier 2 - EM:Europe',
                                            'Tier 3 - EM: EMEA']:
                                notify_US = True
                            # EMEA:
                            notify_EMEA = False
                            if PCOQ is not None:
                                if PCOQ not in ['Tier 1 - APAC',
                                                'Tier Chinese',
                                                'Tier Japanese']:
                                    notify_EMEA = True
                            elif co not in ['Tier 1 - APAC',
                                            'Tier Chinese',
                                            'Tier Portuguese',
                                            'Tier Japanese']:
                                notify_EMEA = True
                            if notify_US is True:
                                MainLogger.debug(
                                    'Sending notification to: Support.Worldwide / NA - Cases')
                                channel_notification_sequence(teams_channels_inst_func.webhooks_dict[
                                                                  'Support.Worldwide / NA - Cases'],
                                                              sql_connector_instance_func, Threat)
                            if notify_EMEA is True:
                                MainLogger.debug(
                                    'Sending notification to: Tier 1 EMEA / Administrative')
                                channel_notification_sequence(teams_channels_inst_func.webhooks_dict[
                                                                  'Tier 1 EMEA / Administrative'],
                                                              sql_connector_instance_func, Threat)
                        elif c_rule_logic_style == 'US':
                            MainLogger.debug(
                                'Sending notification to: Support.Worldwide / NA - Cases')
                            channel_notification_sequence(teams_channels_inst_func.webhooks_dict[
                                                              'Support.Worldwide / NA - Cases'],
                                                          sql_connector_instance_func, Threat)
                        elif c_rule_logic_style == 'US + APJ':
                            notify_US = False
                            if PCOQ is not None:
                                if PCOQ not in ['Tier 1 - APAC',
                                                'Tier Chinese',
                                                'Tier Japanese']:
                                    notify_US = True
                            elif co not in ['Tier 1 - APAC',
                                            'Tier Chinese',
                                            'Tier Japanese']:
                                notify_US = True
                            notify_APJ = False
                            if PCOQ is not None:
                                if PCOQ not in ['Tier 1 - North America',
                                                'Tier 1 - South America',
                                                'Tier 1 - US Federal',
                                                'Tier 2 - EM:Americas',
                                                'Tier 3 - EM:Americas']:
                                    notify_APJ = True
                            elif co not in ['Tier 1 - North America',
                                            'Tier 1 - South America',
                                            'Tier 1 - US Federal',
                                            'Tier 2 - EM:Americas',
                                            'Tier 3 - EM:Americas']:
                                notify_APJ = True
                            if notify_US is True:
                                MainLogger.debug(
                                    'Sending notification to: Support.Worldwide / NA - Cases')
                                channel_notification_sequence(teams_channels_inst_func.webhooks_dict[
                                                                  'Support.Worldwide / NA - Cases'],
                                                              sql_connector_instance_func, Threat)
                            if notify_APJ is True:
                                MainLogger.debug(
                                    'Sending notification to: Support.Worldwide / APAC - Cases and Calls')
                                channel_notification_sequence(teams_channels_inst_func.webhooks_dict[
                                                                  'Support.Worldwide / APAC - Cases and Calls'],
                                                              sql_connector_instance_func, Threat)
                        elif c_rule_logic_style == 'APJ':
                            channel_notification_sequence(teams_channels_inst_func.webhooks_dict[
                                                              'Support.Worldwide / APAC - Cases and Calls'],
                                                          sql_connector_instance_func, Threat)
                        elif c_rule_logic_style == 'Weekend EMEA' or c_rule_logic_style == 'Weekend US':
                            MainLogger.debug(
                                'Sending notification to: Support.Worldwide / Weekend Cases')
                            channel_notification_sequence(
                                teams_channels_inst_func.webhooks_dict['Support.Worldwide / Weekend Cases'],
                                sql_connector_instance_func, Threat)
                        else:
                            MainLogger.critial(
                                'Failed to locate an appropriate channel to notify about A2 rule event, using "Test channel"')
                            channel_notification_sequence(teams_channels_inst_func.webhooks_dict[
                                                              'Test channel'],
                                                          sql_connector_instance_func, Threat)
                    elif Threat.current_SLA <= rule_a3:
                        MainLogger.debug(
                            'Sending notification to: Management.Worldwide / General')
                        channel_notification_sequence(teams_channels_inst_func.webhooks_dict['Management.Worldwide / General'], sql_connector_instance_func, Threat)
                    else:
                        MainLogger.critial(
                            'Cannot apply any A rule for the case, skipping')
                        continue
                else:
                    pass
            if isinstance(Threat, custom_logic.KarmaEvent):
                # was this page already proceed during last hour with the same reason?
                if Threat.event_type == 'reindex':
                    existence = sql_connector_instance_elisa_db.select_existence_id_from_karma_events(xwd_fullname=Threat.info_tuple[5], event_type=Threat.info_tuple[2], created_date=Threat.info_tuple[3])
                else:
                    existence = False
                if existence is False:
                    # checking if it's a VCC bug and we need to notify 2 channels at once:
                    if Threat.info_tuple[8] is True:  # notify only on full
                        if str(Threat.info_tuple[5]).lower().startswith('main.bugs and fixes.found bugs.vbr.bug'):
                            MainLogger.debug('It\'s a VCC bug and we need to notify 2 channels at once')
                            karma_page_id, chars_total = sql_connector_instance_karma_db.select_id_characters_total_from_dbo_knownpages(platform='xwiki', page_id=Threat.info_tuple[5])
                            bug_components_array = sql_connector_instance_karma_db.select_bug_components_from_dbo_knownbugs(page_id=karma_page_id)
                            if 'CloudConnect' in bug_components_array:
                                MainLogger.debug('It\'s a VCC bug, notifying an extra channel')
                                result = custom_logic.send_notification_to_web_hook(
                                    web_hook_url=teams_channels_inst_func.webhooks_dict['WWW VCC'],
                                    threat=Threat)
                                if result is not True:
                                    MainLogger.error(
                                        'Failed to send notification to ' + str(Threat.target_notification_channel))
                    result = custom_logic.send_notification_to_web_hook(
                        web_hook_url=Threat.target_notification_channel,
                        threat=Threat)
                    if result is not True:
                        MainLogger.error('Failed to send notification to ' + str(Threat.target_notification_channel))
                    else:
                        result = sql_connector_instance_func.update_dbo_karma_events_after_notification_sent(
                            row_id=Threat.info_tuple[1])
                        if result is not True:
                            MainLogger.critical('Failed to update DB around row:' + str(Threat.info_tuple[1]))
                        elif result is True:
                            MainLogger.info('Threat ' + str(Threat.info_tuple[2]) + ' of ' + str(Threat.info_tuple[5]) + ' was neutralized and processed')
                else:
                    # The same event was already fired, no need to repeat
                    result = sql_connector_instance_func.update_dbo_karma_events_after_notification_sent(
                        row_id=Threat.info_tuple[1])
                    if result is not True:
                        MainLogger.critical('Failed to update DB around row:' + str(Threat.info_tuple[1]))
                    elif result is True:
                        MainLogger.info('Threat wasn\'t processed since the same event was already fired during last hour, no need to repeat')
        except Exception as error:
            MainLogger.error('Some unknown error has occurred: \n' + str(error))
            exit()
    other_time.sleep(Query_Delay)


def channel_notification_sequence(target_notification_channel, sql_connector_instance_func, threat):
    logger_inst = logging.getLogger()
    result = custom_logic.send_notification_to_web_hook(
        web_hook_url=target_notification_channel,
        threat=threat)
    if result is not True:
        logger_inst.error(
            'Failed to send notification to ' + str(target_notification_channel))
        return False
    else:
        result = sql_connector_instance_func.update_dbo_cases_after_notification_sent(
            row_id=threat.info_tuple[1])
        if result is not True:
            logger_inst.critical('Failed to update DB around row:' + str(threat.info_tuple[1]))
            return False
        elif result is True:
            logger_inst.info('Threat neutralized and processed, '+str(target_notification_channel) + ' was notified')
            return True


try:
    while True:
        main_execution(sql_connector_instance_func=sql_connector_instance_elisa_db, teams_channels_inst_func=teams_channels_inst)

except simple_salesforce.exceptions.SalesforceExpiredSession:
    sf_config_inst = SFConfig()
    SF_connection = Salesforce(username=sf_config_inst.user, password=sf_config_inst.password,
                               security_token=sf_config_inst.token)


