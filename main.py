from simple_salesforce import Salesforce
import simple_salesforce
from configuration import SFConfig, SFQueues, SQLConfig
import logging
from datetime import datetime
import custom_logic

##############################################################
#                        variables                           #
MaxAllowedSLA = 600
#                                                            #
##############################################################


def initialize(sf_config_inst: SFConfig, log_level: str = 'INFO', log_to_file: bool = False):
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger_inst = logging.getLogger()
    logger_inst.setLevel(log_level)
    if log_to_file is True:
        log_name = "logs\main_" + str(datetime.now().strftime("%Y-%m-%d_%H_%M_%S")) + '_v0.1.log'
        fh = logging.FileHandler(log_name)
        fh.setLevel(log_level)
        fh.setFormatter(formatter)
        logger_inst.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(formatter)
    logger_inst.addHandler(ch)
    try:
        sf_connection = Salesforce(username=sf_config_inst.user, password=sf_config_inst.password, security_token=sf_config_inst.token)
    except simple_salesforce.exceptions.SalesforceAuthenticationFailed as error:
        logger_inst.error('Failed to connect to SalesForce due to the following error:\n' + str(error))
        sf_connection = None
    return logger_inst, sf_connection

sf_config_instance = SFConfig()
sf_queues_instance = SFQueues()
sql_config_instance = SQLConfig()
sql_connector_instance = custom_logic.SQLConnector(sql_config_instance)
MainLogger, SF_connection = initialize(sf_config_inst=sf_config_instance, log_level='INFO', log_to_file=False)
MainLogger.info('Main process has been initialized')
##############################################################


# Block A: loading source threats and uploading them to DB
#   A1: Loading SLA cases from all Tier 1 Queues with potentially broken SLA
MainLogger.info('Searching for new potential SLA violations...')
found_cases_list = custom_logic.find_cases_with_potential_sla(sf_connection=SF_connection, max_allowed_sla=MaxAllowedSLA)

if len(found_cases_list) == 0:
    MainLogger.info('Done, no threats were found')
else:
    MainLogger.info('Done, found ' + str(len(found_cases_list)) + ' case(s)')

for case_dict in found_cases_list:
    case_dict['target_notification_channel'] = custom_logic.find_target_teams_channel(case_dict['OwnerId'], case_dict['Previous_Owner__c'])
    result = sql_connector_instance.insert_into_dbo_cases(case_dict=case_dict)
    if result is not False:
        pass
    else:
        MainLogger.error('Some error has occurred, braking execution and notifying an admin')
        if isinstance(MainLogger.root.handlers[0], logging.FileHandler):
            MainLogger.error('Log name: ' + MainLogger.root.handlers[0].baseFilename)
            exit(1)

# Block B: loading threats
MainLogger.info('Loading threats')

Threats = []
#   B1:
MainLogger.info('Loading cases with bad SLA')
try:
    Threats += sql_connector_instance.select_all_unanswered_threats_from_cases()
except custom_logic.NoThreadsFound as error:
    logging.info(error)

if len(Threats) > 0:
    MainLogger.info('Threats loaded, processing')
else:
    MainLogger.debug('no threats found, skipping')
# Block C: reacting on threats

for Threat in Threats:
    if not isinstance(Threat, custom_logic.CaseSLA):
        MainLogger.debug('Unsupported threat type, skipping')
        continue
    try:
        # In order to save the info regarding currently unsupported target_notification_channels,
        # we should resolve CO and pCO:
        if Threat.target_notification_channel == 'undefined':
            MainLogger.error('Unsupported target_notification_channel type, skipping')
            CO = Threat.case_info_tuple[3]
            pCO = Threat.case_info_tuple[11]
            # both could be a user or a group
            try:
                MainLogger.debug('Trying to locate CO')
                CO = custom_logic.sf_get_user_or_group(sf_connection=SF_connection, user_or_group_id=CO)[0]
            except custom_logic.SFGetUserNameError:
                MainLogger.debug('Failed to locate CO, skipping')
                pass
            try:
                MainLogger.debug('Trying to locate pCO')
                pCO = custom_logic.sf_get_user_or_group(sf_connection=SF_connection, user_or_group_id=pCO)[0]
            except custom_logic.SFGetUserNameError:
                MainLogger.debug('Failed to locate pCO, skipping')
                pass
            MainLogger.info('Unsupported target notification channel, CO of the case is:' + str(CO) + ' and pCO is:' + str(pCO))
            sql_connector_instance.update_dbo_cases_after_notification_sent(row_id=Threat.case_info_tuple[1])
        elif Threat.target_notification_channel.startswith('https://outlook.office.com/webhook/'):
            MainLogger.info('Reacting on threat: case '+str(Threat.case_info_tuple[2]))
            result = custom_logic.send_notification_to_web_hook(web_hook_url=Threat.target_notification_channel, threat=Threat, max_allowed_sla=MaxAllowedSLA)
            if result is not True:
                MainLogger.error('Failed to send notification to ' + str(Threat.target_notification_channel))
            else:
                result = sql_connector_instance.update_dbo_cases_after_notification_sent(row_id=Threat.case_info_tuple[1])
                if result is not True:
                    MainLogger.critical('Failed to update DB around row:' + str(Threat.case_info_tuple[1]))
                elif result is True:
                    MainLogger.info('Threat neutralized and processed')
                    exit()
        else:
            pass

    except Exception as error:
        MainLogger.error('Some unknown error has occurred: \n' + str(error))
        exit()

