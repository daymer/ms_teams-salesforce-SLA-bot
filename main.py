from simple_salesforce import Salesforce
import simple_salesforce
from configuration import SFConfig, SFQueues
import logging
from datetime import datetime
import custom_logic


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
    try:
        sf_connection = Salesforce(username=sf_config_inst.user, password=sf_config_inst.password, security_token=sf_config_inst.token)
    except simple_salesforce.exceptions.SalesforceAuthenticationFailed as error:
        logger_inst.error('Failed to connect to SalesForce due to the following error:\n' + str(error))
        sf_connection = None
    return logger_inst, sf_connection

sf_config_instance = SFConfig()
sf_queues_instance = SFQueues()
MainLogger, SF_connection = initialize(sf_config_inst=sf_config_instance, log_level='INFO', log_to_file=False)
MainLogger.info('Main process has been initialized')
MainLogger.info('Searching for new potential SLA violations...')
found_cases_list = custom_logic.find_cases_with_potential_sla(SF_connection)
MainLogger.info('Done, found ' + str(len(found_cases_list)) + ' case(s)')
#print(found_cases_list)
