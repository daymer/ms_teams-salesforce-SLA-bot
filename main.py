from simple_salesforce import Salesforce
import simple_salesforce
from configuration import SFConfig, SFQueues
import logging
from datetime import datetime, timedelta


def initialize(sf_config_instance: SFConfig, log_level: str = 'INFO'):
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger_inst = logging.getLogger()
    logger_inst.setLevel(log_level)
    log_name = "logs\main_" + str(datetime.now().strftime("%Y-%m-%d_%H_%M_%S")) + '_v0.1.log'
    #fh = logging.FileHandler(log_name)
    #fh.setLevel(log_level)
    #fh.setFormatter(formatter)
    #logger_inst.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(formatter)
    try:
        sf_connection = Salesforce(username=sf_config_instance.user, password=sf_config_instance.password, security_token=sf_config_instance.token)
    except simple_salesforce.exceptions.SalesforceAuthenticationFailed as error:
        logging.error('Failed to connect to SalesForce due to the following error:\n' + str(error))
        sf_connection = None
    return logger_inst, sf_connection

sf_config_instance = SFConfig()
sf_queues_instance = SFQueues()
MainLogger, SF_connection = initialize(sf_config_instance=sf_config_instance, log_level='INFO')

queue_selector_string = ''
for queue_id in sf_queues_instance.queue_dict.values():
    queue_selector_string += "'" + queue_id + "',"
queue_selector_string = queue_selector_string[:-1]

case_check_query = "SELECT id, OwnerId, Status, CaseNumber, Previous_Owner__c, CreatedDate, Subject, AccountId, Flag__c, Manager_of_Case_Owner__c from case WHERE Time_to_Respond__c < 100 and Time_to_Respond__c > 0 and status in ('New', 'Open') and FTR_Case_Owner__c = null"
found_cases = SF_connection.query(query=case_check_query)

for row in found_cases['records']:
    case_info = {
        'CaseNumber': row['CaseNumber'],
        'id': row['Id'],
        'OwnerId': row['OwnerId'],
        'Status': row['Status'],
        'CreatedDate': row['CreatedDate'],
        'Subject': row['Subject'],
        'AccountId': row['AccountId'],
        'Flag__c': row['Flag__c'],
        'Previous_Owner__c': row['Previous_Owner__c'],
        'Manager_of_Case_Owner__c': row['Manager_of_Case_Owner__c']
    }
    print(case_info)