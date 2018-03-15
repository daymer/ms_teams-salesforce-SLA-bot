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


target_date = datetime.utcnow() - timedelta(hours=1)
target_date_str = target_date.strftime("%Y-%m-%dT%H:%M:%S.000+0000")

queue_selector_string = ''
for queue_id in sf_queues_instance.queue_dict.values():
    queue_selector_string += "'" + queue_id + "',"
queue_selector_string = queue_selector_string[:-1]

case_check_query = "SELECT id, caseid, targetdate FROM casemilestone where IsCompleted != True and "\
                   "caseid in "\
                        "(SELECT id from case WHERE Time_to_Respond__c < 60 and status != 'Junk' and OwnerId in (" + queue_selector_string + "))"
cases_in_check = SF_connection.query(query=case_check_query)

for row in cases_in_check['records']:
    casemilestone_id = row['Id']
    case_id = row['CaseId']
    target_date = row['TargetDate']
    try:
        case = SF_connection.Case.get(case_id)
        case_info = {
            'OwnerId': case['OwnerId'],
            'Manager_of_Case_Owner__c': case['Manager_of_Case_Owner__c'],
            'CreatedDate': case['CreatedDate'],
            'AccountId': case['AccountId'],
            'CaseNumber': case['CaseNumber']
        }
        print(case_info['CaseNumber'], case_info['OwnerId'])
        owner_name = None
        queue_name = None
        try:
            queue_name = [key for key, value in sf_queues_instance.queue_dict.iteritems() if value == case_info['OwnerId']][0]
        except:
            try:
                owner_name = SF_connection.USER.get(case['OwnerId'])
            except:
                queue_name = SF_connection.Group.get(case['OwnerId'])
        if queue_name is None:
            print(owner_name)
        else:
            print(queue_name)
    except Exception as error:
        print(error)


#case = SF_connection.Case.get('5550e000003FTPqAAO')
#print(case)
#case = SF_connection.Case.get_by_custom_id('CaseNumber', '02679161')
#print(case)


#SLA = SF_connection.casemilestone.get('55532000000N3DtAAK')
#print(SLA)

