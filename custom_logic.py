from simple_salesforce import Salesforce
import pyodbc
import configuration
import logging


def find_cases_with_potential_sla(sf_connection: Salesforce, max_allowed_sla: int = 60)->list:
    if max_allowed_sla < 0:
        max_allowed_sla = 60
    case_check_query = "SELECT id, OwnerId, Status, CaseNumber, Previous_Owner__c, CreatedDate, Subject, AccountId, Flag__c, Manager_of_Case_Owner__c from case WHERE Time_to_Respond__c < "+str(max_allowed_sla)+" and Time_to_Respond__c > 0 and status in ('New', 'Open') and FTR_Case_Owner__c = null"
    found_cases = sf_connection.query(query=case_check_query)
    found_cases_list = []
    for row in found_cases['records']:
        case_info = {
            'CaseNumber': row['CaseNumber'],
            'Id': row['Id'],
            'OwnerId': row['OwnerId'],
            'Status': row['Status'],
            'CreatedDate': row['CreatedDate'],
            'Subject': row['Subject'],
            'AccountId': row['AccountId'],
            'Flag__c': row['Flag__c'],
            'Previous_Owner__c': row['Previous_Owner__c'],
            'Manager_of_Case_Owner__c': row['Manager_of_Case_Owner__c']
        }
        found_cases_list.append(case_info)
    return found_cases_list


class SQLConnector:
    def __init__(self, sql_config: configuration.SQLConfig):
        self.connection = pyodbc.connect(
            'DRIVER=' + sql_config.Driver + ';PORT=1433;SERVER=' + sql_config.Server + ';PORT=1443;DATABASE='
            + sql_config.Database + ';UID=' + sql_config.Username + ';PWD=' + sql_config.Password)
        self.cursor = self.connection.cursor()
        self.logging_inst = logging.getLogger()

    def insert_into_dbo_cases(self, case_dict: dict)->bool:
        CaseId = case_dict['Id']
        CaseNumber = case_dict['CaseNumber']
        OwnerId = case_dict['OwnerId']
        Status = case_dict['Status']
        CreatedDate = case_dict['CreatedDate'][:-5]
        Subject = case_dict['Subject']
        AccountId = case_dict['AccountId']
        Flag = str(case_dict['Flag__c'])
        Previous_Owner = case_dict['Previous_Owner__c']
        Manager_of_Case_Owner = case_dict['Manager_of_Case_Owner__c']
        case_tuple = (CaseNumber, OwnerId, CaseId, CreatedDate,  Status, Subject, AccountId, Flag, Previous_Owner, Manager_of_Case_Owner)
        try:
            self.cursor.execute(
                "insert into [SLA_bot].[dbo].[Cases] values(NEWID(),?,?,?,?,GETDATE(), 0, Null, Null,?,?,?,?,?,?)", *case_tuple[0:10])
            self.connection.commit()
            self.logging_inst.info(
                'insertion of case ' + case_dict['CaseNumber'] + ' was completed')
            return True
        except Exception as error:
            self.connection.rollback()
            self.logging_inst.error('insertion of case ' + str(CaseNumber) + ' has failed due to the following error \n'+str(error))
            return False


