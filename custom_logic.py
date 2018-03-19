from simple_salesforce import Salesforce
import pyodbc
import configuration
import logging
import re
from collections import namedtuple
from abc import ABCMeta



class Threat(object):
    __metaclass__ = ABCMeta

    def __init__(self, target_notification_channel:str):
        self.completed = False
        self.respond_from_target = False
        self.notification_date = False
        self.target_notification_channel = target_notification_channel


class CaseSLA(Threat):
    def __init__(self, target_notification_channel: str, case_info: tuple):
        Threat.__init__(self, target_notification_channel)
        self.case_info_tuple = case_info


def find_target_teams_channel(current_case_owner_id: str, previous_case_owner_id: str)-> str:
    target_teams_channel = 'undefined'
    logger_inst = logging.getLogger()
    sf_queues_inst = configuration.SFQueues()
    queue_dict = sf_queues_inst.queue_dict
    teams_channels_inst = configuration.TeamsChannels()
    try:
        supported_source_pretty_name = [key for key in queue_dict.keys() if (queue_dict[key] == current_case_owner_id)]
        supported_source_pretty_name = supported_source_pretty_name[0]
    except IndexError:
        try:
            supported_source_pretty_name = [key for key in queue_dict.keys() if (queue_dict[key] == previous_case_owner_id)]
            supported_source_pretty_name = supported_source_pretty_name[0]
        except IndexError:
            supported_source_pretty_name = None
            logger_inst.error('Cannot find a target channel to notify about current_case_owner_id:' + str(current_case_owner_id) + ' and previous_case_owner_id:' + str(previous_case_owner_id))
    if supported_source_pretty_name is not None:
        try:
            target_teams_channel = teams_channels_inst.webhooks_dict[supported_source_pretty_name]
        except KeyError:
            logger_inst.error('Cannot find a target channel to notify about ' + str(supported_source_pretty_name))
        except Exception:
            logger_inst.error('Cannot find a target channel to notify about ' + str(supported_source_pretty_name))

    return target_teams_channel


def find_cases_with_potential_sla(sf_connection: Salesforce, max_allowed_sla: int = 60) -> list:
    if max_allowed_sla < 0:
        max_allowed_sla = 60
    case_check_query = "SELECT id, " \
                       "OwnerId, " \
                       "Status, " \
                       "CaseNumber, " \
                       "Previous_Owner__c, " \
                       "CreatedDate, " \
                       "Subject, " \
                       "AccountId, " \
                       "Flag__c, " \
                       "Manager_of_Case_Owner__c from case " \
                            "WHERE Time_to_Respond__c <= " + str(max_allowed_sla) + " and " \
                                  "Time_to_Respond__c > 0 and " \
                                  "status in ('New', 'Open') and " \
                                  "FTR_Case_Owner__c = null"
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

    def insert_into_dbo_cases(self, case_dict: dict) -> bool:
        CaseId = case_dict['Id']
        CaseNumber = case_dict['CaseNumber']
        OwnerId = case_dict['OwnerId']
        Status = case_dict['Status']
        CreatedDate = case_dict['CreatedDate'][:-5]
        Subject = case_dict['Subject']
        AccountId = case_dict['AccountId']
        Flag = str(case_dict['Flag__c'])
        matches = re.search(r" alt=\"(.*)\" ", Flag, re.IGNORECASE)
        if matches:
            Flag = matches.group(1)
        Previous_Owner = case_dict['Previous_Owner__c']
        Manager_of_Case_Owner = case_dict['Manager_of_Case_Owner__c']
        target_notification_channel = case_dict['target_notification_channel']
        case_tuple = (CaseNumber, OwnerId, CaseId, CreatedDate, target_notification_channel, Status, Subject, AccountId, Flag, Previous_Owner,
                      Manager_of_Case_Owner)
        try:
            self.cursor.execute(
                "insert into [SLA_bot].[dbo].[Cases] values(NEWID(),?,?,?,?,GETDATE(), 0, Null, ?,?,?,?,?,?,?)",
                *case_tuple[0:11])
            self.connection.commit()
            self.logging_inst.info(
                '----insertion of case ' + case_dict['CaseNumber'] + ' was completed')
            return True
        except pyodbc.IntegrityError:
            self.connection.rollback()
            self.logging_inst.info(
                '----Case ' + str(CaseNumber) + ' is already added, skipping')
            return True
        except Exception as error:
            self.connection.rollback()
            self.logging_inst.error(
                '----insertion of case ' + str(CaseNumber) + ' has failed due to the following error \n' + str(error))
            self.logging_inst.error('-------------Query arguments:\n' + str(case_dict) + '\nParsed flag: ' + str(Flag))
            return False

    def select_all_unanswered_threats_from_cases(self) -> list:
        query = "SELECT [TargetNotificationChannel]" \
                ",[ID]"\
                ",[CaseNumber]" \
                ",[OwnerId]" \
                ",[CaseID]" \
                ",[CaseCreatedDate]" \
                ",[CreatedDate]" \
                ",[Status]" \
                ",[Subject]" \
                ",[AccountId]" \
                ",[Flag]" \
                ",[PreviousOwner]" \
                ",[ManagerCaseOwner] FROM [dbo].[Cases]" \
                    "where [NotificationSent]=0"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        if rows:
            answer = []
            for row in rows:
                verified_row = CaseSLA(target_notification_channel=list(row).pop(0), case_info=tuple(row))
                answer.append(verified_row)
            return answer
        else:
            raise NoThreadsFound('No unanswered threads were found', {'source': '[dbo].[Cases]'})


class BotExpectedError(Exception):
    def __init__(self, message, arguments):
        """Base class for other DatabaseError exceptions"""
        Exception.__init__(self, message + ": {0}".format(arguments))
        self.ErrorMessage = message
        self.ErrorArguments = arguments
        pass


class NoThreadsFound(BotExpectedError):
    """Raised when no threads were found"""
    pass
