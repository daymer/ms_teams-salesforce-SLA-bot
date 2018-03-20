from simple_salesforce import Salesforce, exceptions
import pyodbc
import configuration
import logging
import re
from collections import namedtuple
from abc import ABCMeta
import sys
import pymsteams


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


def sf_get_user_name(sf_connection: Salesforce, user_id: str)->tuple:
    try:
        user_name = sf_connection.USER.get(user_id)
        answer = (user_name['Username'],)
        return answer
    except exceptions.SalesforceResourceNotFound:
        exc_tuple = sys.exc_info()
        raise SFGetUserNameError('SalesforceResourceNotFound', {'user_id': user_id, 'exception': exc_tuple[1]})
    except Exception:
        exc_tuple = sys.exc_info()
        raise SFGetUserNameError('OtherException', {'user_id': user_id, 'exception': exc_tuple[1]})


def sf_get_group_name(sf_connection: Salesforce, group_id: str)->tuple:
    try:
        group = sf_connection.GROUP.get(group_id)
        answer = (group['Name'],)
        return answer
    except exceptions.SalesforceResourceNotFound:
        exc_tuple = sys.exc_info()
        raise SFGetUserNameError('SalesforceResourceNotFound', {'group_id': group_id, 'exception': exc_tuple[1]})
    except Exception:
        exc_tuple = sys.exc_info()
        raise SFGetUserNameError('OtherException', {'group_id': group_id, 'exception': exc_tuple[1]})


def sf_get_user_or_group(sf_connection: Salesforce, user_or_group_id: str)->tuple:
    try:
        user_name = sf_get_user_name(sf_connection=sf_connection, user_id=user_or_group_id)
        return user_name
    except exceptions.SalesforceResourceNotFound:
        try:
            group_name = sf_get_group_name(sf_connection=sf_connection, group_id=user_or_group_id)
            return group_name
        except exceptions.SalesforceResourceNotFound:
            answer = (None,)
            return answer


def find_target_teams_channel(current_case_owner_id: str, previous_case_owner_id: str)-> str:
    target_teams_channel = 'undefined'
    supported_source_pretty_name = None
    logger_inst = logging.getLogger()
    sf_queues_inst = configuration.SFQueues()
    queue_dict = sf_queues_inst.queue_dict
    teams_channels_inst = configuration.TeamsChannels()
    try:
        supported_source_pretty_name = [key for key in queue_dict.keys() if (queue_dict[key] == current_case_owner_id)]
        supported_source_pretty_name = supported_source_pretty_name[0]
    except IndexError:
        try:
            supported_source_pretty_name = previous_case_owner_id
            pass
        except KeyError:
            supported_source_pretty_name = None
            logger_inst.debug('Cannot find a target channel to notify about current_case_owner_id:' + str(current_case_owner_id) + ' and Previous_Owner_Queue__c:' + str(previous_case_owner_id))
    if supported_source_pretty_name is not None:
        try:
            target_teams_channel = teams_channels_inst.webhooks_dict[supported_source_pretty_name]
        except KeyError:
            logger_inst.debug('Cannot find a target channel to notify about ' + str(supported_source_pretty_name))
        except Exception:
            logger_inst.debug('Cannot find a target channel to notify about ' + str(supported_source_pretty_name))

    return target_teams_channel


def find_cases_with_potential_sla(sf_connection: Salesforce, max_allowed_sla: int = 60) -> list:
    if max_allowed_sla < 0:
        max_allowed_sla = 60
    case_check_query = "SELECT id, " \
                       "OwnerId, " \
                       "Status, " \
                       "CaseNumber, " \
                       "Previous_Owner_Queue__c, " \
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
            'Previous_Owner__c': row['Previous_Owner_Queue__c'],
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

    def update_dbo_cases_after_notification_sent(self, row_id: str)->bool:
        try:
            self.cursor.execute("update [dbo].[Cases] set NotificationSent=1, NotificationSentDate=GETDATE() where ID=?", row_id)
            self.logging_inst.debug('update of Threat with id ' + str(row_id) + ' was completed')
            self.connection.commit()
            return True
        except Exception as error:
            self.connection.rollback()
            self.logging_inst.error(
                'update of Threat with id ' + str(row_id) + ' has failed due to the following error \n' + str(error))
            self.logging_inst.error('Query arguments: row_id:' + str(row_id))
            return False

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


def send_notification_to_web_hook(web_hook_url: str, threat: Threat, max_allowed_sla: int):
    logger_inst = logging.getLogger()
    if uri_validator(web_hook_url) is not True:
        logger_inst.error('Malformed url: ' + str(web_hook_url))
        return False
    team_connection = pymsteams.connectorcard(web_hook_url)
    if isinstance(threat, CaseSLA):
        team_connection.text("**Case " + str(threat.case_info_tuple[2]) + " has <" + str(max_allowed_sla) + " minutes left before target response time**")

        '''
        # create the section
        myMessageSection = pymsteams.cardsection()
        
        # Section Title
        myMessageSection.title("Case " + str(threat.case_info_tuple[2]) + " has <" + str(max_allowed_sla) + " minutes left before target response time")
        
        # Activity Elements
        myMessageSection.activityTitle("my activity title")
        myMessageSection.activitySubtitle("my activity subtitle")
        myMessageSection.activityImage("http://i.imgur.com/c4jt321l.png")
        myMessageSection.activityText("This is my activity Text")

        # Facts are key value pairs displayed in a list.
        myMessageSection.addFact("this", "is fine")
        myMessageSection.addFact("this is", "also fine")

        # Section Text
        myMessageSection.text("This is my section text")

        # Section Images
        myMessageSection.addImage("https://na62.salesforce.com/"+threat.case_info_tuple[3], ititle=threat.case_info_tuple[2])
        
        # Add your section to the connector card object before sending
        team_connection.addSection(myMessageSection)
        '''
        team_connection.addLinkButton("Open case", "https://na62.salesforce.com/"+str(threat.case_info_tuple[4]))
        # send the message.
        result = team_connection.send()
        return result
    else:
        logger_inst.error('Threat type' + str(type(threat)) + ' is not supported')
        return False


def uri_validator(ulr)->bool:
    regex = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|'  # ...or ipv4
        r'\[?[A-F0-9]*:[A-F0-9:]+\]?)'  # ...or ipv6
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    try:
        result = regex.match(ulr)
        if result is not None:
            return True
        else:
            return False
    except:
        return False


class BotExpectedError(Exception):
    def __init__(self, message, arguments):
        """Base class for more or less expected exceptions"""
        Exception.__init__(self, message + ": {0}".format(arguments))
        self.ErrorMessage = message
        self.ErrorArguments = arguments
        pass


class NoThreadsFound(BotExpectedError):
    """Raised when no threads were found"""
    pass


class SFGetUserNameError(BotExpectedError):
    """Raised when no a provided user_id cannot be evaluated to a user_name"""
    pass
