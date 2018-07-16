from simple_salesforce import Salesforce, exceptions
import pyodbc
import configuration
import logging
import re
from collections import namedtuple
from abc import ABCMeta
import sys
import pymsteams
import pickle
import operator


class Threat(object):
    __metaclass__ = ABCMeta

    def __init__(self, target_notification_channel: str):
        self.completed = False
        self.respond_from_target = False
        self.notification_date = False
        self.current_SLA = None
        self.target_notification_channel = target_notification_channel


class CaseSLA(Threat):
    def __init__(self, target_notification_channel: str, case_info: tuple):
        Threat.__init__(self, target_notification_channel)
        self.info_tuple = case_info


class KarmaEvent(Threat):
    def __init__(self, target_notification_channel: str, event_info: tuple, event_type: str):
        Threat.__init__(self, target_notification_channel)
        self.info_tuple = event_info
        self.event_type = event_type


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
    except (exceptions.SalesforceResourceNotFound, SFGetUserNameError):
        try:
            group_name = sf_get_group_name(sf_connection=sf_connection, group_id=user_or_group_id)
            return group_name
        except (exceptions.SalesforceResourceNotFound, SFGetUserNameError):
            answer = (None,)
            return answer


def find_target_teams_channel_for_case_sla(current_case_owner_id: str, previous_case_owner_id: str, product: str, teams_channels_inst: configuration.TeamsChannels)-> str:
    main_logger = logging.getLogger()
    target_teams_channel = 'undefined'
    # supported_source_pretty_name = None
    logger_inst = logging.getLogger()
    sf_queues_inst = configuration.SFQueues()
    queue_dict = sf_queues_inst.queue_dict
    if product in sf_queues_inst.monitor_products:
        main_logger.info('Monitor product found, using extra logic')
        target_teams_channel = teams_channels_inst.webhooks_dict['Tier 2 - EM:Europe']
        return target_teams_channel
    elif product in sf_queues_inst.agent_products:
        main_logger.info('Agent product found, using extra logic')
        target_teams_channel = teams_channels_inst.webhooks_dict['Tier 1 - Agents']
        return target_teams_channel
    try:
        supported_source_pretty_name = [key for key in queue_dict.keys() if (queue_dict[key] == current_case_owner_id)]
        supported_source_pretty_name = supported_source_pretty_name[0]
    except IndexError:
        try:
            supported_source_pretty_name = previous_case_owner_id
            pass
        except KeyError:
            supported_source_pretty_name = None
            logger_inst.error('Cannot find a target channel to notify about current_case_owner_id:' + str(current_case_owner_id) + ' and Previous_Owner_Queue__c:' + str(previous_case_owner_id))
    if supported_source_pretty_name is not None:
        try:
            target_teams_channel = teams_channels_inst.webhooks_dict[supported_source_pretty_name]
        except KeyError:
            logger_inst.error('Cannot find a target channel to notify about ' + str(supported_source_pretty_name))
        except Exception:
            logger_inst.error('Cannot find a target channel to notify about ' + str(supported_source_pretty_name))

    return target_teams_channel


def find_target_teams_channel_for_karma_event(event_type: str, teams_channels_inst: configuration.TeamsChannels, event_dict: dict)-> str:
    main_logger = logging.getLogger()
    target_teams_channel = 'undefined'
    # supported_source_pretty_name = None
    logger_inst = logging.getLogger()
    if event_type == 'delete':
        target_teams_channel = teams_channels_inst.webhooks_dict['xWiki change log']
    elif event_type == 'reindex':
        if event_dict['full'] is True:
            target_teams_channel = teams_channels_inst.webhooks_dict['xWiki change log']
        else:
            target_teams_channel = teams_channels_inst.webhooks_dict['xWiki change log']
    elif event_type == 'vote':
        target_teams_channel = teams_channels_inst.webhooks_dict['xWiki change log']
    else:
        logger_inst.critical('find_target_teams_channel_for_karma_event, Unsupported event_type: '+ event_type)
        return target_teams_channel
    return target_teams_channel


def find_cases_with_potential_sla(sf_connection: Salesforce, max_allowed_sla: int = 60, min_allowed_sla: int = 0) -> list:
    if max_allowed_sla < 0:
        max_allowed_sla = 60
    if min_allowed_sla < 0:
        min_allowed_sla = 0
    case_check_query = "SELECT id, " \
                       "OwnerId, " \
                       "Status, " \
                       "CaseNumber, " \
                       "Previous_Owner_Queue__c, " \
                       "CreatedDate, " \
                       "Subject, " \
                       "AccountId, " \
                       "Flag__c, " \
                       "Product__c, " \
                       "Manager_of_Case_Owner__c from case " \
                            "WHERE Time_to_Respond__c <= " + str(max_allowed_sla) + " and " \
                                  "Time_to_Respond__c > " + str(min_allowed_sla) + " and " \
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
            'Manager_of_Case_Owner__c': row['Manager_of_Case_Owner__c'],
            'Product__c': row['Product__c']
        }
        found_cases_list.append(case_info)
    return found_cases_list


def get_current_case_sla(sf_connection: Salesforce, case_id: str):
    try:
        case_sla = sf_connection.CASE.get(case_id)
        answer = case_sla['Time_to_Respond__c']
        return answer
    except exceptions.SalesforceResourceNotFound:
        exc_tuple = sys.exc_info()
        raise SFGetUserNameError('SalesforceResourceNotFound', {'case_id': case_id, 'exception': exc_tuple[1]})
    except Exception:
        exc_tuple = sys.exc_info()
        raise SFGetUserNameError('OtherException', {'case_id': case_id, 'exception': exc_tuple[1]})


class SQLConnectorELISADB:
    def __init__(self, sql_config: configuration.SQLConfigELISADB, use_test_instance: bool=False):
        if use_test_instance is False:
            self.connection = pyodbc.connect(
                'DRIVER=' + sql_config.Driver + ';PORT=1433;SERVER=' + sql_config.Server + ';PORT=1443;DATABASE='
                + sql_config.Database + ';UID=' + sql_config.Username + ';PWD=' + sql_config.Password)
        else:
            self.connection = pyodbc.connect(
                'DRIVER=' + sql_config.Driver + ';PORT=1433;SERVER=' + sql_config.Server + ';PORT=1443;DATABASE='
                + sql_config.Database_test + ';UID=' + sql_config.Username + ';PWD=' + sql_config.Password)
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

    def update_dbo_karma_events_after_notification_sent(self, row_id: str)->bool:
        try:
            self.cursor.execute("update [dbo].[Karma_events] set NotificationSent=1, NotificationSentDate=GETDATE() where ID=?", row_id)
            self.logging_inst.debug('update of Threat with id ' + str(row_id) + ' was completed')
            self.connection.commit()
            return True
        except Exception as error:
            self.connection.rollback()
            self.logging_inst.error(
                'update of Threat with id ' + str(row_id) + ' has failed due to the following error \n' + str(error))
            self.logging_inst.error('Query arguments: row_id:' + str(row_id))
            return False

    def insert_into_dbo_cases(self, case_dict: dict, rule: str) -> bool:
        CaseId = case_dict['Id']
        CaseNumber = case_dict['CaseNumber']
        OwnerId = case_dict['OwnerId']
        Status = case_dict['Status']
        CreatedDate = case_dict['CreatedDate'][:-5]
        Subject = case_dict['Subject']
        AccountId = case_dict['AccountId']
        Flag = str(case_dict['Flag__c'])
        Product = case_dict['Product__c']
        matches = re.search(r"\salt=\"([^\"]*)\"\s", Flag, re.IGNORECASE)
        if matches:
            Flag = matches.group(1)
        Previous_Owner = case_dict['Previous_Owner__c']
        Manager_of_Case_Owner = case_dict['Manager_of_Case_Owner__c']
        target_notification_channel = case_dict['target_notification_channel']
        case_tuple = (CaseNumber, OwnerId, CaseId, CreatedDate, target_notification_channel, Status, Subject, AccountId, Flag, Previous_Owner,
                      Manager_of_Case_Owner, rule, Product)
        try:
            self.cursor.execute(
                "insert into [dbo].[Cases] values(NEWID(),?,?,?,?,GETDATE(), 0, Null, ?,?,?,?,?,?,?,?,?)",
                *case_tuple[0:13])
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

    def insert_into_dbo_karma_events(self, event_dict: dict, event_type=str):
        if 'direction' not in event_dict:
            event_dict['direction'] = None
            event_dict['user_name'] = None
        if 'full' not in event_dict:
            event_dict['full'] = None
        event_tuple = (event_type, event_dict['date'], event_dict['target_notification_channel'], event_dict['link'], event_dict['xwd_fullname'], event_dict['user_name'], event_dict['direction'], event_dict['full'])
        try:
            self.cursor.execute(
                "insert into [dbo].[Karma_events] values(NEWID(),?,?, ?,?,?,0,NULL,?,?,?)",
                *event_tuple[0:10])
            self.connection.commit()
            self.logging_inst.info(
                '----Insertion of event with xwd ' + event_dict['xwd_fullname'] + ' was completed')
            return True
        except pyodbc.IntegrityError:
            self.connection.rollback()
            self.logging_inst.info(
                '----Event for xwd ' + event_dict['xwd_fullname'] + ' is already added, skipping')
            return True
        except Exception as error:
            self.connection.rollback()
            self.logging_inst.error(
                '----insertion of event with xwd ' + str(event_dict['xwd_fullname']) + ' has failed due to the following error \n' + str(error))
            self.logging_inst.error('-------------Query arguments:\n' + str(event_dict))
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
                ",[ManagerCaseOwner]" \
                ",[Product] FROM [dbo].[Cases]" \
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

    def select_all_unanswered_threats_from_karma_events(self) -> list:
        query = "SELECT [TargetNotificationChannel]" \
                ",[ID]"\
                ",[Type]" \
                ",[CreatedDate]" \
                ",[link]" \
                ",[xwd_fullname]" \
                ",[user_name]" \
                ",[direction]" \
                ",[full]" \
                "FROM [dbo].[Karma_events]" \
                    "where [NotificationSent]=0"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        if rows:
            answer = []
            for row in rows:
                verified_row = KarmaEvent(target_notification_channel=row.TargetNotificationChannel, event_info=tuple(row), event_type=row.Type)
                answer.append(verified_row)
            return answer
        else:
            raise NoThreadsFound('No unanswered threads were found', {'source': '[dbo].[Karma_events]'})

    def select_existence_id_from_karma_events(self, xwd_fullname, event_type, created_date):
        query = "SELECT count([ID]) as cc "\
                "FROM [dbo].[Karma_events] "\
                "where[xwd_fullname] = ? and [Type] = ? and DATEDIFF(HH, [CreatedDate], ?) <= 1 and [NotificationSent] = 1"
        self.cursor.execute(query, xwd_fullname, event_type, created_date)
        row = self.cursor.fetchone()
        if row:
            if row.cc > 0:
                return True
            else:
                return False
        else:
            return None


def send_notification_to_web_hook(web_hook_url: str, threat: Threat):
    logger_inst = logging.getLogger()
    logger_inst.debug('web_hook_url: ' + str(web_hook_url))
    logger_inst.debug('threat: ' + str(threat))
    if uri_validator(web_hook_url) is not True:
        logger_inst.error('Malformed url: ' + str(web_hook_url))
        return False
    team_connection = pymsteams.connectorcard(web_hook_url)
    if isinstance(threat, CaseSLA):
        team_connection.text("**Case 0" + str(threat.info_tuple[2]) + "** has **<" + str(threat.current_SLA) + "** minutes left before the target response time")
        team_connection.addLinkButton("Open case", "https://veeam.my.salesforce.com/" + str(threat.info_tuple[4]))
        #team_connection.entities(type='mention', id='ba07baab-431b-49ed-add7-cbc3542f5140', name='Test channel')
        team_connection.color('red')
        result = team_connection.send()
        return result
    if isinstance(threat, KarmaEvent):
        sql_config_instance_karma_db = configuration.SQLConfigKARMADB()
        sql_connector_instance_karma_db = SQLConnectorKARMADB(sql_config_instance_karma_db)
        if threat.event_type == 'delete':
            text = 'Page was **deleted** from xWiki, former page id: **"' + str(threat.info_tuple[5]) + '"**'
            team_connection.text(text)
            team_connection.color('EB984E')
            result = team_connection.send()
            return result
        elif threat.event_type == 'vote':
            page_name = sql_connector_instance_karma_db.select_page_title_by_page_id(str(threat.info_tuple[5]))
            logger_inst.debug('page_name: ' + str(page_name))
            page_stats = sql_connector_instance_karma_db.select_page_stats(xwd_id=str(threat.info_tuple[5]))
            if page_stats is not None:
                logger_inst.debug('page_stats: ' + str(page_stats))
                pretty_name = threat.info_tuple[6][:1].capitalize() + '. ' + threat.info_tuple[6][1:2].capitalize() + threat.info_tuple[6][2:]
                if threat.info_tuple[7] == 1:
                    text = '**Voted UP** **"' + page_name + '"** by ' + pretty_name + '\n\n'
                else:
                    text = '**Voted DOWN** **"' + page_name + '"** by ' + pretty_name + '\n\n'
                text += 'Top contributor(s):'
                for key, value in page_stats['contributors_percents'].items():
                    if key == 'XWiki.bot':
                        continue
                    name = str(key).replace('XWiki.', '')
                    pretty_name = name[:1].capitalize() + '. ' + name[1:2].capitalize() + \
                                  name[2:]
                    text += ' ' + pretty_name + ' (' + str(value) + '%),'
                text = text[:-1] + ';'
                text += ' Karma score: ' + str(page_stats['page_karma_score'])
                team_connection.color('5DADE2')
                logger_inst.debug('text: ' + str(text))
                team_connection.text(text)
                team_connection.addLinkButton("Go to the article", str(threat.info_tuple[4]))
                result = team_connection.send()
                return result
            else:
                logger_inst.info(
                    'Page has less than 100 characters, no need to notify about it, page_id:' + str(
                        threat.info_tuple[5]))
                return True
        elif threat.event_type == 'reindex':
            page_name = sql_connector_instance_karma_db.select_page_title_by_page_id(str(threat.info_tuple[5]))
            if page_name is not None:
                page_stats = sql_connector_instance_karma_db.select_page_stats(xwd_id=str(threat.info_tuple[5]))
                if page_stats is not None:
                    if threat.info_tuple[8] is False:
                        # it's an increment
                        text = '**Updated** version of \n**"' + page_name + '"**\nis available on xWiki now\n\n'
                        text += 'Top contributor(s):'
                        for key, value in page_stats['contributors_percents'].items():
                            if key == 'XWiki.bot':
                                continue
                            name = str(key).replace('XWiki.', '')
                            pretty_name = name[:1].capitalize() + '. ' + name[1:2].capitalize() + \
                                          name[2:]
                            text += ' ' + pretty_name + ' (' + str(value) + '%),'
                        text = text[:-1] + ';'
                        # text += 'Karma score: ' + str(page_stats['page_karma_score']) + ', '+ str(page_stats['up_votes']) +'⇧' + str(page_stats['down_votes']) + '⇩ '
                        text += ' Karma score: ' + str(page_stats['page_karma_score'])
                        team_connection.color('F4D03F')
                    else:
                        # it's a full
                        if str(threat.info_tuple[5]).startswith('Main'):
                            xwiki_part = 'Main'
                        elif str(threat.info_tuple[5]).startswith('Staging'):
                            xwiki_part = 'Staging'
                        else:
                            xwiki_part = 'Administrative'
                        text = 'A **new** article **"' + page_name + '"** was added into the **' + xwiki_part + '** part of the xWiki!\n\n'
                        text += 'Top contributor(s):'
                        for key, value in page_stats['contributors_percents'].items():
                            if key == 'XWiki.bot':
                                continue
                            name = str(key).replace('XWiki.', '')
                            pretty_name = name[:1].capitalize() + '. ' + name[1:2].capitalize() + \
                                          name[2:]
                            text += ' ' + pretty_name + ' (' + str(value) + '%),'
                        text = text[:-1] + ';'
                        # text += 'Karma score: ' + str(page_stats['page_karma_score']) + ', '+ str(page_stats['up_votes']) +'⇧' + str(page_stats['down_votes']) + '⇩ '
                        text += ' Karma score: ' + str(page_stats['page_karma_score'])
                        team_connection.color('C39BD3')
                    team_connection.text(text)
                    team_connection.addLinkButton("Go to the article", str(threat.info_tuple[4]))
                    result = team_connection.send()
                    return result
                else:
                    logger_inst.info(
                        'Page has less than 100 characters, no need to notify about it, page_id:' + str(threat.info_tuple[5]))
                    return True
            else:
                logger_inst.critical('Unable to get page_title by the provided page_id:' + str(threat.info_tuple[5]))
                logger_inst.critical('Aborting message send operation for '+str(threat.info_tuple[1]))
                return False
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


class SQLConnectorKARMADB:
    def __init__(self, sql_config: configuration.SQLConfigKARMADB):
        self.connection = pyodbc.connect(
            'DRIVER=' + sql_config.Driver + ';PORT=1433;SERVER=' + sql_config.Server + ';PORT=1443;DATABASE='
            + sql_config.Database + ';UID=' + sql_config.Username + ';PWD=' + sql_config.Password)
        self.cursor = self.connection.cursor()
        self.logging_inst = logging.getLogger()

    def select_page_title_by_page_id(self, page_id: str) -> str:
        self.cursor.execute(
            "SELECT [page_title] FROM [dbo].[KnownPages] where [page_id] = ?", 'xwiki:'+page_id)
        raw = self.cursor.fetchone()
        if raw:
            return raw.page_title
        return None

    def select_page_stats(self, xwd_id):
        result = self.select_id_characters_total_from_dbo_knownpages(platform='xwiki', page_id=xwd_id)
        if result is None:
            return None
        page_sql_id = result[0]
        total_characters_of_requested_page = int(result[1])
        if total_characters_of_requested_page < 100:
            return None
        self.cursor.execute(
            "EXEC dbo.[get_page_karma_and_votes] @page_id = ?", page_sql_id)
        raw = self.cursor.fetchone()
        if raw:
            up_votes = raw.up
            down_votes = raw.down
            karma_score = raw.karma_total_score
            answer = {
                'up_votes': up_votes,
                'down_votes': down_votes,
                'page_karma_score': karma_score,
                'contributors_percents': {}
            }
            total_contribute_of_requested_page = pickle.loads(
                self.select_datagram_contribution_from_dbo_knownpages_contribution(
                    sql_id=page_sql_id))
            for Contributor, Value in total_contribute_of_requested_page.items():
                percent = round(((Value / total_characters_of_requested_page) * 100), 2)
                answer['contributors_percents'].update({Contributor: percent})
            contributors_percents_sorted = sorted(answer['contributors_percents'].items(),
                                                  key=operator.itemgetter(1), reverse=True)
            answer['contributors_percents'] = {}
            for unit in contributors_percents_sorted[:3]:
                answer['contributors_percents'].update({unit[0]: unit[1]})
            return answer

    def select_id_characters_total_from_dbo_knownpages(self, platform: str, page_id: str=None, page_title: str=None):
        logger = logging.getLogger()
        if page_id is not None:
            if platform.lower() == 'xwiki':
                page_id = 'xwiki:' + page_id
            logger.debug('page_id: ' + str(page_id) + 'platform: ' + str(platform))
            self.cursor.execute(
                "select [id],[characters_total] FROM [dbo].[KnownPages] where [page_id] = ? and [platform] LIKE LOWER(?)",
                page_id, platform)
            raw = self.cursor.fetchone()
            logger.debug('select [id],[characters_total] FROM [dbo].[KnownPages] where [page_id] result: ' + str(raw))
            if raw:
                logger.debug('raw.id: ' + str(raw.id) + 'raw.characters_total:' + str(raw.characters_total))
                return raw.id, raw.characters_total
            return None
        if page_title is not None:
            self.cursor.execute(
                "select [id], [characters_total] from [dbo].[KnownPages] where [page_title] = ? and [platform] LIKE LOWER(?)",
                page_title,
                platform)
            raw = self.cursor.fetchone()
            if raw:
                return raw.id, raw.characters_total
            return None

    def select_datagram_contribution_from_dbo_knownpages_contribution(self, sql_id: str):
        self.cursor.execute(
            "select [datagram_contribution] from [dbo].[KnownPages_contribution] where [KnownPageID] = ?", sql_id)
        raw = self.cursor.fetchone()
        if raw:
            return raw.datagram_contribution
        return None

    def find_karma_events(self, event_type: str) -> list:
        if event_type == 'delete':
            query = "SELECT [ID]"\
                    ",[date]"\
                    ",[link]"\
                    ",[xwd_fullname] "\
                    "FROM [dbo].[WebRequests_delete_page_by_XWD_FULLNAME] "\
                    "where [committed]=1 and [result]=1 and datediff(HH,[date],GETDATE()) <= 1"
        elif event_type == 'reindex':
            query = "SELECT [ID]"\
                    ",[date]"\
                    ",[link]"\
                    ",[xwd_fullname]" \
                    ",[full] " \
                    "FROM [dbo].[WebRequests_reindex_page_by_XWD_FULLNAME]"\
                    "where [committed]=1 and [result]=1 and datediff(HH,[date],GETDATE()) <= 1 "
        elif event_type == 'vote':
            query = "SELECT [ID]"\
                    ",[date]"\
                    ",[link]"\
                    ",[xwd_fullname] " \
                    ",[user_name] " \
                    ",[direction] " \
                    "FROM [dbo].[WebRequests_vote_for_page_as_user] "\
                    "where [committed]=1 and [result]=1 and datediff(HH,[date],GETDATE()) <= 1"
        else:
            self.logging_inst.critical('Requested Event type is not supported: ' + event_type)
            return []
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        found_events_list = []
        if rows:
            for row in rows:
                    event_info = {
                        'type': event_type,
                        'Id': row.ID,
                        'date': row.date,
                        'link': row.link,
                        'xwd_fullname': row.xwd_fullname,
                    }
                    if event_type == 'vote':
                        event_info.update({'direction': row.direction})
                        event_info.update({'user_name': row.user_name})
                    if event_type == 'reindex':
                        event_info.update({'full':row.full})

                    found_events_list.append(event_info)
        return found_events_list

'''

def entities(self, type, id, name):
    self.payload["entities"] = []
    newfact = {
        'type' : type,
        'id': id,
        'name': name
    }
    self.payload["entities"].append(newfact)


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