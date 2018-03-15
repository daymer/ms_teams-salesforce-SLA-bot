from simple_salesforce import Salesforce


def find_cases_with_potential_sla(sf_connection: Salesforce)->list:
    case_check_query = "SELECT id, OwnerId, Status, CaseNumber, Previous_Owner__c, CreatedDate, Subject, AccountId, Flag__c, Manager_of_Case_Owner__c from case WHERE Time_to_Respond__c < 100 and Time_to_Respond__c > 0 and status in ('New', 'Open') and FTR_Case_Owner__c = null"
    found_cases = sf_connection.query(query=case_check_query)
    found_cases_list = []
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
        found_cases_list.append(case_info)
    return found_cases_list

