import pymsteams

# You must create the connectorcard object with the Microsoft Webhook URL
myTeamsMessage = pymsteams.connectorcard("<Microsoft Webhook URL>")

# Add text to the message.
myTeamsMessage.text("this is my text")

# send the message.
myTeamsMessage.send()
'''
from datetime import datetime, timedelta
print(datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000+0000"))
current_utc_time = datetime.utcnow() - timedelta(hours=1)
current_utc_time = current_utc_time.strftime("%Y-%m-%dT%H:%M:%S.000+0000")
print(current_utc_time)
'''