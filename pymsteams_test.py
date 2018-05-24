import pymsteams

# You must create the connectorcard object with the Microsoft Webhook URL
myTeamsMessage = pymsteams.connectorcard("https://outlook.office.com/webhook/475674b9-9ce0-41d7-8754-7aac41e75c27@ba07baab-431b-49ed-add7-cbc3542f5140/IncomingWebhook/fbf7b100df1e44a38b4caef4ec86e40e/80192086-62c7-4ecc-b59a-9d36ebe80b65")

# Add text to the message.
myTeamsMessage.text("This is a test message <at>@Dmitriy Rozhdestvenskiy</at>")


myTeamsMessage.addMention(id='7ec73d06-9065-43b6-8b8f-95d34f72a25e', name='Dmitriy Rozhdestvenskiy')

# aa32037b-8df9-4143-b9ea-de1269c14b69


myTeamsMessage.printme()
# send the message.
result = myTeamsMessage.send()
print(result)
'''
from datetime import datetime, timedelta
print(datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000+0000"))
current_utc_time = datetime.utcnow() - timedelta(hours=1)
current_utc_time = current_utc_time.strftime("%Y-%m-%dT%H:%M:%S.000+0000")
print(current_utc_time)
'''