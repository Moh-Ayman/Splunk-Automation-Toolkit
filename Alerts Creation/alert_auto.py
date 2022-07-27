import json
import requests


search_name = 'No events in important_index for the last 24 hours'

#data = {'output_mode': 'json'}
#response = requests.delete(splunk_server + '/servicesNS/' + app_author + '/' + app_name + '/saved/searches/' + search_name, data=data, auth=(user, password), verify=False)

app_author='admin'
app_name='test_app'
email = 'j.smith@corporation.com'
search = 'index="important_index" earliest=-1d'
alert_comparator = 'equal to'
alert_threshold = 0
cron = '0 5 * * *'

data = {
'output_mode': 'json',
'action.email.bcc': '',
'action.email.cc': '',
'action.email.content_type': 'plain',
'action.email.message.alert': 'The alert condition for \'$name$\' was triggered.',
'action.email.message.report': 'The scheduled report \'$name$\' has run.',
'action.email.to': email,
'action.email.sendresults': '1',
'action.email.inline': '0',
'action.email.format': 'csv',
'actions': 'email',
'alert.digest_mode': '1',
'alert.expires': '24h',
'alert.managedBy': '',
'alert.severity': '3',
'alert.suppress': '0',
'alert.suppress.fields': '',
'alert.suppress.period': '',
'alert.track': '0',
'alert_comparator': alert_comparator,
'alert_condition': '',
'alert_threshold': alert_threshold,
'alert_type': 'number of events',
'allow_skew': '0',
'cron_schedule': cron,
'description': '',
'disabled': '0',
'displayview': '',
'is_scheduled': '1',
'is_visible': '1',
'max_concurrent': '1',
'name': search_name,
'realtime_schedule': '1',
'restart_on_searchpeer_add': '1',
'run_n_times': '0',
'run_on_startup': '0',
'schedule_priority': 'default',
'schedule_window': '0',
'search': search,
'action.email': '1'
}
user='admin'
password='admin'
splunk_server="https://192.168.197.145:8089"
response = requests.post(splunk_server + '/servicesNS/' + app_author + '/' + app_name + '/saved/searches', data=data, auth=(user, password), verify=False)
print(response.text)
