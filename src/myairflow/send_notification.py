import os
import requests

def send_noti(message):
    #WEBHOOK_ID = Variable.get('WEBHOOK_ID')
    #WEBHOOK_TOKEN = Variable.get('WEBHOOK_TOKEN')
    WEBHOOK_ID = os.getenv('WEBHOOK_ID')
    WEBHOOK_TOKEN = os.getenv('WEBHOOK_TOKEN')
    WEBHOOK_URL = f"https://discordapp.com/api/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}"
    #data = {"content": f"{kwargs['dag'].dag_display_name} {kwargs['task'].task_id} {kwargs['data_interval_start'].in_tz('Asia/Seoul').strftime('%Y%m%d%H')} OK/Jacob"}
    #data = { "content": "Jacob's workflow encountered an error" }
    data = { "content": message}
    response = requests.post(WEBHOOK_URL, json=data)

    if response.status_code == 204:
        print("메시지가 성공적으로 전송되었습니다.")
    else:
        print(f"에러 발생: {response.status_code}, {response.text}")
    return response.status_code
