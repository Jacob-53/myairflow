import requests
import os
from myairflow.send_notification import send_noti

def test_notify():
    message = "pytest:Jacob"
    r = send_noti(message)
    assert r == 204
