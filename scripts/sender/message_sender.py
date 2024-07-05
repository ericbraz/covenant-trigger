# Documentation
# https://bump.sh/aldinokemal/doc/go-whatsapp-web-multidevice/operation/operation-sendmessage

# API Repo
# https://github.com/aldinokemal/go-whatsapp-web-multidevice

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import requests
from config import API_URL

class Sender:
    def __init__(self):
        self.api_url = API_URL
        pass

    def __api_request(self, method, url, body=None, headers={}, params=None):
        method = method.upper()
        if method == "LOGIN":
            response = requests.get(url)
        elif method == "GET":
            response = requests.get(url, params=params)
        elif method == "POST":
            response = requests.post(url, json=body)
        elif method == "PUT":
            response = requests.put(url, json=body)
        elif method == "DELETE":
            response = requests.delete(url, headers=headers)
        else:
            raise Exception(f"Unsupported method: {method}")

        if response.status_code >= 200 and response.status_code < 300:
            return response
        else:
            print("ERROR:", response)
        return response

    def wpp_send_message(self, body=None):
        headers = {}
        endpoint = "/send/message"
        response = self.__api_request(method="post", url=self.api_url + endpoint, body=body, headers=headers)
        return response
    
    def body_formatter(self, phone, message, reply_id=None):
        phone_number = "{}@s.whatsapp.net".format(phone)
        body = {
            "phone": phone_number,
            "message": message,
            "reply_message_id": reply_id,
        }
        return body
    
    def send_test(self, number: str, message: str=None):
        phone_number = number
        if not message:
            message = 'Mensagem de teste\n\n "Jesus chorou", \nJoão 11:35'
        reply_message_id = "" # ID format when it's the case "3EB089B9D6ADD58153C561"
        body = self.body_formatter(phone=phone_number, message=message, reply_id=reply_message_id)

        resp = self.wpp_send_message(body=body)
        print(resp)
        return resp

def main():
    # Below an example of the code sending a message
    
    sender = Sender()
    
    phone_number = "556184617368"
    message = '"Mensagem de teste\n\n Jesus chorou", \nJoão 11:35'
    reply_message_id = ""
    body = sender.body_formatter(phone=phone_number, message=message, reply_id=reply_message_id)

    resp = sender.wpp_send_message(body=body)
    print(resp)

if __name__ == "__main__":
    main()
