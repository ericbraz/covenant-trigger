# Documentation
# https://bump.sh/aldinokemal/doc/go-whatsapp-web-multidevice/operation/operation-sendmessage

# API Repo
# https://github.com/aldinokemal/go-whatsapp-web-multidevice

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import requests
import urllib.parse
from config import API_URL
from scripts.data_cleaner.df_manager import DfManager

class Sender:
    def __init__(self):
        self.api_url = API_URL
        self._df_manager = DfManager()

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
    
    def body_formatter(self, phone, message, reply_id=None) -> dict:
        phone_number = "{}@s.whatsapp.net".format(phone)
        body = {
            "phone": phone_number,
            "message": message,
            "reply_message_id": reply_id,
        }
        return body
    
    def set_message(self, variant: str) -> str:
        return f"""🌐 *{variant}*

    👉 Nós, da Covenant, somos especialistas na criação de sites e ecommerces personalizados. Oferecemos design único, manutenção regular e serviços complementares para otimizar sua presença digital. 🚀 Criamos plataformas personalizadas, com design exclusivo, otimizadas para SEO e que se adaptam a qualquer dispositivo.

    📲 Me responde com "SIM" e vamos marcar um bate-papo sem compromisso para discutir suas necessidades e como podemos te ajudar a alcançar seus objetivos! 😉
    """

    def whatsapp_link_formatter(self, whatsapp: str, variant_text="") -> str:
        clean_wpp = self._df_manager.clean_telephones(whatsapp)
        formatted_text = self.set_message(variant_text)
        encoded_variant_text = urllib.parse.quote(formatted_text)
        return f"https://web.whatsapp.com/send?phone={clean_wpp}&text={encoded_variant_text}"

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
