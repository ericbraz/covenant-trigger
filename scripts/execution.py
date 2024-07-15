import os
import time
import pandas as pd
from datetime import datetime
from config import path
from config import MAX_ATTEMPTS, MAX_TRIGGERS, MIN_INTERVAL, GLOBAL_TIMER, VARIANTS, BRAZIL_PREFIX, DIVIDER
from scripts.data_cleaner.df_manager import DfManager
from scripts.sender.message_sender import Sender
from constants.prospectColumns import PROSPECTS_COLUMNS, BREVO_COLUMNS

class CSVPromptHandler:

    def __init__(self):
        ...

    def generate_clean_csv_files_from_apify(self) -> None:
        df_manager = DfManager()
        data = df_manager.open_data("/files/apify/")
        selected_data = data[PROSPECTS_COLUMNS].copy()
        selected_data["cid"] = selected_data["cid"].astype(object)
        success = df_manager.save_file_data(selected_data, "/files/clean/clean_data.csv", sheet_name="data")
        time.sleep(5)
        return success
    
    def generate_csv_from_apify_to_email_marketing_platform(self) -> None:
        df_manager = DfManager()

        data = df_manager.open_data("/files/apify/emails")

        subset = ["emails/0", "contactDetails/emails/0"]
        nonemptyrows_dataframe = data.dropna(subset=subset, how="all")
        temp_columns = [col for col in PROSPECTS_COLUMNS]
        for col in subset:
            temp_columns.append(col)

        nonemptyrows_dataframe = nonemptyrows_dataframe[temp_columns]
        nonemptyrows_dataframe["email"] = nonemptyrows_dataframe["contactDetails/emails/0"].combine_first(nonemptyrows_dataframe["emails/0"]).copy()
        nonemptyrows_dataframe = nonemptyrows_dataframe.drop(columns=subset)
        cleaned_dataframe = nonemptyrows_dataframe.drop_duplicates(subset=["email"])

        selected_data = cleaned_dataframe[BREVO_COLUMNS].reset_index(drop=True).copy()
        selected_data["cid"] = selected_data["cid"].astype(object)
        selected_data = selected_data.rename(
            columns={
                "cid": "id",
                "title": "firstname",
                "subTitle": "lastname",
                "phoneUnformatted": "phone",
            }
        )
        selected_data["email"] = selected_data["email"].apply(self.clean_email)
        print("DataFrame shape:", selected_data.shape)
        success = df_manager.save_file_data(selected_data, "/files/brevo/brevo_data.csv")
        time.sleep(2)

        return success
    
    def clean_email(self, email):
        if isinstance(email, str):
            return email.replace('//', '').lower()
        return email

class WhatsappExecution:

    def __init__(self, execute_instance, remove_prompt_timers=False):
        self._csv_prompt_handler = CSVPromptHandler()

        self.execute_instance = execute_instance
        self.max_triggers = MAX_TRIGGERS
        self.message_interval = MIN_INTERVAL

        # List with columns to be saved on final DataFrame
        self._mobile_bool_list = []
        # True counter to be used inside for loop
        self._num = 0

        self._remove_timers = remove_prompt_timers

        self._prompt_one_executed = False
        self._prompt_two_executed = False
        self.file_exists = False

        self.df_manager = None
        self.start_time = None
        self.end_time = None
        self.filtered_data = None
        self.percentage = None
    
    def time_calc(self, interval: int, triggers: int) -> None:
        total_time = interval * triggers
        hours = int(total_time / 60)
        minutes = int(((total_time / 60) - hours) * 60)
        return "Tempo total de execução em aproximadamente {} minutos(s) e {} segundos(s)".format(hours, minutes)
        
    def bool_number(self, value: bool) -> int:
        if value:
            return 1
        return 0
    
    def is_mobile(self, number: str) -> bool:
        if not isinstance(number, (int, float)):
            return False
        split_number = number.split()
        if len(split_number) != 3:
            return False
        is_mobile = split_number[2].startswith("9")
        return is_mobile
 
    def print_execution_time(self) -> None:
        elapsed_time_seconds = self.end_time - self.start_time
        elapsed_time_minutes = elapsed_time_seconds / 60

        print(f"Tempos de execução: {elapsed_time_minutes:.2f} minutos")

    def prompt_one(self) -> None:
        self.generate_new_clean_data_csv = input("Há novos arquivos gerados pelo Apify? (y) ")
        limit = 0
        if self._remove_timers:
            self._prompt_one_executed = True
            return
        while True:
            self.message_interval = int(input("Defina intervalo entre as mensagens no disparo. (mínimo de {} segundos) ".format(MIN_INTERVAL)))
            limit += 1
            if self.message_interval >= MIN_INTERVAL:
                break
            else:
                print("Intervalo mínimo é de {} segundos. Tente novamente.".format(MIN_INTERVAL))
            if limit >= MAX_ATTEMPTS :
                print("Execução será encerrada")
                break
        if limit < MAX_ATTEMPTS:
            limit = 0
            while True:
                self.max_triggers = int(input("Defina o total de disparos. (máximo de {} disparos) ".format(MAX_TRIGGERS)))
                limit += 1
                if self.max_triggers <= MAX_TRIGGERS:
                    break
                else:
                    print("Por questões de segurança o máximo de disparos por vez é de {} disparos. Tente novamente.".format(MAX_TRIGGERS))
                if limit >= MAX_ATTEMPTS:
                    print("Execução será encerrada")
                    break
            if limit < MAX_ATTEMPTS:
                self._prompt_one_executed = True
    
    def prompt_two(self) -> None:
        self.start_time = time.time()

        time.sleep(GLOBAL_TIMER)
        print()
        print("Iniciando execução")

        if not self._remove_timers:
            time.sleep(GLOBAL_TIMER)
            print()
            print(self.time_calc(self.message_interval, self.max_triggers))

        if self.generate_new_clean_data_csv == "y":
            time.sleep(GLOBAL_TIMER)
            print()
            self._csv_prompt_handler.generate_clean_csv_files_from_apify()
        else:
            time.sleep(GLOBAL_TIMER)
            print()
            print("Nenhuma tabela nova foi gerada")
        
        if not self._remove_timers:
            time.sleep(GLOBAL_TIMER)
            print()
            print("Intervalo definido entre mensagens de {} segundos".format(self.message_interval))

        self.df_manager = DfManager()
        clean_data = self.df_manager.open_data("/files/clean/", dtype={"send_cid": str})
        clean_data["cid"] = clean_data["cid"].apply(lambda x: int(float(x)))

        time.sleep(5)

        # Removing rows without phone numbers
        self.filtered_data = clean_data[clean_data['phone'].notna() & (clean_data['phone'].str.startswith(BRAZIL_PREFIX) | pd.isnull(clean_data['phone']))]
        # Removing all prospects that already received messages
        sent = self.df_manager.open_data("/files/sent/", selected_file="sent.csv", dtype={"send_cid": str})
        list_sent_cids = []
        print()
        if not isinstance(sent, pd.DataFrame):
            print("Arquivo com prospectos enviados ainda não existe")
        else:
            self.file_exists = True
            sent["send_cid"] = sent["send_cid"].apply(lambda x: int(float(x)))
            sent["send_cid"] = sent["send_cid"]
            list_sent_cids = sent["send_cid"].to_list()
            print("Excluindo prospectos que já receberam mensagem")
            self.filtered_data = self.filtered_data[~self.filtered_data["cid"].isin(list_sent_cids)]
        
        self._prompt_two_executed = True
    
    def extract_csv_data(self):
        ...
    
    # filtered_data
    # _mobile_bool_list
    # df_manager
    # max_triggers
    # message_interval
    def trigger_all_messages_in_loop(self, filtered_data):
        ...

class FlaskExecution:
    
    def __init__(self) -> None:
        self._whatsapp_execution_instance = WhatsappExecution(self, remove_prompt_timers=True)
    
    def run_flask(self) -> None:
        self._whatsapp_execution_instance.prompt_one()

        if self._whatsapp_execution_instance._prompt_one_executed:
            self._whatsapp_execution_instance.prompt_two()

            cids = []
            names = []
            number = []
            mobile_bool = []
            dates = []
            for index, row in self._whatsapp_execution_instance.filtered_data.iterrows():
                current_trigger = self._whatsapp_execution_instance._num + 1

                # Identifies if number is mobile or not
                is_mobile = self._whatsapp_execution_instance.is_mobile(row["phone"])

                if is_mobile:

                    cids.append(row["cid"])
                    names.append(row["title"])
                    number.append(row["phone"])
                    mobile_bool.append(self._whatsapp_execution_instance.bool_number(is_mobile))
                    dates.append(datetime.now())

                    self._whatsapp_execution_instance._num += 1

                    if current_trigger >= self._whatsapp_execution_instance.max_triggers:
                        print("current_trigger:", current_trigger)
                        break
            
            pre_df = {
                "send_cid": cids,
                "names": names,
                "number": number,
                "is_mobile": mobile_bool,
                "time": dates,
            }
            temp_df = pd.DataFrame(pre_df)
            self._whatsapp_execution_instance.df_manager.save_file_data(temp_df, "/files/sent/sent_temp.csv", float_format="%.0f")

            os.system("python app.py")

class Execute:

    def __init__(self):
        self._csv_prompt_handler = CSVPromptHandler()
        self._whatsapp_execution_instance = WhatsappExecution(self)
        #self._generate_csvs_for_email_marketing_instance = GenerateCsvsForEmailMarketing(self)
        self._flask_execution = FlaskExecution()

    def csv_for_email_marketing_execution(self) -> None:
        print("Iniciando extração e exportação de arquivo CSV adaptado para plataforma de e-mail marketing")
        print()
        time.sleep(GLOBAL_TIMER)

        success = self._csv_prompt_handler.generate_csv_from_apify_to_email_marketing_platform()

        if success:
            print()
            print("Arquivo gerado com sucesso")
            print()
        else:
            print()
            print("Arquivo não pôde ser gerado")
            print()

    def whatsapp_trigger_execution(self) -> None:
            self._whatsapp_execution_instance.prompt_one()

            if self._whatsapp_execution_instance._prompt_one_executed:
                self._whatsapp_execution_instance.prompt_two()

                sender = Sender()
                print()
                print("Iniciado disparos...")
                print("Enviando requisições para a rota: {}".format(sender.api_url))
                print()
                time.sleep(GLOBAL_TIMER)

                for index, row in self._whatsapp_execution_instance.filtered_data.iterrows():
                    current_trigger = self._whatsapp_execution_instance._num + 1

                    # Identifies if number is mobile or not
                    split_number = row["phone"].split()
                    is_mobile = is_mobile = self._whatsapp_execution_instance.is_mobile(row["phone"])

                    if is_mobile:
                        print()
                        print(DIVIDER)
                        print()
                        self._whatsapp_execution_instance._mobile_bool_list.append(self._whatsapp_execution_instance.bool_number(is_mobile))

                        # Removing first digit (9) when DDD is not from SP
                        if split_number[1] not in list(range(11, 20)) and is_mobile:
                            phone = split_number[0] + split_number[1] + split_number[2][1:]
                        else:
                            phone = split_number[0] + split_number[1] + split_number[2]

                        # Removing non-numbers substring
                        clean_phone = phone.replace("+", "").replace("-", "")

                        cids = row["cid"]
                        names = row["title"]
                        number = row["phone"]
                        mobile_bool = self._whatsapp_execution_instance.bool_number(is_mobile)
                        dates = datetime.now()
                        print()
                        print(VARIANTS[self._whatsapp_execution_instance._num % len(VARIANTS)])
                        print()
                        #body = sender.body_formatter(phone=clean_phone, message=sender.set_message(VARIANTS[_whatsapp_execution_instance._num % len(VARIANTS)]))
                        #resp = sender.wpp_send_message(body)

                        if True: # resp.status_code == 200:
                            next_df = {
                                "send_cid": [cids],
                                "names": [names],
                                "number": [number],
                                "is_mobile": [mobile_bool],
                                "time": [dates],
                            }
                            if self._whatsapp_execution_instance.file_exists:
                                sent = self._whatsapp_execution_instance.df_manager.open_data("/files/sent/", selected_file="sent.csv")
                            sent_cids = pd.DataFrame(next_df)
                            final_sent_cids = pd.concat([sent, sent_cids])
                            print()
                            print("Salvando dados do prospecto")
                            # float_format="%.0f" i used to prevent the column send_cid to be saved without any data changes
                            self._whatsapp_execution_instance.df_manager.save_file_data(final_sent_cids, "/files/sent/sent.csv", float_format="%.0f")
                            self._whatsapp_execution_instance.file_exists = True

                            # Terminal messages to inform the user
                            print("Disparo n {}: Mensagem enviado para empresa {} no Whatsapp {} / {}.".format(current_trigger, row["title"], row["phone"], int(row["phoneUnformatted"])))
                            print("Fixed phone number: {}".format(row["phone"]))
                        else:
                            print("ERRO: Disparo n {} para empresa {} não foi possível no número {} / {}.".format(current_trigger, row["title"], row["phone"], int(row["phoneUnformatted"])))

                        if current_trigger != self._whatsapp_execution_instance.max_triggers:
                            print("Aguarde próximo envio em {} segundos".format(self._whatsapp_execution_instance.message_interval))
                        if current_trigger >= self._whatsapp_execution_instance.max_triggers:
                            time.sleep(3)
                            print()
                            print(DIVIDER)
                            print()
                            print("Execução finalizada")
                            break
                        time.sleep(self._whatsapp_execution_instance.message_interval)

                        self._whatsapp_execution_instance._num += 1
                    else:
                        continue
                    
                self._whatsapp_execution_instance.end_time = time.time()
                self._whatsapp_execution_instance.print_execution_time() #(self.start_time, self.end_time)

                self._whatsapp_execution_instance.percentage = (sum(self._whatsapp_execution_instance._mobile_bool_list) / len(self._whatsapp_execution_instance._mobile_bool_list)) * 100
                print()
                print("Total de números de celular (%): {}%".format(self._whatsapp_execution_instance.percentage))
                print()
    
    def whatsapp_flask_execution(self) -> None:
        self._flask_execution.run_flask()
