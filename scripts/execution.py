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
        df_manager.save_file_data(selected_data, "/files/clean/clean_data.csv", sheet_name="data")
        time.sleep(5)
    
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
        print("DataFrame shape:", selected_data.shape)
        success = df_manager.save_file_data(selected_data, "/files/brevo/brevo_data.csv")
        time.sleep(2)

        return success

class WhatsappExecution:

    def __init__(self, execute_instance):
        self.execute_instance = execute_instance
    
    def set_message(variant: str) -> str:
        return """🌐 *{}*

    👉 Nós, da Covenant, somos especialistas na criação de sites e ecommerces personalizados. Oferecemos design único, manutenção regular e serviços complementares para otimizar sua presença digital. 🚀 Criamos plataformas personalizadas, com design exclusivo, otimizadas para SEO e que se adaptam a qualquer dispositivo.

    📲 Me responde com "SIM" e vamos marcar um bate-papo sem compromisso para discutir suas necessidades e como podemos te ajudar a alcançar seus objetivos! 😉
    """.format(variant)
    
    def time_calc(self, interval: int, triggers: int) -> None:
        total_time = interval * triggers
        hours = int(total_time / 60)
        minutes = int(((total_time / 60) - hours) * 60)
        return "Tempo total de execução em aproximadamente {} minutos(s) e {} segundos(s)".format(hours, minutes)
        
    def bool_number(self, value: bool) -> int:
        if value:
            return 1
        return 0
    
    def print_execution_time(self, start_time, end_time) -> None:
        elapsed_time_seconds = end_time - start_time
        elapsed_time_minutes = elapsed_time_seconds / 60

        print(f"Tempos de execução: {elapsed_time_minutes:.2f} minutos")

    def prompts(self) -> None:
        generate_new_clean_data_csv = input("Gerar nova tabela com dados limpos? (y) ")
        limit = 0
        while True:
            message_interval = int(input("Defina intervalo entre as mensagens no disparo. (mínimo de {} segundos) ".format(MIN_INTERVAL)))
            limit += 1
            if message_interval >= MIN_INTERVAL:
                break
            else:
                print("Intervalo mínimo é de {} segundos. Tente novamente.".format(MIN_INTERVAL))
            if limit >= MAX_ATTEMPTS :
                print("Execução será encerrada")
                break
        if limit < MAX_ATTEMPTS:
            limit = 0
            while True:
                max_triggers = int(input("Defina o total de disparos. (máximo de {} disparos) ".format(MAX_TRIGGERS)))
                limit += 1
                if max_triggers <= MAX_TRIGGERS:
                    break
                else:
                    print("Por questões de segurança o máximo de disparos por vez é de {} disparos. Tente novamente.".format(MAX_TRIGGERS))
                if limit >= MAX_ATTEMPTS:
                    print("Execução será encerrada")
                    break
            if limit < MAX_ATTEMPTS:
                return {
                    "generate_new_clean_data_csv": generate_new_clean_data_csv,
                    "message_interval": message_interval,
                    "max_triggers": max_triggers,
                }
            return None
        return None
    
    def extract_csv_data(self):
        ...
    
    # filtered_data
    # mobile_bool_list
    # df_manager
    # max_triggers
    # message_interval
    def trigger_all_messages_in_loop(self, filtered_data):
        ...

class Execute:

    def __init__(self):
        self._csv_prompt_handler = CSVPromptHandler()
        self._whatsapp_execution_instance = WhatsappExecution(self)
        #self._generate_csvs_for_email_marketing_instance = GenerateCsvsForEmailMarketing(self)

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

    def whatsapp_execution(self) -> None:
            prompt = self._whatsapp_execution_instance.prompts()

            if prompt:
                start_time = time.time()

                time.sleep(GLOBAL_TIMER)
                print()
                print("Iniciando execução")

                time.sleep(GLOBAL_TIMER)
                generate_new_clean_data_csv, message_interval, max_triggers = prompt["generate_new_clean_data_csv"], prompt["message_interval"], prompt["max_triggers"]
                print()
                print(self._whatsapp_execution_instance.time_calc(message_interval, max_triggers))

                if generate_new_clean_data_csv == "y":
                    time.sleep(GLOBAL_TIMER)
                    print()
                    self._csv_prompt_handler.generate_clean_csv_files_from_apify()
                else:
                    time.sleep(GLOBAL_TIMER)
                    print()
                    print("Nenhuma tabela nova foi gerada")

                time.sleep(GLOBAL_TIMER)
                print()
                print("Intervalo definido entre mensagens de {} segundos".format(message_interval))
                print("Aguarde pois o disparo começará em breve")

                df_manager = DfManager()
                clean_data = df_manager.open_data("/files/clean/", dtype={"send_cid": str})
                clean_data["cid"] = clean_data["cid"].apply(lambda x: int(float(x)))

                time.sleep(5)

                # List with columns to be saved on final DataFrame
                mobile_bool_list = []

                # True counter to be used inside for loop
                num = 0

                # Removing rows without phone numbers
                filtered_data = clean_data[clean_data['phone'].notna() & (clean_data['phone'].str.startswith(BRAZIL_PREFIX) | pd.isnull(clean_data['phone']))]
                # Removing all prospects that already received messages
                # dtype={"send_cid": str} makes column send_cid to be read as strings to prevent data corruption
                sent = df_manager.open_data("/files/sent/", selected_file="sent.csv", dtype={"send_cid": str})
                list_sent_cids = []
                file_exists = False
                if not isinstance(sent, pd.DataFrame):
                    print("Arquivo com prospectos enviados ainda não existe")
                else:
                    file_exists = True
                    # dtype={"send_cid": str} turned send_cid as strings but they need to be integers
                    sent["send_cid"] = sent["send_cid"].apply(lambda x: int(float(x)))
                    sent["send_cid"] = sent["send_cid"]
                    list_sent_cids = sent["send_cid"].to_list()
                    print("Extraindo prospectos que já receberam mensagem")
                    filtered_data = filtered_data[~filtered_data["cid"].isin(list_sent_cids)]

                sender = Sender()
                print()
                print("Iniciado disparos...")
                print("Enviando requisições para a rota: {}".format(sender.api_url))
                print()
                time.sleep(GLOBAL_TIMER)
                #time.sleep(100000)

                #DIVIDER = "____________________________________________________________________________________"
                for index, row in filtered_data.iterrows():
                    current_trigger = num + 1

                    # Identifies if number is mobile or not
                    split_number = row["phone"].split()
                    is_mobile = split_number[2].startswith("9")

                    if is_mobile:
                        print()
                        print(DIVIDER)
                        print()
                        mobile_bool_list.append(self._whatsapp_execution_instance.bool_number(is_mobile))

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
                        print(VARIANTS[num % len(VARIANTS)])
                        print()
                        #body = sender.body_formatter(phone=clean_phone, message=self._whatsapp_execution_instance.set_message(VARIANTS[num % len(VARIANTS)]))
                        #resp = sender.wpp_send_message(body)

                        if True: # resp.status_code == 200:
                            next_df = {
                                "send_cid": [cids],
                                "names": [names],
                                "number": [number],
                                "is_mobile": [mobile_bool],
                                "time": [dates],
                            }
                            if file_exists:
                                sent = df_manager.open_data("/files/sent/", selected_file="sent.csv")
                            sent_cids = pd.DataFrame(next_df)
                            final_sent_cids = pd.concat([sent, sent_cids])
                            print()
                            print("Salvando dados do prospecto")
                            # float_format="%.0f" i used to prevent the column send_cid to be saved without any data changes
                            df_manager.save_file_data(final_sent_cids, "/files/sent/sent.csv", float_format="%.0f")
                            file_exists = True

                            # Terminal messages to inform the user
                            print("Disparo n {}: Mensagem enviado para empresa {} no Whatsapp {} / {}.".format(current_trigger, row["title"], row["phone"], int(row["phoneUnformatted"])))
                            print("Fixed phone number: {}".format(row["phone"]))
                        else:
                            print("ERRO: Disparo n {} para empresa {} não foi possível no número {} / {}.".format(current_trigger, row["title"], row["phone"], int(row["phoneUnformatted"])))

                        if current_trigger != max_triggers:
                            print("Aguarde próximo envio em {} segundos".format(message_interval))
                        if current_trigger >= max_triggers:
                            time.sleep(3)
                            print()
                            print(DIVIDER)
                            print()
                            print("Execução finalizada")
                            break
                        time.sleep(message_interval)

                        num += 1
                    else:
                        continue
                    
                end_time = time.time()
                self._whatsapp_execution_instance.print_execution_time(start_time, end_time)

                percentage = (sum(mobile_bool_list) / len(mobile_bool_list)) * 100
                print()
                print("Total de números de celular (%): {}%".format(percentage))
                print()

