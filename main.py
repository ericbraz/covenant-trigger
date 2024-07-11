from config import DIVIDER
from scripts.execution import Execute

def main():
    print("Entre as opções abaixo qual você deseja executar?")
    print(" 1 - Gerar arquivo CSV para a plataforma de Email marketing")
    print(" 2 - Disparar mensagens no whatsapp")
    print(" 3 - Gerar lista para enviar mensagens no whatsapp via link")
    choice = input("Informe o número da execução desejada: ")
    print()
    print(DIVIDER)
    print()

    execute = Execute()
    if choice == "1":
        execute.csv_for_email_marketing_execution()
    elif choice == "2":
        execute.whatsapp_trigger_execution()
    elif choice == "3":
        execute.whatsapp_flask_execution()
    else:
        print("Opção informada não existe.")

if __name__ == "__main__":
    main()
