import os
from flask import Flask, render_template, redirect, url_for, request
import webbrowser
import threading
import signal
from scripts.data_cleaner.df_manager import DfManager
from scripts.sender.message_sender import Sender
from config import VARIANTS

PID = os.getpid()
app = Flask(__name__)

# Routes

@app.route("/")
def home():
    df_manager = DfManager()
    dataframe = df_manager.open_data("/files/sent/", selected_file="sent_temp.csv")
    size = dataframe.shape[0]

    sender = Sender()
    whatsapp_link = [
        sender.whatsapp_link_formatter(whatsapp=phone, variant_text=VARIANTS[idx % len(VARIANTS)])
        for idx, phone in enumerate(dataframe["number"])
    ]
    dataframe["whatsapp_link"] = whatsapp_link

    data_list = dataframe.to_dict(orient="records")

    data = {
        "title": "WhatsApp List",
        "message": f"Lista contém {size} links para Whatsapp",
        "data": data_list,
    }

    return render_template("index.html", data=data)

@app.route("/execute_script")
def execute_script():
    os.system("python3 update_sent_from_wpp_links.py")
    return redirect(url_for("close_window"))

@app.route("/close_window")
def close_window():
    return render_template("close_window.html")

@app.route("/shutdown", methods=["POST"])
def shutdown():
    shutdown_server()
    return "Server shutting down..."

# Other scripts

def shutdown_server():
    # this mimics a CTRL+C hit by sending SIGINT
    # it ends the app run, but not the main thread
    pid = os.getpid()
    assert pid == PID
    os.kill(pid, signal.SIGINT)
    return "OK", 200

# Flag to check if the browser has already been opened
browser_opened = False

def open_browser():
    global browser_opened
    if not browser_opened:
        webbrowser.open("http://localhost:5000/")
        browser_opened = True

if __name__ == "__main__":
    threading.Timer(1, open_browser).start()
    app.run(debug=True)
