from pyspark.sql.types import *
from pyspark.sql import functions as F
from datetime import datetime,date,timedelta
from databricks.sdk.runtime import *
from pyspark.sql.utils import AnalysisException
import os
import csv
import builtins
import shutil
import requests
import json
import collections
import numpy as np
from  datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import DataFrame
from typing import List, Union
import pytz
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from pyspark.sql import SparkSession
from email import encoders
import pandas as pd
from IPython.display import HTML, display
from pyspark.sql.functions import *
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor


def sending_chat_message_to_google_chats(title,description=None,path=None):
    if title and (description is None and path is None):
        payload = {
        "text" : f"✅ {title}"
    }
    else:

        payload = {
            "cards": [
                {
                    "header": {
                        "title": "Databricks Job Notification",
                        "subtitle": f'❌ {title}'
                    },
                    "sections": [
                        {
                            "widgets": [
                                {
                                    "keyValue": {
                                        "topLabel": "𝐃𝐞𝐬𝐜𝐫𝐢𝐩𝐭𝐢𝐨𝐧",
                                        "content": f'{description}' or "No description provided"
                                    }
                                },
                                {
                                    "keyValue": {
                                        "topLabel": "𝐍𝐨𝐭𝐞𝐛𝐨𝐨𝐤 𝐏𝐚𝐭𝐡",
                                        "content": path or "No path provided"
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    try:
        webhook_url = dbutils.secrets.get(scope="google_chat", key="dl_notification")
        response = requests.post(webhook_url,data=json.dumps(payload),headers=headers)
        return response.status_code
    except Exception as e:
        return str(e)



def sending_text_message(title):
    payload = {
        "text" : f"{title}"
    }
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    try:
        webhook_url = dbutils.secrets.get(scope="google_chat", key="dl_notification")
        response = requests.post(webhook_url,data=json.dumps(payload),headers=headers)
        return response.status_code
    except Exception as e:
        return str(e)


def create_tabular_card_readable(df, title, limit_rows=5):
    print("running latest code")
    rows = df.limit(limit_rows).toPandas()
    print("Rows printing  ",rows)
    headers = rows.columns
    print("Headers ",headers)
    data_rows = rows.astype(str).values.tolist()
    #data_rows = [[str(getattr(r, c)) for c in headers] for r in rows]
    print("Data rows ",data_rows)
    # col_widths = [
    #     max(len(headers[i]), max(len(row[i]) for row in data_rows))
    #     for i in range(len(headers))
    # ]
    col_widths=[]
    for i in range(len(headers)):
        print("Looping ",i)
        max_len = len(headers[i])
        for row in data_rows:
            max_len = builtins.max(max_len,len(row[i]))
        col_widths.append(max_len)
    print("Column width",col_widths)
    def fmt_row(row):
        return " | ".join(row[i].ljust(col_widths[i]) for i in range(len(row)))

    header_line = fmt_row(headers)
    print("code come here")
    separator = " | ".join("-" * col_widths[i] for i in range(len(col_widths)))
    data_lines = [fmt_row(row) for row in data_rows]
    table_text = f"{title}\n\n" + "\n".join([header_line, separator] + data_lines)
    print("Table text data ",table_text)
    payload = {
        "text": f"```\n{table_text}\n```"
    }
    print("Payload ",payload)
    headers_req = {'Content-Type': 'application/json; charset=UTF-8'}
    webhook_url = dbutils.secrets.get(scope="google_chat", key="dl_notification")
    res = requests.post(webhook_url, json=payload,headers=headers_req)
    return res.status_code


def convert_df_to_html(df,title):
    pdf = df.toPandas()
    html_table = pdf.to_html(index=False,border=0)
    styles_html = f"""
    <html>
    <head>
    <style>
    table {{
      border-collapse: collapse;
      font-family: Arial;
      width: 100%;
    }}
    th {{
      background-color: #2F80ED;
      color: white;
      padding: 8px;
    }}
    td {{
      padding: 8px;
      border-bottom: 1px solid #ddd;
    }}
    tr:nth-child(even) {{background-color: #f2f2f2;}}
    </style>
    </head>
    <body>
    <h2>{title}</h2>
    {html_table}
    </body>
    </html>
    """
    return styles_html

def send_dataframe_in_body(html_content, sender_email, receiver_email, cc_emails=None, subject=None, message_body=None, dataframe=None):
    if sender_email=='mxnagri@gmail.com':
        sender_email='mxnagori@gmail.com'
    else:
        sender_email

    # Create MIME object
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    if cc_emails is not None:
        msg['Cc'] = ', '.join(cc_emails) if cc_emails else None

    # Attach HTML message to the email
    msg.attach(MIMEText(html_content, 'html'))
     
    # Get email app password from secrets
    password = dbutils.secrets.get(scope='kv-cenomi',key='datalakeemailpassword')

    # Create SMTP session for sending the mail
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(sender_email, password)
        server.send_message(msg)

