# Import necessary modules

import sys
import os
import pyodbc
import pandas as pd
import logging
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders
import config  # Import the query from config.py
import dateutil.relativedelta

# Get the current date
now = datetime.now()

### Database related code ###
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler('log_document.log')
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(file_handler)

# Unpack dictionary credentials, test if credentials are valid
try:
    cnxn = pyodbc.connect(
        f'DRIVER={config.DRIVER};SERVER={config.SERVER};DATABASE={config.DATABASE};UID={config.UID};PWD={config.PWD}')
    cursor = cnxn.cursor()
    logger.info("Successfully connected to the database.")
except pyodbc.Error as err:
    logger.error("Script exiting. Something went wrong: {}".format(err))
    sys.exit(0)
### End of Database Related Code ###

# Your SQL query
query = config.queryEmailreportmonthly

# Determine the previous month
previous_month = now - dateutil.relativedelta.relativedelta(months=1)

# Define the file_date as the previous month in the format 'CCYYMM'
file_date = previous_month.strftime('%Y%m')

# Define the email recipients
toAddr = ['rakesh.vinnakollu@telus.com']  # to: recipients
ccAddr = ['Bhaktavatsalam.Pamidipati@telus.com','varshitha.tl@telus.com']  # cc: recipients

# Execute the SQL query and store the result in the df dataframe
df = pd.read_sql(query, cnxn)

# Check if the CABS revenue report is not empty
if not df.empty:
    # Create an Excel file
    file_name = f'TELUS Mobility CABS revenue report (for trading partner JE) {file_date}.xlsx'
    with pd.ExcelWriter(file_name) as writer:
        df.to_excel(writer, 'Sheet 1', merge_cells=False, index=False)
else:
    print('Detected empty dataframe. Now exiting script.')
    sys.exit(0)

# Define the make_mail function for creating the email
def make_mail():
    msg = MIMEMultipart()
    msg['From'] = '"rakesh.vinnakollu@telus.com" <noresponse@telus.com>'
    msg['To'] = ", ".join(toAddr)
    msg['Cc'] = ", ".join(ccAddr)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = f'TELUS Mobility CABS revenue reports for {file_date}'
    msg.attach(MIMEText('TELUS Mobility CABS revenue report to complete the trading partner JE.'))
    part = MIMEBase('application', "octet-stream")
    part.set_payload(open(file_name, 'rb').read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename="%s"' % file_name)
    msg.attach(part)

    return msg

# Define the send_mail function
def send_mail(msg):
    try:
        smtp = smtplib.SMTP('abmsg.tsl.telus.com', 25)
        smtp.set_debuglevel(1)
        smtp.ehlo()
        smtp.starttls()
        smtp.sendmail(msg['From'], toAddr + ccAddr, msg.as_string())
        smtp.quit()
    except smtplib.SMTPException as err:
        logger.error(f"{err}.")
        file_handler.close()
        sys.exit(0)

# Read the first line of the log file
try:
    with open('emailTrackerFile.txt', 'r+') as f:
        last_send_date = f.readline()

        # Determine the substrings to check against the file's first row
        check_dates = [file_date]

        if any(d in last_send_date for d in check_dates):
            print('The report for this month has already been sent.')
            sys.exit(0)
        elif os.path.isfile(file_name):
            msg = make_mail()
            send_mail(msg)
            print('The report has been sent to all recipients.')
            f.seek(0)
            f.truncate()
            f.write(f'CCYYMM of last report sent: {file_date}')
        else:
            print('No file detected.')
            sys.exit(0)
except IOError:
    f = open('emailTrackerFile.txt', 'w+')
    f.close()
    print('Could not find emailTrackerFile.txt, now generating file. Please rerun script.')
