# coding=utf-8

# =========================================================================================================
# Created By  : Harisha P
# Created Date: 19 OCT 2020
# FILENAME: send_email.py
# =========================================================================================================
"""     The Script Has Been Build  to send email notification     """
# =========================================================================================================
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def send_email(mail_subject: str, is_html: bool, message_body: str, to_address: list):
    """To send email notification

    Args:
        subject (str): Subject of the Email.
        is_html (bool): is the content of the email is HTML or Not.
        message_body (str): Body content of HTML.
        to_address (list): list of email address, email to be sent.
    """
    SMTP_SERVER = "smtpgw.pg.com"
    SMTP_PORT = 25
    SMTP_SENDER = "datalabalerts.im@pg.com"

    message = MIMEMultipart()
    message["To"] = ', '.join(to_address)
    message["From"] = SMTP_SENDER
    message["Subject"] = mail_subject
    if is_html:
        message.attach(MIMEText(message_body, 'html'))
    else:
        message.attach(MIMEText(message_body, 'plain'))
    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.sendmail(SMTP_SENDER, to_address, message.as_string())
    except Exception as ex:
        template = "An exception of type {0} occurred in send_email.py(send_email). Arguments:\n{1!r}"
        error_message = template.format(type(ex).__name__, ex.args)
        print(error_message)
    finally:
        server.quit()


if __name__ == '__main__':
    subject = "Test Subject!"
    html_content = False
    message_text = "Test Message"
    to_email_address = ["puttaswamy.hp@pg.com"]

    send_email(mail_subject=subject, is_html=html_content,
               message_body=message_text, to_address=to_email_address)
