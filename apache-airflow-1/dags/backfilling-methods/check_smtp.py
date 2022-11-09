import datetime
import smtplib
import time

smtp_server = 'localhost'
creds = {'service_acct': 'a', 'service_acct_pwd': 'c'}

class SmtpConnection():
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SmtpConnection, cls).__new__(cls)
            cls._instance._create_connection()
        return cls._instance

    def _create_connection(self):
        self.connection = smtplib.SMTP(smtp_server, 587)
        self.connection.starttls()
        self.connection.login(creds.service_acct, creds.service_acct_pwd)
        log.info("Connected to SMTP server")

    @property
    def smtp_connection(self):
        try:
            status = self.connection.noop()[0]
        except:  # smtplib.SMTPServerDisconnected
            status = -1

        # Why 250? ==> https://en.wikipedia.org/wiki/List_of_SMTP_server_return_codes
        if status != 250:
            self._create_connection()
        return self.connection

class SmtpConnection2():
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SmtpConnection2, cls).__new__(cls)
            cls._instance._create_connection()
        return cls._instance

    def _create_connection(self):
        self.connection = datetime.datetime.now()

    @property
    def smtp_connection(self):
        return self.connection



con1 = SmtpConnection2()
print(con1)
print(con1.smtp_connection)
time.sleep(5)
con2 = SmtpConnection2()
print(con2)
print(con2.smtp_connection)