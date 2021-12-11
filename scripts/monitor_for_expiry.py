#!/usr/bin/env python3

'''This python script monitors X509 certificates for expiration.'''

import sys
import os
import subprocess
import datetime
import smtplib
from email.mime.text import MIMEText

#CERTS_TO_CHECK = ["/tmp/x509up_u33527", "/tmp/x509up_u456264", "/tmp/x509up_u7588"]
CERTS_TO_CHECK = ["/home/shock-ssd/scottcal/condor/x509up_u7588"]
WARNING_HOURS = 24
EMAIL_TO = "scottcal@usc.edu"


def send_email(cert, time_remaining):
    if time_remaining > datetime.timedelta(0):
        body = "Certificate %s will expire in %d hours." % (
            cert, int(time_remaining.days * 24 + time_remaining.seconds/3600))
        subject = "certificate %s expiring" % cert
    else:
        body = "Certificate %s has expired." % (cert)
        subject = "certificate %s expired" % cert

    body = body.replace(" ", "\ ")
    subject = subject.replace(" ", "\ ")
    # SSH into strike to send email
    cmd = 'ssh -i /home1/scottcal/.ssh/id_rsa scottcal@strike.scec.org /home/scec-00/scottcal/send_email.py --subject "%s" --body "%s"' % (subject, body)
    os.system(cmd)
    '''if time_remaining>datetime.timedelta(0):
                msg = MIMEText("Certificate %s will expire in %d hours." % (cert, int(time_remaining.days * 24 + time_remaining.seconds/3600)))
                msg['Subject'] = "certificate %s expiring" % cert
        else:
                msg = MIMEText("Certificate %s has expired." % (cert))
                msg['Subject'] = "certificate %s expired" % cert
        msg['From'] = EMAIL_TO
        msg['To'] = EMAIL_TO
        s = smtplib.SMTP('localhost')
        s.sendmail(msg['To'], [msg['From']], msg.as_string())
        s.quit()'''


for cert in CERTS_TO_CHECK:
    print("Checking validity of certificate %s" % cert)
    p = subprocess.Popen(["openssl", "x509", "-enddate",
                         "-in", cert, "-noout"], stdout=subprocess.PIPE)
    expiry_string = p.communicate()[0].decode('utf-8').split("=")[1]
    # Jan 17 21:23:33 2014 GMT
    expiry_dt = datetime.datetime.strptime(
        expiry_string.strip(), "%b %d %H:%M:%S %Y %Z")
    now = datetime.datetime.utcnow()
    time_remaining = expiry_dt - now
    if time_remaining < datetime.timedelta(hours=WARNING_HOURS) and time_remaining > datetime.timedelta(0):
        print("Certificate %s expires in %d hours, sending email." % (cert, int(time_remaining.days * 24 + time_remaining.seconds/3600)))
        send_email(cert, time_remaining)
    elif time_remaining < datetime.timedelta(0):
        print("Certificate %s has expired, sending email." % (cert))
        send_email(cert, time_remaining)
    else:
        print("Certificate %s still valid for %d hours." % (cert, int(time_remaining.days*24 + time_remaining.seconds/3600)))
