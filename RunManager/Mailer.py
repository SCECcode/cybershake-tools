#!/usr/bin/env python

import sys
import socket
import os
import pwd
import smtplib


class Mailer:
    smtphosts = []
    from_user = ""

    def __init__(self):

        # Get the current user id
        userid = pwd.getpwuid(os.getuid())[0]
        domain = socket.getfqdn()
        self.from_user = userid + "@" + domain

        # Create list of possible SMTP servers
        self.smtphosts.append('localhost')
        comps = domain.split('.', 1)
        if (len(comps) > 1):
            althost = 'smtp.' + comps[1]
            self.smtphosts.append(althost)
        althost = 'smtp.' + domain
        self.smtphosts.append(althost)

        return

    def send(self, to_user, subject, msg):
        to_str = ""
        if (type(to_user) == type([])):
            for n in to_user:
                if (to_str == ""):
                    to_str = n
                else:
                    to_str = to_str + "," + n 
        else:
            to_str = str(to_user)

        msg_str = "From: " + self.from_user + \
            "\r\nTo: " + to_str + \
            "\r\nSubject: " + subject + \
            "\r\n" + msg + \
            "\r\n-------------------------------------------------------" + \
            "\r\nAutomated msg from CyberShake Mailer\r\n"
    
        for h in self.smtphosts:
            try:
                print "Connecting to SMTP host " + h
                server = smtplib.SMTP(h)
                #server.set_debuglevel(1)
                server.sendmail(self.from_user, to_str, msg_str)
                server.quit()
                return 0
            except:
                print sys.exc_info()
                print "Unable to send notification via host " + h

        # Exhausted all possible smtp hosts
        return 1


