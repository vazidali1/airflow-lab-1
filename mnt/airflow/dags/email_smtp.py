
# Import modules
import smtplib, ssl
class mail_request:
   def email_notity(email_to,email_string):
  #  Please replace below with your email address and password
    email_from = 'vazidalikhan13@gmail.com'
    password = 'grrlqtfmenfzgskr'
    email_to = 'vazidalikhan13@gmail.com'

  # Plain Text string as the email message
    email_string = 'This is a test email sent by Python code.'

  # Connect to the Gmail SMTP server and Send Email
  # Create a secure default settings context
    context = ssl.create_default_context()
  # Connect to Gmail's SMTP Outgoing Mail server with such context
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
  # Provide Gmail's login information
     server.login(email_from, password)
  # Send mail with from_addr, to_addrs, msg, which were set up as variables above
    server.sendmail(email_from, email_to, email_string)

import smtplib

class email_notification:
    
    gmail_user = 'mailto:vazidalikhan13@gmail.com'
    gmail_password = 'zemyfhqdqqlgwovf'
        
        
    def email_properties(self):
        
        self.sent_from = self.gmail_user
        self.to = ['mailto:vazidalikhan13@gmail.com']
        self.subject = 'python code'
        
    def email_msg(self,msg=''):
        self.email_properties()
        self.msg = msg
        return msg
        
    def email_send_requests(self,enter_msg):
        self.email_msg(enter_msg)
        try:
            server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
            server.ehlo()
            server.login(self.gmail_user, self.gmail_password)
            server.sendmail(self.sent_from,self.to,self.msg)
            server.close()  
            print ('Email sent!')
            
        except Exception as e:
            print(e)
            print ('Something went wrong ...')
                        
email_notify =email_notification()
# email_notify.email_send_requests()





