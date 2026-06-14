"""
Module for generating HTML email content for ETL job status updates.
"""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, parseaddr
from typing import Union, List


class MailSender:
    """
    Class to handle secure email sending using SMTP with context management.
    """
    def __init__(
        self, 
        email_host: str, 
        email_port: int, 
        email_user: str, 
        email_password: str, 
        email_use_tls: bool = True, 
        email_use_ssl: bool = False, 
        email_from: str = None
    ):
        """
        Initialize the MailSender instance.

        :param email_host: The SMTP server host address.
        :param email_port: The port number for the SMTP server.
        :param email_user: The username/email for SMTP authentication.
        :param email_password: The password/app password for SMTP authentication.
        :param email_use_tls: Flag to enable Transport Layer Security (TLS).
        :param email_use_ssl: Flag to enable Secure Sockets Layer (SSL).
        :param email_from: Optional custom sender identity string (e.g., "Name <email@domain.com>").
        """
        self.email_host = email_host
        self.email_port = int(email_port)
        self.email_user = email_user
        self.email_password = email_password
        self.email_use_tls = email_use_tls
        self.email_use_ssl = email_use_ssl
        
        # Parse friendly name and email address from the custom sender configuration
        self.email_sender_name, self.email_sender_address = parseaddr(email_from)
        
        if not self.email_sender_address:
            self.email_sender_address = email_user
            
        self.session = None
    
    def __enter__(self):
        """
        Establish an authenticated SMTP session when entering the 'with' context block.

        :return: The initialized MailSender instance.
        """
        print(f"Setting up the mail session {self.email_host}:{self.email_port}...")
        
        if self.email_use_ssl:
            self.session = smtplib.SMTP_SSL(self.email_host, self.email_port)
        else:
            self.session = smtplib.SMTP(self.email_host, self.email_port)
        
        if self.email_use_tls:
            self.session.starttls()
        
        self.session.login(self.email_user, self.email_password)
        print("Mail session launched and authenticated...")
        
        return self
    
    def _to_list(self, addresses: Union[str, List[str]]) -> List[str]:
        """
        Helper method to normalize single string email addresses or lists into a list.
        """
        if not addresses:
            return []
        if isinstance(addresses, str):
            return [addr.strip() for addr in addresses.split(',') if addr.strip()]
        return [addr.strip() for addr in addresses if addr.strip()]

    def send_mail(
        self, 
        receiver_address: Union[str, List[str]], 
        subject: str, 
        body_html: str, 
        attachments: list = None, 
        cc_addresses: Union[str, List[str]] = None, 
        bcc_addresses: Union[str, List[str]] = None
    ):
        """
        Construct and send an HTML email message, with optional file attachments.

        :param receiver_address: The email address(es) of the primary recipient(s). Can be a string or list.
        :param subject: The subject line of the email.
        :param body_html: The HTML-formatted body content of the email.
        :param attachments: Optional list of MIME base instances to append to the email.
        :param cc_addresses: Optional email address(es) to include in the carbon copy (CC).
        :param bcc_addresses: Optional email address(es) to include in the blind carbon copy (BCC).
        """
        if not self.session:
            raise RuntimeError("SMTP session is not active. Use the 'with' block context.")
        
        # Normalize all inputs into clean lists of strings
        to_list = self._to_list(receiver_address)
        cc_list = self._to_list(cc_addresses)
        bcc_list = self._to_list(bcc_addresses)

        if not to_list:
            raise ValueError("At least one primary receiver_address must be provided.")

        # Setup the MIME multipart message container
        message = MIMEMultipart()
        message['From'] = formataddr((self.email_sender_name, self.email_sender_address))
        message['To'] = ', '.join(to_list)
        
        if cc_list:
            message['Cc'] = ', '.join(cc_list)
        if bcc_list:
            message['Bcc'] = ', '.join(bcc_list)
            
        message['Subject'] = subject
        
        # Process and append any attached files
        if attachments:
            for attachment in attachments:
                message.attach(attachment)

        # Attach the primary HTML body content
        message.attach(MIMEText(body_html, 'html'))
        text = message.as_string()

        # SMTP sendmail requires a flat list of all envelope recipients (To + CC + BCC)
        all_recipients = to_list + cc_list + bcc_list

        self.session.sendmail(self.email_sender_address, all_recipients, text)
        print(f'Mail Sent to {message["To"]} with subject "{subject}".')
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Gracefully terminate the SMTP session when exiting the 'with' context block.
        """
        if self.session:
            self.session.quit()
            print("Mail session closed.")


##################################################################################################
## CONSTANTS
##################################################################################################
MAIL_CONTENT = """
<!DOCTYPE html>
<html>
<head>
    <style>
        table {{
            font-family: Arial, sans-serif;
            border-collapse: collapse;
            width: 100%;
        }}
        th, td {{
            border: 1px solid #dddddd;
            text-align: left;
            padding: 8px;
        }}
        th {{
            background-color: #f2f2f2;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
    </style>
</head>
<body>
Bonjour à tous,<br>
<br>
L'exécution du job <strong>{etl_name}</strong> est terminée.<br>
<br>
Ci-dessous le statut de mise à jour des tables:
<br>
<table>
    <tr>
        <th>Check Date</th>
        <th>Table ID</th>
        <th>Table Name</th>
        <th>Last Date</th>
        <th>Load Date</th>
        <th>Status</th>
        <th>Missing Dates</th>
    </tr>
    {html_table}
</table>

<br>
Bonne réception.<br>
Big Data & Customer Analytics
<hr>
<i>Attention : Ce mail a été généré automatiquement.</i>
</body>
</html>
"""
##################################################################################################

def generate_mail(etl_name, html_table, html_template=MAIL_CONTENT):
    """
    Generate the HTML content for the email.
    
    Args:
        html_template (str): The HTML template for the email.
        etl_name (str): The name of the ETL process.
        
    Returns:
        str: The generated HTML content.
    """
    return html_template.format(etl_name=etl_name, html_table=open(html_table, "r").read())


