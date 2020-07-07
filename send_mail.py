import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
import datetime 
import setting

# Receiver list 
receiver = setting.mail_notice_observers

# Gmail ssl port
port = 465  

# Gmail smtp server
smtp_server = "smtp.gmail.com"  

# Sender's mail address and password
sender = "dev.noel.c@gmail.com"
password = "******"

# Create MIME message object
# Add Subject, From, To information
# Attach html info and attachment info
today = datetime.datetime.today()
message = MIMEMultipart("alternative")
message["Subject"] = "friDay shopping app events' summary: {0}".format(str(today.year)+'/'+str(today.month)+'/'+str(today.day))
message["From"] = sender
message["To"] = ','.join(receiver)

# Get attachment info from setting 
# Set attached image to message object
htmlAttachInfoStr = ''
for chart in setting.get_mail_notify_attachments():
    htmlAttachInfoStr += '<img src="cid:{0}" width="800"><br>'.format(chart)
    with open(chart, 'rb') as image_file:
        image = MIMEImage(image_file.read())
        image.add_header('Content-ID', '<{0}>'.format(chart))
    message.attach(image)


# Set html content
html = """
<html>
    <body>
        <p>Hi All</p>
        <p>friDay shopping app's purchase event summary as below</p>
        <p>For more events' summary, click <a href='mytesthosting20200630.firebaseapp.com'>here
        <br>
        {0}
    </body>
</html>
""".format(htmlAttachInfoStr)
part = MIMEText(html, "html")
message.attach(part)

# Generate SSL connection, login sender gmail and send message
# Message needs to be converted to string format
context = ssl.create_default_context()
with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
    server.login(sender, password)
    server.sendmail(sender, receiver, message.as_string())