#!/bin/bash

# crontab command
# */5 * * * * cd ~/Documents/PythonProject/DataScience && ./my_script.sh

# Declare project path
homeDir=~/Documents/PythonProject/DataScience
dataAnalysisDir=~/Documents/PythonProject/DataScience
webPageDir=~/Documents/MySampleCode/MyHostingSample

# Set PATH explicitly
export PATH=/usr/local/opt/ruby/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin

# Change folder to data analysis and 
cd $dataAnalysisDir

# Run Untitled.ipynb to update chart and output to html
jupyter nbconvert --ExecutePreprocessor.timeout=600 --execute Events_Dashboard.ipynb

# Rename html file and move to hosting folder
mv Events_Dashboard.html $webPageDir/public/index.html

# Change folder to hosting project and deploy
cd $webPageDir
firebase deploy

# Send mail to inform users
cd $homeDir
python3 send_mail.py