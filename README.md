This guide assumes you have at least Python 3.11 installed

####MacOS/Linux####
Open a terminal window and enter the following:
mkdir stocklens
cd stocklens
python -m venv venv
source venv/bin/activate
python -m pip install dash==2.8.1 pandas==1.5.3
pip install sqlalchemy
pip install pymysql

Next, we need to pull the source code from Github
via SSH:
git pull git@github.com:Zac-Fidler/datasystems.git

via HTTPS: 
git pull https://github.com/Zac-Fidler/datasystems.git

This may take a few moments…

Install xampp
https://www.apachefriends.org/download.html 

Start the Apache and MySQL servers
Go to “localhost/phpmyadmin”
Click import in the top navigation bar
Choose warehouse.sql which should be have been pulled from github

We then need to perform the ETL procedure (loads onto local sql)
python etl.py

This may take a few moments…
When complete it will inform you in the terminal
Finally start the application
 python app.py

The webapp can be accessed at localhost:8050
And can be stopped by typing ctrl+c

####Windows####
mkdir stocklens
cd stocklens
python -m venv venv
venv\Scripts\activate
python -m pip install dash==2.8.1 pandas==1.5.3
pip install sqlalchemy
pip install pymysql

Next, we need to pull the source code from Github
via SSH:
git pull git@github.com:Zac-Fidler/datasystems.git

via HTTPS: 
git pull https://github.com/Zac-Fidler/datasystems.git

This may take a few moments…

Install xampp
https://www.apachefriends.org/download.html 

Start the Apache and MySQL servers
Go to “localhost/phpmyadmin”
Click import in the top navigation bar
Choose warehouse.sql which should be have been pulled from github

We then need to perform the ETL procedure
python etl.py

This may take a few moments…
When complete it will inform you in the terminal
Finally start the application
 python app.py

The webapp can be accessed at localhost:8050
And can be stopped by typing ctrl+c

