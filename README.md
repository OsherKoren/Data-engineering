# Data-engineering

**Reauirements:
**Attached is a requirements.txt for more convenience.**

Python version 3.9.5

The required packages are:

configparser==5.0.2

greenlet==1.1.0

Mako==1.1.4

MarkupSafe==2.0.1

numpy==1.21.0

pandas==1.3.0

pyodbc==4.0.31

python-dateutil==2.8.1

python-editor==1.0.4

pytz==2021.1

six==1.16.0

SQLAlchemy==1.4.20

For printing the DataFrame in a more convenience way please add to Operations file the following lines:

desired_width=320

pd.set_option('display.width', desired_width)

np.set_printoptions(linewidth=desired_width)

pd.options.display.max_colwidth = 100

pd.set_option('display.max_columns', 10)


## Guide for setup:
Attached is a db_config.ini file with the DB credentials (configuration).
The configuration is for connecting remotely to my local computer(server).
The program was tested with a different remote computer.
DB - MS SQL SERVER
The program is set to connect to a remote server by default in the DB file and class
with self.conf = Conf(self.logger, local=False)
Should you choose to test the program in your local computer, 
below is the setup steps.

Setup steps for changing the current default configuration to local connection:
1. Change the file local_db_config.ini with your local parameters:

   driver
   
   server name

Any change of configuration should be applied in this file only.

2. Create a new database with the name: AdsDB on your local MS SQL SERVER.
3. In DB.py file in DB __init__ constructor , change the default variable from local=False to local=True 

##The Data
Random ads data was constructed in the Operations.py file with the method: set_ads_table.

##Description
The program performs 3 operations of sorting the ads as describes below:
1) Standard sorting. 
2) K way sorting with chunks. 
3) Multiprocessing with chunks.

The queries are presented in the DB.py file in DB class methods. 
For more convenience, the results were also loaded to a csv file. 
Attached is also a log file presenting some trials

## How to run the program
Run main.py file
