# PySpark Training Project

## Setup Instructions

# 1. Clone the repo
git clone https://github.com/REAL_USERNAME/pyspark-training.git
cd pyspark-training

# 2. Create a virtual environment (Python 3.11 recommended)
python -m venv .venv
 
# 3. Activate the virtual environment
#Windows:
.venv\Scripts\activate

# Linux/macOS:
source .venv/bin/activate


# Install dependencies
pip install -r requirements.txt
Run the code
python test/test.py


# Notes
Requires Java 11 or 17 installed for PySpark
Make sure Python 3.11 is used

---
# How someone else use my repo

Step by step:

```powershell
git clone https://github.com/REAL_USERNAME/pyspark-training.git
cd pyspark-training
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python test/test.py


# Generate requirement.txt from your virtual environment:

pip freeze > requirements.txt


#Db conig.

Created .env file for the Db configuration (host,user,password etc)
and installed pip install python-dotenv
dbConfig.py returns all the required details about Db from .env file to the sql connection script (you have to import dbconfig file)



