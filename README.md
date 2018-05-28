# log analysis 
## About the project:
My project was Log analysis project and this run in python2 and connect to a database name psycopg2.
## Required Softwares:
1.Vagrant.
2.Virtualbox.
3.python2.
4.postgresql.
5.Psycopg2.
## Content files:
1.news.py.
2.output.txt.
## Process:
1.If you don't already have the latest version of python download it from the link in requirements.
2.Download Vagrant and VirtualBox.
3.Install Vagrant and VirtualBox.
4.Download the database psycopg2.
5.launch the virtual machine withvagrant up
6.Once Vagrant installs necessary files use vagrant ssh to continue.
7.The command line will now start with vagrant. Here cd into the /vagrant folder.
8.To load the database type psql -d news -f newsdata.sql
9.To run the database type psql -d news
10.You must run the commands from the Create views section here to run the python program successfully.
11.Use command python news.py to run the python program that fetches query results.
