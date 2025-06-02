from sqlalchemy import create_engine
import sqlite3

def get_conn():
    return sqlite3.connect("banco_bagre.db")

def get_engine():
     return create_engine('sqlite:///banco_bagre.db')