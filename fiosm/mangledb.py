'''
Created on 15.11.2012

@author: Scondo

StreetMangler with some db addition
'''
from config import connstr
import psycopg2

conn=None
guess_tbl="mangle_guess"

db=None
usable=False

def InitMangle(no_guess=True):
    global conn,usable,db
    try:
        import streetmangler
        conn=psycopg2.connect(connstr)
        conn.autocommit=True
        locale=streetmangler.Locale('ru_RU')
        db = streetmangler.Database(locale)

        db.Load("ru_RU.txt")
        for guess in ListGuess(all_=not(no_guess)):
            db.Add(guess)

        usable=True
    except:
        usable=False
        
def ListGuess(all_=False):
    """Get guess for streetmangle from sql db
    """
    cur_=conn.cursor()    
    if all_:
        cur_.execute("SELECT name FROM "+guess_tbl)
    else:
        cur_.execute("SELECT name FROM "+guess_tbl+" WHERE valid=1")
    for row in cur_.fetchall():
        yield row[0].decode('UTF-8')
        
def AddMangleGuess(name):
    if not usable:
        return
    cur_=conn.cursor()
    #Do not save twice
    cur_.execute("SELECT name FROM mangle_guess WHERE name=%s",(name,))
    if cur_.fetchone():
        return
    
    cur_.execute("INSERT INTO mangle_guess (name) VALUES (%s)",(name,))