db_host = 'localhost'
db_name = 'Test'
db_user = 'postgres'
db_pass = '1234'


from ast import Try
from fileinput import close
from sqlite3 import Cursor
import psycopg2
from parser import *




def Create_ResearchPaper_Table():
    """Create research paper if not exist"""
    sql= """ CREATE TABLE IF NOT EXISTS ResearchPapers(
                Index bigint PRIMARY KEY,
                Title TEXT,
                Main_Author TEXT default NULL,
                Abstract TEXT,
                UNIQUE(Index,Title)
                )"""
    
    sql1=""" CREATE UNIQUE INDEX uniq  ON ResearchPapers  (Index,Title)
                WHERE Main_Author IS NULL;
                 """
    try:
        conn = psycopg2.connect(host = db_host , database = db_name , user = db_user , password = db_pass) 
        cursor = conn.cursor()
        cursor.execute(sql)
        
        conn.commit()
        conn.close()
    except:
        print("Unable to create research paper table in database")

    try:
        conn = psycopg2.connect(host = db_host , database = db_name , user = db_user , password = db_pass) 
        cursor = conn.cursor()
        cursor.execute(sql1)
        
        conn.commit()
        conn.close()
    except:
        print("unique constraint already exist ignoring now")

def Insert_rp(Rp_attributes):
    """Insert data into research paper table """
    sql= """INSERT INTO ResearchPapers(Index,Title,Abstract,Main_Author)
            VALUES(%s,%s,%s,%s) 
            ON CONFLICT(Index,Title) DO NOTHING
             """
    
    try:
        conn = psycopg2.connect(host = db_host , database = db_name , user = db_user , password = db_pass) 
        cursor = conn.cursor()

        for rp in Rp_attributes:
            cursor.execute(sql,(rp[0],rp[1],rp[2],rp[3],))
        conn.commit()
        conn.close()
    except:
        print("Unable to insert into research paper table")


# research paper and its authors relation 

def create_rp_co_author():
    """ create rp co authors for research paper"""
    sql ="""CREATE TABLE IF NOT EXISTS Rp_coAuthors
            (
                index bigint REFERENCES ResearchPapers(Index) ON DELETE CASCADE ,
                co_author TEXT NOT NULL REFERENCES Authors(name),
                UNIQUE (index,co_author)  
                    
            )"""
    try:
        conn = psycopg2.connect(host = db_host , database = db_name , user = db_user , password = db_pass) 
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        conn.close()
    except:
        print("Unable to create co author table ")
    
        #print("Unable to create co_Authors")

def insert_coAuthors(co_authors):
    """insert into coAuthors research paper"""
    sql="""INSERT INTO  Rp_coAuthors(index,co_author)
            VALUES(%s,%s)
            ON CONFLICT (index,co_author) DO NOTHING 
            """
    try:
        conn = psycopg2.connect(host = db_host , database = db_name , user = db_user , password = db_pass) 
        cursor = conn.cursor()
        for a in co_authors:
            for b in a[1]:
                cursor.execute(sql,(a[0],b,))
            
                
        conn.commit()
        conn.close()
    except:
        print("Unable to insert into co_Authors")


def create_paper_cited():
    """create paper cited by paper table"""
    sql = """CREATE TABLE IF NOT EXISTS paper_cited
            (
                index bigint,
                cited_index bigint default NULL,
                UNIQUE(index,cited_index),
                CONSTRAINT fk_rp
                    FOREIGN KEY(index)
                        REFERENCES ResearchPapers(Index)

            )
            """
    sql1=""" CREATE UNIQUE INDEX unq ON paper_cited (index) WHERE cited_index IS NULL"""

    try:
        conn = psycopg2.connect(host = db_host , database = db_name , user = db_user , password = db_pass) 
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        conn.close()
    except:
        print("Unable to create paper_cited table")
    try:
        conn = psycopg2.connect(host = db_host , database = db_name , user = db_user , password = db_pass) 
        cursor = conn.cursor()
        cursor.execute(sql1)
        conn.commit()
        conn.close()
    except:
        print("ignoring creating constraint")


def insert_paper_cited(indexes):

    sql = """INSERT INTO paper_cited(index,cited_index)
            VALUES(%s,%s)
            ON CONFLICT (index,cited_index) DO NOTHING
            """
    
    try:
        conn = psycopg2.connect(host = db_host , database = db_name , user = db_user , password = db_pass) 
        cursor = conn.cursor()
        
        for index in indexes:
            for ind in index[1]:
                cursor.execute(sql,(index[0],ind,))
        conn.commit()
        conn.close()
    except:
        print("Unable to insert into paper cited table")

def create_rp_venue():
    """"""
    sql= """CREATE TABLE IF NOT EXISTS rp_venues
            (
                index bigint NOT NULL,
                venue TEXT REFERENCES Venues(name),
                year varchar(128) default NULL,
                UNIQUE (index,venue,year),

                CONSTRAINT fk_index
                    FOREIGN KEY(index)
                        REFERENCES ResearchPapers(Index)
            )
            """
    try:
        conn = psycopg2.connect(host = db_host , database = db_name , user = db_user , password = db_pass) 
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        conn.close()
    except:
        print("Unable to create VENUE table")
def insert_rp_venue(venues):

    sql = """INSERT INTO rp_venues(index,venue,year)
             VALUES(%s,%s,%s)
             ON CONFLICT(index,venue,year) DO NOTHING 
              """
    try:
        conn = psycopg2.connect(host = db_host , database = db_name , user = db_user , password = db_pass) 
        cursor = conn.cursor()
        for v in venues:
            cursor.execute(sql,(v[0],v[1],v[2],))
        conn.commit()
        conn.close()
    except:
        print("Unable to insert into rp_venue table")

def create_authors():
    """create Authors"""
    sql= """CREATE TABLE IF NOT EXISTS Authors
            (
                name varchar(300) NOT NULL,
                UNIQUE (name)
            )
            """
    try:
        conn = psycopg2.connect(host = db_host , database = db_name , user = db_user , password = db_pass) 
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        conn.close()
    except:
        print("Unable to create authors table")

def insert_authors(Authors):

    sql="""INSERT INTO Authors(name)
            VALUES(%s)  
            ON CONFLICT(name) DO NOTHING"""


    try:
        conn = psycopg2.connect(host = db_host , database = db_name , user = db_user , password = db_pass) 
        cursor = conn.cursor()
        for Author in Authors:
            for name in Author:
                cursor.execute(sql,(name,))
        conn.commit()
        conn.close()
    except:
        print("Unable to insert into Authors")

def create_venues():
    sql= """CREATE TABLE IF NOT EXISTS Venues
            (
                name varchar(500),
                UNIQUE (name)
            )
            """
    try:
        conn = psycopg2.connect(host = db_host , database = db_name , user = db_user , password = db_pass) 
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        conn.close()
    except:
        print("Unable to create Venues table")

def insert_into_venues(names):
    sql="""INSERT INTO Venues(name)
            VALUES(%s) 
            ON CONFLICT(name) DO NOTHING"""

    try:
        conn = psycopg2.connect(host = db_host , database = db_name , user = db_user , password = db_pass) 
        cursor = conn.cursor()
        for name in names:
            cursor.execute(sql,(name,))
        conn.commit()
        conn.close()
    except:
        print("Unable to insert into Venues")


#------ main starts from here-------   
#----- Creating tables ---------

#create research paper table
Create_ResearchPaper_Table() 

#create whole author table
create_authors()

#create all venues table
create_venues()

#create rp and co author relationship table
create_rp_co_author()

#create paper cited relationship
create_paper_cited()

#create rp_venue relationship 
create_rp_venue()

#-------opening source file-----------
try:
    source_file= open('source.txt','r',encoding="utf8")
    output_file = open('output.txt','w')
except:
    print("Unable to open source.txt file")

if source_file.closed:
    print("source file not open ")
    exit
    


#-------get details to push in to database
Rp_attributes=[]
Authors=[]
Venues=[]
co_authors=[]
p_cited_attr=[]
rp_venue_attr=[]

line=source_file.readline()
no_papers = int(line)


#----- get each paper details from read_RpAuthor from reader.py
for i in range(no_papers):
    index,Title,Author,abstract,paper_cited,venue,year=read_each_paper(source_file)

    if (Author[0]!=None):
        Authors.append(Author)
    if venue!=None:
        Venues.append(venue)
    if (len(index)!=0 and len(Title)!=0):
        #gathering research paper details in list 
        tuple = (index,Title,abstract,Author[0])
        Rp_attributes.append(tuple)
        if (len(Author)>=2  ):
            #gathering details of co author in list
            co_author_tuple=(index,Author[1:])
            co_authors.append(co_author_tuple)
        
            #gathering paper cited details in list
            #checks if any paper cited itself
        for p in paper_cited:
            if index==p:
                paper_cited.remove(p)
        p_cited_tuple=(index,paper_cited)
        p_cited_attr.append(p_cited_tuple)
        if venue:
            venue_tuple = (index,venue,year)
            rp_venue_attr.append(venue_tuple)
            
        
        


#inserting details into database
Insert_rp(Rp_attributes)
insert_authors(Authors)
insert_into_venues(Venues)

insert_coAuthors(co_authors)
insert_paper_cited(p_cited_attr)
insert_rp_venue(rp_venue_attr)


 


#clear lists
Rp_attributes.clear()
Authors.clear()
Venues.clear()

co_authors.clear()
p_cited_attr.clear()
rp_venue_attr.clear()


source_file.close()

