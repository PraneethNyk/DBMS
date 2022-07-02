
from itertools import count
from pickle import TRUE
from turtle import title
from wsgiref.util import FileWrapper


flag=False



#return a single paper ( Index , Title , Author , abstract) on a call 
def read_each_paper(source_file):
    #Authors=[]
    line = source_file.readline()
    Author=[]
    
    abstract=""
    venue=""
    paper_cited=[]
    year=""
    while line !='\n' :
        if line.startswith('#*'):
            #title_counter+=1
            #print(f"Title: {line.removeprefix('#*')}")
            line= line.removeprefix('#*')
            line = line.removesuffix('\n')
            Title=line
        elif line.startswith('#@'):
            #authors+=line.count(',') + 1
            #print(f"Authours are {line.removeprefix('#@')}")
            line= line.removeprefix('#@')
            line = line.removesuffix('\n')
            if len(line)!=0:
                Author=line.split(',')
                #print(f'lenght of line is {len(line)}')
            for a in Author:
                #print(a)
                if len(a)==0:
                   # print(f'a is removed ')
                    Author.remove(a)
        elif line.startswith('#index'):
            #print(f"ID : {line.removeprefix('#index')}")
            line = line.removeprefix('#index')
            line = line.removesuffix('\n')
            index=line
        elif line.startswith('#!'):
            #print(f"Abstract : { line.removeprefix('#!')  }")
            line = line.removeprefix('#!')
            line = line.removesuffix('\n')
            abstract = line
        elif line.startswith('#%'):
            line = line.removeprefix('#%')
            line = line.removesuffix('\n')
            paper_cited.append(line)
        elif line.startswith('#c'):
            line = line.removeprefix('#c')
            line = line.removesuffix('\n')
            venue=line
        elif line.startswith('#t'):
            line = line.removeprefix('#t')
            line = line.removesuffix('\n')
            year=line

        line = source_file.readline()
#----while loop exited
    if(len(paper_cited)==0):
        paper_cited.append(None)
    if(len(venue)==0):
        venue=None
    if(len(Author)==0):
        Author.append(None)
    return index,Title,Author,abstract,paper_cited,venue,year

def file_opener():
    try:
        source_file= open('source.txt','r',encoding="utf8")
        output_file = open('output.txt','w')
        return source_file
    except:
        print("Unable to open source.txt file")       