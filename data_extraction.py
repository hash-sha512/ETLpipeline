# import sys to get more detailed Python exception info
import sys

# import the connect library for psycopg2
from psycopg2 import connect

# import the error handling libraries for psycopg2
from psycopg2 import OperationalError, errorcodes, errors

"""
define a function that handles and parses psycopg2 exceptions
def print_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()

    # get the line number when exception occured
    line_num = traceback.tb_lineno

    # print the connect() error
    print ("\npsycopg2 ERROR:", err, "on line number:", line_num)
    print ("psycopg2 traceback:", traceback, "-- type:", err_type)

    # psycopg2 extensions.Diagnostics object attribute
    print ("\nextensions.Diagnostics:", err)#err.diag)

    # print the pgcode and pgerror exceptions
    print ("pgerror:", err) #err.pgerror)
    print ("pgcode:", err)#err.pgcode, "\n")
"""


import requests
import psycopg2

def createAllTables():
    commandsForTableCreation = (
        """
        CREATE TABLE IF NOT EXISTS public.base
        (
        pageid integer NOT NULL,
        querystring character varying COLLATE pg_catalog."default" NOT NULL,
        CONSTRAINT "querystringPageidPK" PRIMARY KEY (pageid, querystring)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS public.title
        (
        pageid integer NOT NULL,
        title character varying COLLATE pg_catalog."default" NOT NULL,
        wordcount integer NOT NULL,
        querystring character varying COLLATE pg_catalog."default" NOT NULL,
        CONSTRAINT title_pkey PRIMARY KEY (pageid),
        CONSTRAINT "querystringPageidFK" FOREIGN KEY (pageid, querystring)
            REFERENCES public.base (pageid, querystring) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
            NOT VALID
            )
        """,
        """
        CREATE TABLE IF NOT EXISTS public.textcontent
        (
        pageid integer NOT NULL,
        textcontent text COLLATE pg_catalog."default" NOT NULL,
        CONSTRAINT textcontent_pkey PRIMARY KEY (pageid),
        CONSTRAINT "pageidFK" FOREIGN KEY (pageid)
            REFERENCES public.title (pageid) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
            NOT VALID
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS public.internallinks
        (
        internallink character varying COLLATE pg_catalog."default" NOT NULL,
        pageid integer NOT NULL,
        CONSTRAINT "intlinksPageid" PRIMARY KEY (internallink, pageid),
        CONSTRAINT "pageidFK" FOREIGN KEY (pageid)
            REFERENCES public.title (pageid) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
            NOT VALID
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS public.extlinks
        (
        extlink character varying COLLATE pg_catalog."default" NOT NULL,
        pageid integer NOT NULL,
        CONSTRAINT "extlinkPageid" PRIMARY KEY (extlink, pageid),
        CONSTRAINT "pageidFK" FOREIGN KEY (pageid)
            REFERENCES public.title (pageid) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
            NOT VALID
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS public.contributors
        (
        userid integer NOT NULL,
        username character varying COLLATE pg_catalog."default" NOT NULL,
        pageid integer NOT NULL,
        CONSTRAINT "useridPageid" PRIMARY KEY (userid, pageid),
        CONSTRAINT "pageidFK" FOREIGN KEY (pageid)
            REFERENCES public.title (pageid) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
            NOT VALID
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS public.categories
        (
        category character varying COLLATE pg_catalog."default" NOT NULL,
        pageid integer NOT NULL,
        CONSTRAINT "categoryPageid" PRIMARY KEY (category, pageid),
        CONSTRAINT "pageidFK" FOREIGN KEY (pageid)
            REFERENCES public.title (pageid) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
            NOT VALID
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS public.actualimages
        (
        imageurl character varying COLLATE pg_catalog."default" NOT NULL,
        image bytea NOT NULL,
        CONSTRAINT actualimages_pkey PRIMARY KEY (imageurl)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS public.images
        (
        imageurl character varying COLLATE pg_catalog."default" NOT NULL,
        pageid integer NOT NULL,
        CONSTRAINT images_pkey PRIMARY KEY (imageurl, pageid),
        CONSTRAINT "imageurlFK" FOREIGN KEY (imageurl)
            REFERENCES public.actualimages (imageurl) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
            NOT VALID,
        CONSTRAINT "pageidFK" FOREIGN KEY (pageid)
            REFERENCES public.title (pageid) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
        )
        """)
    conn = None
    try:
        conn = psycopg2.connect(
            host="<ENV.....>",
            database="<ENV.....>",
            user="<ENV.....>",
            password="<ENV.....>"
            )
        cur = conn.cursor()
        for command in commandsForTableCreation:
            cur.execute(command)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        conn.close()
    


def apiDataExtractionIntoDatabaseDump():
    createAllTables()
    try:
        querystrings = ['Deep learning', 'Machine Learning', 'NLP', 'Computer Vision', 'Mathematics', 'Optimization','Statistics', 'Linear Algebra', 'Calculus', 'Linguistics', 'Computation', 'Probability', 'Database', 'Biology', 'Physics', 'Chemistry', 'Economics', 'Political Sciences', 'Drama', 'Poetry', 'Arts']
        S = requests.Session()
        URL = "https://en.wikipedia.org/w/api.php"
    
        counter = 0 #pages(21 queryStrings * 20 pages for each querystring)

        conn = psycopg2.connect(
            host="<ENV.....>",
            database="<ENV.....>",
            user="<ENV.....>",
            password="<ENV.....>"
            )
        cur = conn.cursor()
    
        for querystring in querystrings:
            PARAMS = {
                "action": "query",
                "format": "json",
                "list": "search",
                "srsearch": querystring,
                "srlimit": 20,
                "sroffset": 0,
                }
            R = S.get(url=URL, params=PARAMS)
            pages = R.json()['query']['search']
            #print(pages)
            for page in pages :
                counter += 1
                print(counter)
                title = page['title']
                pageid = page['pageid']
                wordcount = page['wordcount']
                cur.execute("INSERT INTO base (pageid, querystring) VALUES (%s, %s) ON CONFLICT (pageid, querystring) DO NOTHING ", (pageid, querystring))
                conn.commit()
                cur.execute("INSERT INTO title (pageid, title, wordcount, querystring) VALUES (%s, %s, %s, %s) ON CONFLICT (pageid) DO NOTHING ", (pageid, title, wordcount, querystring))
                conn.commit()
                pagedata = S.get(f"https://en.wikipedia.org/w/api.php?action=parse&pageid={pageid}&props=text&format=json")
                pagedata.raise_for_status()
                imagefilenames = pagedata.json()['parse']['images']
                if(imagefilenames):
                    #imageurls can only be extraced for 50 images at a time
                    for imagefilenames50atonceiter in range((len(imagefilenames)//50)+1):
                        imagefilenames50atonce = '|File:'.join(imagefilenames[imagefilenames50atonceiter:len(imagefilenames) if imagefilenames50atonceiter+50 > len(imagefilenames) else imagefilenames50atonceiter+50])
                        imagefilenames50atonceiter += 50
                        imagefilenames50atonce = 'File:' + imagefilenames50atonce
                        #print(imagefilenames50atonce)
                        imageurls50atonce = S.get(f'https://en.wikipedia.org/w/api.php?action=query&format=json&prop=imageinfo&iiprop=url&titles={imagefilenames50atonce}')
                        imageurls50atonce.raise_for_status()
                        imageurls50atonce = imageurls50atonce.json()['query']['pages']
                        #imageurls = imageurls.json()
                        for imageurl in imageurls50atonce.keys():
                            if('imageinfo' in imageurls50atonce[imageurl]):
                                url = imageurls50atonce[imageurl]['imageinfo'][0]['url']
                                img = S.get(f"{imageurls50atonce[imageurl]['imageinfo'][0]['url']}").content
                                cur.execute("INSERT INTO actualimages (imageurl, image) VALUES (%s, %s) ON CONFLICT (imageurl) DO NOTHING ", (url, img ))
                                cur.execute("INSERT INTO images (imageurl, pageid) VALUES (%s, %s) ON CONFLICT (imageurl, pageid) DO NOTHING ", (url, pageid ))
                                #print(S.get(f"{temppp[c]['imageinfo'][0]['url']}").content)
                            conn.commit()
                else:
                    print('No images found for', pageid, 'despite having received file names of the now supposedly missing images in the api response')
                cur.execute("INSERT INTO textcontent (textcontent, pageid) VALUES (%s, %s) ON CONFLICT (pageid) DO NOTHING ", (pagedata.json()['parse']['text']['*'], pageid))
                conn.commit()
                externallinks = pagedata.json()['parse']['externallinks']
                for extlink in externallinks:
                    cur.execute("INSERT INTO extlinks (extlink, pageid) VALUES (%s, %s) ON CONFLICT (extlink, pageid) DO NOTHING ", (extlink, pageid))
                conn.commit()
                iwlinks = pagedata.json()['parse']['iwlinks']
                for iwlink in iwlinks: 
                    cur.execute("INSERT INTO internallinks (internallink, pageid) VALUES (%s, %s) ON CONFLICT (internallink, pageid) DO NOTHING ", (iwlink['url'], pageid))
                conn.commit()
                categories = pagedata.json()['parse']['categories']
                for category in categories:
                    cur.execute("INSERT INTO categories (category, pageid) VALUES (%s, %s) ON CONFLICT (category, pageid) DO NOTHING ", (category['*'], pageid))
                conn.commit()
                contributors = S.get(f"https://en.wikipedia.org/w/api.php?action=query&pageids={pageid}&prop=contributors&pclimit=max&format=json")
                contributors.raise_for_status()
                for contributor in contributors.json()['query']['pages'][str(pageid)]['contributors'] : 
                    cur.execute("INSERT INTO contributors (userid, username, pageid) VALUES (%s, %s, %s) ON CONFLICT (userid, pageid) DO NOTHING ", (contributor['userid'], contributor['name'], pageid))
                conn.commit()
        print('fin')
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print ("OOps: Something Else", err)
    except (psycopg2.DatabaseError) as dberr:
        print("database error", dberr)
    except KeyError as error:
        print(error)
    except Exception as err:
        print_psycopg2_exception(err)
        print('wtf', err)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    apiDataExtractionIntoDatabaseDump()
