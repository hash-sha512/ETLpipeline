import requests
import psycopg2
import base64
import cv2
import numpy as np
from PIL import Image

def get_images(queryparams, n):
    try:
        conn = psycopg2.connect(
            host="<ENV...>",
            database="<ENV...>",
            user="<ENV...>",
            password="<ENV...>"
            )
        cur = conn.cursor()
        queryparams = tuple(queryparams)
        cur.execute("SELECT image FROM images INNER JOIN base ON images.pageid = base.pageid and base.querystring IN %s INNER JOIN actualimages ON actualimages.imageurl = images.imageurl ORDER BY RANDOM() LIMIT %s", (queryparams, n))
        image_sets = [bytes(row[0]) for row in cur.fetchall()]
        print(image_sets)
        """
            its a mess w the encoding .... some are svgs .. some are pure bytes and then utf-8 erros when np.array() is done 
            will consider this at a later time
        """
        #base64_img = base64.b64encode(binary_img)
        #images_as_bytes = [bytes(image_array) for image_array in image_sets]
        #print(np.array(Image.open(images_as_bytes[2])))
        #image = cv2.imread(images_as_bytes[0])
        #nparr = np.fromstring(imgages_as_bytes[0], np.uint8)
        #img_np = cv2.imdecode(nparr, cv2.CV_LOAD_IMAGE_COLOR)
        #print(img_np)
        # BGR -> RGB
        #img = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        #print(image)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        cur.close()
        conn.close()

def get_ordered_attributes(queryparams):
    try:
        conn = psycopg2.connect(
            host="<ENV....>",
            database="<ENV...>",
            user="<ENV....>",
            password="<ENV....>"
        )
        cur = conn.cursor()
        print('query started ... now waitssss one of these mf took 55mins 7secs')
        cur.execute("""SELECT title.title, textcontent.textcontent, ARRAY_AGG (
        DISTINCT contributors.userid || ' - ' || contributors.username)
        contributors FROM base INNER JOIN title ON base.querystring = %s AND base.pageid = title.pageid INNER JOIN textcontent ON textcontent.pageid = title.pageid INNER JOIN contributors ON contributors.pageid = title.pageid INNER JOIN extlinks ON title.pageid = extlinks.pageid INNER JOIN internallinks ON internallinks.pageid = title.pageid  GROUP BY title.title, textcontent.textcontent ORDER BY COUNT(extlinks.extlink)/COUNT(internallinks.internallink)""", (queryparams, )) 
        print(cur.fetchall())
        #alternatively use generators .. yield 
        print('fin')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    get_images(['Arts', 'Poetry'], 3)
    get_ordered_attributes('Computer Vision')
