from bs4 import BeautifulSoup
import math, sqlite3

db = sqlite3.connect('allfields.db')
cur = db.cursor()

res = []

with open('КУР-СОЛ-0029-1.kml', 'r', encoding='utf-8') as f:
    s = BeautifulSoup(f, 'xml')
    res = s.find_all('coordinates')
    coords = [(float(c.split(',')[1]), float(c.split(',')[0])) for c in res[0].text.strip().split(' ')]

    lenght = 0
    centerCoords = []

    for i in range(l := len(coords)):
        x1, y1 = coords[i]
        x2, y2 = coords[(i+1) % l]
        xd, yd = coords[(l // 2 + i) % l]
        cos111 = math.cos(111)
        lnght = math.sqrt((x1 * 111 - x2 * 111) ** 2 + (y1 * cos111 - y2 * cos111) ** 2)
        d = math.sqrt((xd) ** 2 + (yd) ** 2)

        centerCoords.append((
            (x1 + xd) / 2,
            (y1 + yd) / 2
        ))
        lenght += lnght
    
    center = (
        sum([a[0] for a in centerCoords]) / l,
        sum([a[1] for a in centerCoords]) / l
    )
    
    cur.execute('INSERT INTO fields_geo(field, center_x, center_y, radius)')
    