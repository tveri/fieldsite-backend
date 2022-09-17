from flask import Flask, request, send_from_directory, jsonify, render_template, redirect, make_response, send_file
import re, os, datetime, time, json, sqlite3
import openpyxl as ox
from openpyxl.utils.cell import coordinate_from_string, column_index_from_string
from flask_cors import CORS, cross_origin

MONTHS = [
    'Январь',
    'Февраль',
    'Март',
    'Апрель',
    'Май',
    'Июнь',
    'Июль',
    'Август',
    'Сентябрь',
    'Октябрь',
    'Ноябрь',
    'Декабрь',
]

app = Flask(__name__, static_url_path="")
app.config["DEBUG"] = True
app.config["JSON_AS_ASCII"] = False
app.config["HOST"] = "0.0.0.0"
application = app
CORS(app, support_credentials=True)

db = sqlite3.connect('./fields.db', check_same_thread=False)
cur = db.cursor()

fieldColumns = {col[0]: col[1] for col in cur.execute('PRAGMA table_info(field62z05)').fetchall()}



def getConnection():
    return sqlite3.connect('./fields.db', check_same_thread=False)


def tryDecorator(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(e)
            return 0
    return wrapper


def Gfunc(data, row):
    if data[row][3] > 0:
        return 0.0061 * ((25 + data[row][3]) ** 2) * (1 - 0.01 * data[row][4]) * 0.64 * (1 + 0.19 * data[row][5])
    return 0


def Hfunc(data, row):
    return 0.0000015592 * data[row][0] ** 3 - 0.0007462857 * data[row][0] ** 2 + 0.0896413572 * data[row][0] + 0.9787526972


def Ifunc(data, row):
    if data[row][2] > 0:
        return data[row][3] + (data[row-1][8] if row else 0)
    return 0


def Jfunc(data, row):
    if data[row][8] == 0:
        return 0
    return -0.000000000177496 * data[row][8] ** 3 + 0.000000336028555 * data[row][8] ** 2 + 0.0001264253108 * data[row][8] + 0.734104749417373


@tryDecorator
def Lfunc(data, row):
    if data[row][10] == 0:
        return 0.75 * data[row][6]
    return data[row][10] * data[row][6]


def Mfunc(data, row):
    if data[row][10] == 0:
        return 0.25
    return -0.000000000025495 * data[row][8] ** 3 - 0.000000045527389 * data[row][8] ** 2 + 0.000367679195804 * data[row][8] + 0.250076486014065


def Nfunc(data, row, globalData):
    return globalData[0] * globalData[1] * data[row][12] * 1000


def Ofunc(data, row, globalData):
    return globalData[0] * globalData[1] * globalData[2] * data[row][12] * 1000


def Pfunc(data, row, globalData):
    return data[row][13] * globalData[3] - data[row][14]


def Qfunc(data, row, globalData):
    return data[row][13] * globalData[4]


def Rfunc(data, row):
    if not row: return 0
    return data[row][16] - data[row-1][16]


def Sfunc(data, row):
    if not row:
        return data[row][16]
    return data[row-1][25]


def Tfunc(data, row):
    return data[row][13] - data[row][18] + data[row][11]


def Wfunc(data, row):
    if data[row][20] + data[row][21] < data[row][19]:
        return data[row][20] + data[row][21]
    if data[row][20] + data[row][21] > data[row][19]:
        return data[row][19]
    return data[row][20] + data[row][21]


def Xfunc(data, row):
    return data[row][18] + data[row][22] + data[row][17] - data[row][11]


def Yfunc(data, row):
    if data[row][23] > data[row][14]:
        return 0
    if data[row][23] < data[row][14]:
        return data[row][15] / 2


def Zfunc(data, row):
    return data[row][23] + data[row][24]


def AAfunc(data, row):
    return data[row][13] + data[row][17] - data[row][25]


def ABfunc(data, row):
    return -(data[row][13] + data[row][17] - data[row][14])


def ACfunc(data, row):
    return -(data[row][13] + data[row][17] - data[row][25])


def calcAllData(data, globalData):
    for row in range(len(data)):
        data[row][6] = round(Gfunc(data, row), 2)
        data[row][7] = round(Hfunc(data, row), 2)
        data[row][8] = round(Ifunc(data, row), 2)
        data[row][9] = round(Jfunc(data, row), 2)
        data[row][11] = round(Lfunc(data, row), 2)
        data[row][12] = round(Mfunc(data, row), 2)
        data[row][13] = round(Nfunc(data, row, globalData), 2)
        data[row][14] = round(Ofunc(data, row, globalData), 2)
        data[row][15] = round(Pfunc(data, row, globalData), 2)
        data[row][16] = round(Qfunc(data, row, globalData), 2)
        data[row][17] = round(Rfunc(data, row), 2)
        data[row][18] = round(Sfunc(data, row), 2)
        data[row][19] = round(Tfunc(data, row), 2)
        data[row][22] = round(Wfunc(data, row), 2)
        data[row][23] = round(Xfunc(data, row), 2)
        data[row][24] = round(Yfunc(data, row), 2)
        data[row][25] = round(Zfunc(data, row), 2)
        data[row][26] = round(AAfunc(data, row), 2)
        data[row][27] = round(ABfunc(data, row), 2)
        data[row][28] = round(ACfunc(data, row), 2)
    return data

def dataFromDBtoTableData(rawData):
    data = []
    columnsInRow = [0 for _ in range(29)]
    for row in range(len(rawData)):
        data.append(columnsInRow[:])
        date = datetime.datetime.fromtimestamp(round(d if (d := rawData[row][1]) else 0, 2))
        data[row][0] = round(d if (d := rawData[row][0]) else 0, 2)
        data[row][1] = f'{date.day if date.day > 9 else f"0{date.day}"}.{date.month if date.month > 9 else f"0{date.month}"}.{date.year}'
        data[row][2] = round(d if (d := rawData[row][2]) else 0, 2)
        data[row][3] = round(d if (d := rawData[row][3]) else 0, 2)
        data[row][4] = round(d if (d := rawData[row][4]) else 0, 2)
        data[row][5] = round(d if (d := rawData[row][5]) else 0, 2)
        data[row][10] = round(d if (d := rawData[row][6]) else 0, 2)
        data[row][20] = round(d if (d := rawData[row][7]) else 0, 2)
        data[row][21] = round(d if (d := rawData[row][8]) else 0, 2)
    return data

def getDataFromDB(db, field='62-05'):
    return cur.execute(f"SELECT * FROM field{field.replace('-', 'z')}").fetchall()


def getGlobalDataFromDB(db, field='62-05'):
    return cur.execute(f"SELECT * FROM field{field.replace('-', 'z')}global").fetchall()[0][1:]


def getTableData(db, field='62-05'):
    return calcAllData(dataFromDBtoTableData(getDataFromDB(db, field)), getGlobalDataFromDB(db, field))


@app.route('/api/getgraphics/')
def getGraphics(field='62-05'):
    resp = []
    data = getTableData(db, field)

    for row in data:
        resp.append({
                    'data': f'{(d := datetime.datetime.fromtimestamp(row[1])).day} {MONTHS[d.month]}',
                    'humidityRange': round(row[27]),
                    'humidity': round(row[28]),
                    'waterIntake': round(row[11]),
                    'rain': round(row[20]),
                    'watering':  round(row[21])
                })


    return make_response(resp)


@app.route('/api/gettable/')
@cross_origin(supports_credentials=True)
def getTable(field='62-05'):
    return make_response(getTableData(db, field))

# data = [(r[0].value, int(datetime.datetime.timestamp(r[1].value)), r[2].value, r[3].value, r[4].value, r[5].value, r[10].value, r[20].value, r[21].value) for r in [row for row in ws.rows][5:] if r[1].value]

colMatch = {
    0: 0,
    1: 1,
    2: 2,
    3: 3,
    4: 4,
    5: 5,
    10: 6,
    20: 7,
    21: 8,
}

@app.route('/api/settablechange/')
def setTableChange(field='62-05'):
    if (col := colMatch.get(int(request.args['column']))) and col <= 8 and request.args['value']:
        print(f"replacing {col} {request.args['row']} with {request.args['value']}")
        cur.execute(f'UPDATE field62z05 set {fieldColumns[col]} = ? WHERE id = ?', (request.args['value'], int(request.args['row'])+1))
        db.commit()
        return make_response(getTableData(db, field))
    return make_response('')


if __name__ == "__main__":
    app.run(host='0.0.0.0')
