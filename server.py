from flask import Flask, request, send_from_directory, jsonify, render_template, redirect, make_response, send_file
import re, os, datetime, time, json
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

workBook = ox.load_workbook('fullData.xlsx', 'stacked')
workBookDataOnly = ox.load_workbook('fullData.xlsx', 'stacked', data_only = True)
workSheet = workBookDataOnly['62-05']

ifRegex = re.compile('IF\((.+)\)')
cellRegex = re.compile('([A-Z]+)(\d+)')


# def remake(strToRemake, rec=False):
#     strRes = []
#     res = ifRegex.findall(strToRemake)
#     args = [strToRemake]
#     print(res)
#     for obj in res:
#         args = [c for c in [l.partition(obj) for l in args] if c]
#         print(args, '|', obj)
#         if 'IF' in obj:
#             strToRemake.replace(obj, remake(obj, rec=True))
#     args.append('')
#     return f"if({args[0]}){'{'+ args[1] + '}'}else{'{'+ args[2] + '}'}"


def remake(strToRemake):
    res = ifRegex.findall(strToRemake)
    args = res[0].split(',')
    args.append('')
    return f"if({args[0]}){'{'+ args[1] + '}'}else{'{'+ args[2] + '}'}"

def replaceCellWithValue(strFunc):
    for cell in cellRegex.findall(strFunc):
        strFunc = strFunc.replace(f'{"".join(cell)}', f'data[{column_index_from_string(cell[0])}][{cell[1]}]')
    return strFunc


def IFFunc(condition, truearg, falsearg, *, obj, lastStartIndex=0):
    if condition == 'IFFunc':
        startIndex = lastStartIndex + 2
        truearg = obj[startIndex + 2]
        falsearg = obj[startIndex + 3]
        condition = IFFunc(obj[startIndex], obj[startIndex+1], obj[startIndex+2], obj=obj[1:], lastStartIndex=lastStartIndex + 0)
    if truearg == 'IFFunc':
        startIndex = lastStartIndex + 3
        falsearg = obj[startIndex + 3]
        truearg = IFFunc(obj[startIndex], obj[startIndex+1] if len(obj[2:]) >= startIndex+1 else '', obj[startIndex+2] if len(obj[2:]) >= startIndex+2 else '', obj=obj[2:], lastStartIndex=lastStartIndex + 0)
    if falsearg == 'IFFunc':
        startIndex = lastStartIndex + 4
        falsearg = IFFunc(obj[startIndex], obj[startIndex+1] if len(obj[3:]) >= startIndex+2 else '', obj[startIndex+2] if len(obj[3:]) >= startIndex+2 else '', obj=obj[3:], lastStartIndex=lastStartIndex + 0)
    return f'if({condition}){"{" + truearg + "}"}else{"{" + falsearg + "}"}'


def makeJSFunc(strFunc, column):
    if column == 22:
        return ''
#     print(strFunc)
    strFunc = strFunc.replace('$', '')
    if 'IF' in strFunc:
        return replaceCellWithValue(remake(strFunc).strip('=')).replace('^', '**')
#     if 'IF' in strFunc:
#         obj = [item.strip('() =') for sublist in [s.split('(') for s in strFunc.strip('=').replace('IF(', 'IFFunc(').split(',')] for item in sublist]
#         return replaceCellWithValue(IFFunc(obj[1], obj[2] if len(obj) >= 2 else '', obj[3] if len(obj) >= 3 else '', obj=obj)).replace('^', '**')
    return replaceCellWithValue(strFunc.strip('=')).replace('^', '**')

def maxRow(column):
    for i, row in enumerate(column):
#         print(row.value)
        if not row.value:
            return i


@app.route('/api/getgraphics/')
def getGraphics():
    resp = []
    column = [i[1] for i in workSheet.rows]
    maxRowValue = (a := maxRow(column[5:]) + 5)

    for i in range(6, maxRowValue):
        try:
            resp.append({
                        'data': f'{(d := workSheet[i][1].value).day} {MONTHS[d.month]}',
                        'humidityRange': round(workSheet[i][27].value),
                        'humidity': round(workSheet[i][28].value),
                        'waterIntake': round(workSheet[i][11].value),
                        'rain': round(workSheet[i][20].value),
                        'watering':  round(workSheet[i][21].value)
                    })
        except Exception as e:
            print(
            workSheet[i][27].value,
            workSheet[i][28].value,
            workSheet[i][11].value,
            workSheet[i][20].value,
            workSheet[i][21].value,
            i
            )

    return make_response(resp)


@app.route('/api/getxlsxfile/')
@cross_origin(supports_credentials=True)
def getXlsxFile(sheetName='62-05'):
    resp = []
    ws = workBook[sheetName]
    wsdo = workBookDataOnly[sheetName]

    oneColumn = [i[1] for i in workSheet.rows]
    maxRowValue = (a := maxRow(oneColumn[5:]) + 5)


    for y, row in enumerate(zip(ws.rows, wsdo.rows)):
        resp.append([])
        for x, col in enumerate(zip(row[0][:maxRowValue], row[1][:maxRowValue])):
            resp[y].append({
            'value': col[1].value,
            'coordinate': col[0].coordinate if col[0].value else '',
            'column': x+1,
            'row': y,
            'dataType': col[0].data_type if col[0].value and '!' not in (col[0].value if type(col[0].value) == str else '') and 'AVER' not in (col[0].value if type(col[0].value) == str else '') and 'VLOOK' not in (col[0].value if type(col[0].value) == str else '') and 'SUM' not in (col[0].value if type(col[0].value) == str else '') else None,
            'func': None if col[0].data_type != 'f' else makeJSFunc(col[0].value, col[0].column)
            })
    return make_response(resp)


@app.route('/api/settablechange/')
def setTableChange():
    print(request.args['value'], request.args['column'], request.args['row'])
    return make_response('')


if __name__ == "__main__":
    app.run(host='0.0.0.0')
