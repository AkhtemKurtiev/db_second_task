"""Модуль с парсингом сайта."""

import datetime
import os

import re
import requests
from xlrd import open_workbook

from constants import SKIP_WORDS, URL
from models.database import Session
from models.spimex_trading_results import Spimex_trading_results
from .service import string_to_date

parse = True
page = 1

while parse:

    url: str = URL + f'{page}'
    print(url)

    try:
        html = requests.get(url).text
    except Exception as e:
        print(e, 'Соединение разорвано, ещё одна попытка!')
        continue

    xls: list = []
    new_xls: list = []

    regular = r'href="(/upload/reports/oil_xls/oil_xls_\d{14}\.xls\?r=\d{4})"'
    filename: str | None = None

    for line in html.split('\n'):
        if 'href="/upload/reports/oil_xls/' in line:
            xls.append(line.strip())

    for i in xls:
        res = re.search(regular, i)
        if res:
            filename = res.group(1).split('/')[-1]
            new_xls.append('/upload/reports/oil_xls/' + filename)

    for i in new_xls:
        print(i)

    for name in new_xls:
        full_connect = True
        while full_connect:
            try:
                response = requests.get('https://spimex.com' + name)
                full_connect = False
            except Exception as e:
                print(
                    e,
                    'Соединение разорвано (скачаивание документа), ещё раз!'
                )
                continue

        filename = f'{name[-4:len(name)]}.xls'

        if response.status_code == 200:
            with open(filename, 'wb') as file:
                file.write(response.content)

            workbook = open_workbook(filename)
            sheet = workbook.sheet_by_index(0)

            valid_data: bool = False
            session = Session()

            for row_idx in range(sheet.nrows):
                row_data = sheet.row_values(row_idx)
                if re.match(r'Дата торгов: \d{2}\.\d{2}\.\d{4}', row_data[1]):
                    year, month, day = string_to_date(row_data[1][13:])
                    if year == 2022:
                        break
                if row_data[1] in SKIP_WORDS:
                    continue
                if row_data[1] == 'Маклер СПбМТСБ':
                    break
                if valid_data:
                    count = row_data[14]
                    if count == '-':
                        continue
                    new_data = Spimex_trading_results(
                        exchange_product_id=row_data[1],
                        exchange_product_name=row_data[2],
                        oil_id=row_data[1][:4],
                        delivery_basis_id=row_data[1][4:7],
                        delivery_basis_name=row_data[3],
                        delivery_type_id=row_data[1][-1],
                        volume=row_data[4],
                        total=row_data[5],
                        count=count,
                        date=datetime.date(
                            year, month, day
                        )
                    )
                    session.add(new_data)
                    session.commit()
                if 'Единица измерения: Метрическая тонна' in row_data[1]:
                    valid_data = True
            if year == 2022:
                parse = False
                break
        os.remove(filename)
    page += 1
