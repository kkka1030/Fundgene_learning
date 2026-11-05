import requests
import pandas as pd
import time
import re
import json

def get_fund_history(fund_code, start_date, end_date):
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Referer': f'http://fundf10.eastmoney.com/jjjz_{fund_code}.html'
    }
    page = 1
    all_data = []
    while True:
        url = f'http://api.fund.eastmoney.com/f10/lsjz?callback=jQuery18308909743577296265_1618718938738&fundCode={fund_code}&pageIndex={page}&pageSize=20&startDate={start_date}&endDate={end_date}'
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            break
        # 提取 JSON 数据
        match = re.search(r'\((.*)\)', response.text)
        if not match:
            break
        json_data = json.loads(match.group(1))
        data = json_data['Data']['LSJZList']
        if not data:
            break
        all_data.extend(data)
        total_count = int(json_data['TotalCount'])
        if page * 20 >= total_count:
            break
        page += 1
        time.sleep(0.5)  # 防止请求过快被封
    df = pd.DataFrame(all_data)
    return df


fund_codes = ['110022', '161005', '050011', '510050', '001180', '000083', '008888', '008763', '003003', '110005']
start_date = '2020-01-01'
end_date = '2020-12-31'

for code in fund_codes:
    df = get_fund_history(code, start_date, end_date)
    df.to_csv(f'{code}_history.csv', index=False, encoding='utf-8-sig')
    print(f'已保存基金 {code} 的历史净值数据')
