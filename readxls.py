import pandas as pd
from bs4 import BeautifulSoup as bs
page = open(r'd:\selestock\xsjj.html','r',encoding="utf-8")
soup = bs(page,"lxml")

page.close()

tables = soup.findAll('table')
tab = tables[0]
data=[]
for tr in tab.findAll('tr'):
    row=[]
    for td in tr.findAll('td'):
        row.append(td.getText().replace('\t',''))
        
    data.append(row)

cols=data[0]
data=data[1:]
df=pd.DataFrame(data,columns=cols)


