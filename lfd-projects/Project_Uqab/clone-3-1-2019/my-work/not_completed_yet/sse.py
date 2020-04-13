import requests
from bs4 import BeautifulSoup
url = "http://english.sse.com.cn/listed/company/"
r = requests.get(url)
if r.ok:
    html = r.text
    soup = BeautifulSoup(html, 'lxml')
    a = soup.find("div", {"id" : "tableData_1092"})
    s = a.find("script")
    s = [i.strip() for i in a.get_text().splitlines() if "<a href=" in i and "download</a>" in i][0]
    b = s.find("listed")
    e = s.find(">download")
    s = "http://query.sse.com.cn/" + s[b:e-1]
    s = s.replace("http://", "") 
    print(s)
