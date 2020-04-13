from bs4 import BeautifulSoup
import requests

s = BeautifulSoup(requests.get("https://www.propertyfinder.ae/en/rent/properties-for-rent.html").text, "lxml")
total_adds_qty = int((s.find("div", {"class" : "property-header__list-count ge_resultsnumber"}).text).split()[0])
pages_links = []
for i in range(1, round(total_adds_qty / 25) + 1):
	pages_links.append("https://www.propertyfinder.ae/en/rent/properties-for-rent.html?page=" + str(i))
	
adds_links = []
for link in pages_links:
	try:
		s = BeautifulSoup(requests.get(link).text, "lxml")
		for l_ in s.find("div", {"class" : "card-list card-list--property",
					  "data-qs" : "cardlist"}).select("div", {"class" : "card-list__item"}):
				adds_links.append("https://www.propertyfinder.ae" + l_.find("a")["href"])
	except:
		pass