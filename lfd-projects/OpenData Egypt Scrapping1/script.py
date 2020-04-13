# extract pages from PDF and make another PDF that contain only these pages
import PyPDF2
pfr = PyPDF2.PdfFileReader(open("a.pdf", "rb"))

writer = PyPDF2.PdfFileWriter()

d = {}
for page_num in list(range(18, 33, 2)):
    d["extracted_page_" + str(page_num)] = pfr.getPage(page_num)

# we specified  pages, now we create an another pdf with only these pages
for page in d.keys():
    writer.addPage(d[page])

with open("allTables.pdf", "wb") as outputStream:
    writer.write(outputStream)






from PyPDF2 import PdfFileReader
with open("a.pdf", 'rb') as f:
    pages_qty = PdfFileReader(f).getNumPages()
    
lst = []
import camelot
for i in range(1, pages_qty+1):
    try:
        lst.append(camelot.read_pdf('a.pdf',flavor="stream", pages=str(i))[0].df)
    except:
        pass
    
import pandas as pd
writer = pd.ExcelWriter('pandas_multiple.xlsx')
for e, df in enumerate(lst):
    df.to_excel(writer, sheet_name='Sheet' + str(e+1))

writer.save()





from tabula import wrapper
tables = wrapper.read_pdf("a.pdf",multiple_tables=True,pages='all')

writer = pd.ExcelWriter('pandas_multiple_tabula.xlsx')
for e, df in enumerate(tables):
    df.to_excel(writer, sheet_name='Sheet' + str(e+1))
writer.save()