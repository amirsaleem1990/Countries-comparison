with open("msg_ok.txt", "r") as file:
    f = file.read()

E_pounds = float(f[f.find("خصم")+3:f.find("من")].strip())
date_time = f[f.find("في")+2: f.find("شكرا")].strip()
account = int(f[f.find("رقم")+4 : f.find("في")].strip())

print(E_pounds)
print(account)
print(date_time)