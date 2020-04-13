import pandas as pd

def process(name):
    df = pd.read_excel(name + ".xlsx")
    df.columns = list(df.loc[0])
    return df

def func(n):
    df = process(name)
    df = df.drop(df.columns[:3], axis=1)[2:]
    df.columns = ["name", "address"]
    df.to_csv(name+"_ok.xlsx", index=False)



names = ["733316", "733380", "733315", "841246844A", "COREIN220914", "FACNBFC220914", "IFC19092016", "NMFI012014FL", "RIDFNB311214"]
for name in names:
    func(name)
    
    
    
    
for name in ["59260", "P2P30062018"]:
    df = process(name)
    df = df.drop(df.columns[:3], axis=1)[3:]
    df.columns = ["name", "address"]
    df.to_csv(name+"_ok.xlsx", index=False)
    
    
name = "LSCRCRBI07092016"
df = process(name)
df = df.drop(df.columns[:2], axis=1)[1:]
df.columns = ["name", "address"]
df.to_csv(name+"_ok.xlsx", index=False)


name = "80544_13"
df = pd.read_excel(name + ".xlsx")
df.columns = list(df.iloc[1])
df = df.drop(df.columns[:2], axis=1)[1:]
df = df.drop(df.columns[-1], axis=1)
df.columns = ["name", "inclusion_date"]
df.to_csv(name+"_ok.xlsx", index=False)
