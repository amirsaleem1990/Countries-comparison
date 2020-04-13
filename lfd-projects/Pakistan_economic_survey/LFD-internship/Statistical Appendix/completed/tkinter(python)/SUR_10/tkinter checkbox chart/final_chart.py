from tkinter import *
import pandas as pd
import matplotlib.pyplot as plt
import tkinter as tk
from PIL import Image, ImageTk


df = pd.read_csv("~/Dropbox/sources/data.csv")

class Checkbar(Frame):
   def __init__(self, parent=None, picks=[], side=TOP, anchor=E):
      Frame.__init__(self, parent)
      self.vars = []
      for pick in picks:
         var = IntVar()
         chk = Checkbutton(self, text=pick, variable=var)
         chk.pack(side=side, anchor=anchor, expand=YES)
         self.vars.append(var)
   def state(self):
      return map((lambda var: var.get()), self.vars)


def select():
    global root
    try: root2
    except NameError: pass
    else: root2.destroy()
    root = Tk()
    root.geometry("{0}x{1}+0+0".format(root.winfo_screenwidth(), root.winfo_screenheight()))
    tables = Checkbar(root, df.table.unique())
    samples = Checkbar(root, df.samples)
    tables.pack(side=LEFT)
    samples.pack(side=RIGHT)
    Button(root, text='Next', command=nextt).pack(side=BOTTOM)
    Button(root, text='Exit', command=root.destroy).pack(side=BOTTOM)
    select.tables = tables
    select.samples = samples
    root.mainloop()

def allstates(): 
    return list(select.tables.state()), list(select.samples.state())

def nextt():
    global root
    global root2
    root.destroy()
    root2 = Tk()
    root2.geometry("{0}x{1}+0+0".format(root2.winfo_screenwidth(), root2.winfo_screenheight()))
    
    tables =[]
    for i in range(len(allstates()[0])):
        if allstates()[0][i] == 1:
            tables.append(i)
    samples =[]
    for i in range(len(allstates()[1])):
        if allstates()[1][i] == 1:
            samples.append(i)

    df_samples = df.loc[samples]
    df_tables = df.set_index('table').loc[df.table.unique()[tables]].reset_index()
    adf = df_tables.append(df_samples)
    new_df = pd.DataFrame(columns=adf.columns)

    for i in range(len(adf)): 
        if adf.iloc[i].samples in list(df_samples.samples) and adf.iloc[i].table in list(df_tables.table):
            new_df.loc[i] = adf.iloc[i]
    new_df = new_df.drop_duplicates()
    plt.figure(figsize=(12,6))
    mng = plt.get_current_fig_manager()
    for i in range(len(new_df)):
        plt.plot(new_df.iloc[i][:-2])
    figManager = plt.get_current_fig_manager()
    figManager.full_screen_toggle() 
    plot_lagend = list(new_df.samples)
    plt.title('Table: ' + list(new_df.table.unique())[0])
    plt.xticks(range(18), list(df.columns[1:-1]), rotation = 90)
    plt.legend(plot_lagend)
    plt.subplots_adjust(bottom = 0.2)
    plt.grid()
    file_name = 'line_chart.png'
    plt.savefig(file_name)
    imageFile = file_name
    image1 = ImageTk.PhotoImage(Image.open(imageFile))
    panel1 = tk.Label(root2, image=image1)
    panel1.pack(fill='both', expand='yes', side='top')
    panel1.image = image1
    Button(root2, text='Back', command=select).pack(side=BOTTOM)
    Button(root2, text='Exit', command=root2.destroy).pack(side=BOTTOM)
    root2.mainloop()
    



select()
