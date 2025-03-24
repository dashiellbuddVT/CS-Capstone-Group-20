# Import Module
from tkinter import *
from DBaccess import *
import webbrowser

# create root window
root = Tk()

# root window title and dimension
root.title("Welcome to GeekForGeeks")
# Set geometry (widthxheight)
root.geometry('1200x800')


# adding Entry Field
numEntries = Entry(root, width = 45)
numEntries.grid()


resultsList = Listbox(root, height = 50, width = 100, selectmode= SINGLE)
resultsList.grid(row=1, columnspan = 2)

metaDataList = Listbox(root, height= 25, width = 100)
metaDataList.grid(column = 2, row = 1, columnspan=2)

results_IRIs = []

# function to display text when
# button is clicked
def getEntries():
    resultsList.delete(0,END)
    searchButton.configure(text="Reload")
    linkButton.configure(fg="green")
    metaDataButton.configure(fg="green")
    for result in getnTitles(numEntries.get()):
        results_IRIs.insert(0, result["s"]["value"])
        resultsList.insert(0, result["o"]["value"])

def followLink():
    link = getLink(results_IRIs[resultsList.curselection()[0]])
    webbrowser.open_new(link)

def showMetaData():
    MD = getMD(results_IRIs[resultsList.curselection()[0]])
    for line in MD:
        metaDataList.insert(END, line)
    

linkButton = Button(root, text = 'Go to paper', fg ="red", command=followLink)
linkButton.grid(column=2, row=0)
metaDataButton = Button(root, text = 'more info', fg ="red", command=showMetaData)
metaDataButton.grid(column=3, row=0)

# button widget with red color text
# inside
searchButton = Button(root, text = "Find ETDs" ,
             fg = "green", command=getEntries, width = 45)
# set Button grid
searchButton.grid(column=1, row=0)



# all widgets will be here
# Execute Tkinter
root.mainloop()


