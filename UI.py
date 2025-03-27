from tkinter import *
from DBaccess import *  
from Neo4jaccess import *
import webbrowser

# Root window
root = Tk()
root.title("ETD Knowledge Graph Search")
root.geometry('950x900')


backends = {
    "virtuoso": {
        "get_titles": getnTitles,
        "get_link": getLink,
        "get_meta": getMD
    },
    "neo4j": {
        "get_titles": getNeo4jTitles,
        "get_link": getNeo4jLink,
        "get_meta": getNeo4jMD
    }
}

current_backend = None
results_IRIs = []

def findETDs(backend):
    global current_backend
    current_backend = backend
    resultsList.delete(0, END)
    metaDataList.delete(0, END)
    results_IRIs.clear()
    
    try:
        query_fn = backends[backend]["get_titles"]
        results = query_fn(numEntries.get())
        for result in results:
            results_IRIs.append(result["s"]["value"])
            resultsList.insert(END, result["o"]["value"])
    except Exception as e:
        resultsList.insert(END, f"{backend.capitalize()} Error: {e}")

def followLink():
    try:
        index = resultsList.curselection()[0]
        iri = results_IRIs[index]
        link = backends[current_backend]["get_link"](iri)
        if link:
            webbrowser.open_new_tab(link)
    except Exception as e:
        print("Error:", e)

def showMetaData():
    try:
        index = resultsList.curselection()[0]
        iri = results_IRIs[index]
        metaDataList.delete(0, END)
        MD = backends[current_backend]["get_meta"](iri)
        for line in MD:
            metaDataList.insert(END, line)
    except Exception as e:
        print("Error:", e)

# UI Layout 

# Entry Field
numEntries = Entry(root, width=40)
numEntries.grid(row=0, column=0, padx = 10, pady=10)

# Search Buttons
Button(root, text="Find ETDs (Virtuoso)", fg="green",
       command=lambda: findETDs("virtuoso")).grid(row=0, column=1, pady=10, padx=10)

Button(root, text="Find ETDs (Neo4j)", fg="blue",
       command=lambda: findETDs("neo4j")).grid(row=0, column=2, pady=10, padx=10)

# Results List
resultsList = Listbox(root, height=25, width=100, selectmode=SINGLE)
resultsList.grid(row=1, column=0, columnspan=4, padx=10, pady=10)

# Action Buttons
Button(root, text="Go to Paper", fg="red", command=followLink).grid(row=2, column=1, pady=10)
Button(root, text="More Info", fg="red", command=showMetaData).grid(row=2, column=2, pady=10)

# Metadata Label + List
Label(root, text="Metadata:").grid(row=3, column=0, sticky=W, padx=10)
metaDataList = Listbox(root, height=15, width=100)
metaDataList.grid(row=4, column=0, columnspan=4, padx=10, pady=10)

# Run the GUI
root.mainloop()