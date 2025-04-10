from tkinter import *
import tkinter.ttk as ttk
import webbrowser

# BACKEND SWITCHING
import ETDQueries as virtuoso_backend
import Neo4jQueries as neo4j_backend

# Choose backend at runtime
BACKEND = None
backend_module = None

# Root window
root = Tk()
root.title("ETD Explorer")
root.geometry('950x900')

# Store the IRIs for selected ETDs
results_iris = []


def set_backend(selected):
    global BACKEND, backend_module
    BACKEND = selected
    backend_module = virtuoso_backend if selected == "Virtuoso" else neo4j_backend
    status_label.config(text=f"Switched to {BACKEND} backend")
    display_count()


def display_count():
    if BACKEND is None:
        return
    try:
        count = backend_module.get_etd_count()
        status_label.config(text=f"{BACKEND} database contains {count} ETDs")
    except:
        status_label.config(text=f"{BACKEND} database count unavailable")


def find_etds():
    if BACKEND is None:
        status_label.config(text="Select a backend first!")
        return
    result_list.delete(0, END)
    metadata_list.delete(0, END)
    results_iris.clear()
    try:
        limit = limit_entry.get()
        limit = int(limit) if limit.strip() else 100
        results = backend_module.get_etd_titles(limit)
        for result in results:
            results_iris.append(result["s"]["value"])
            result_list.insert(END, result["o"]["value"] if "o" in result else result["title"]["value"])
        status_label.config(text=f"Found {len(results)} ETDs")
    except Exception as e:
        result_list.insert(END, f"Error: {str(e)}")
        status_label.config(text="Error retrieving ETDs")


def search_by_keyword():
    if BACKEND is None:
        status_label.config(text="Select a backend first!")
        return
    result_list.delete(0, END)
    metadata_list.delete(0, END)
    results_iris.clear()
    keyword = search_entry.get().strip()
    if not keyword:
        status_label.config(text="Please enter a keyword to search")
        return
    try:
        results = backend_module.search_etds_by_keyword(keyword)
        for result in results:
            results_iris.append(result["s"]["value"])
            result_list.insert(END, result["title"]["value"])
        status_label.config(text=f"Found {len(results)} ETDs matching '{keyword}'")
    except Exception as e:
        result_list.insert(END, f"Error: {str(e)}")
        status_label.config(text="Error searching ETDs")


def search_by_year():
    if BACKEND is None:
        status_label.config(text="Select a backend first!")
        return
    result_list.delete(0, END)
    metadata_list.delete(0, END)
    results_iris.clear()
    
    year = str(year_entry.get()).strip()
    if not year:
        status_label.config(text="Please enter a year to search")
        return
    
    try:
        results = backend_module.get_etds_by_year(year)
        for result in results:
            results_iris.append(result["s"]["value"])
            result_list.insert(END, result["title"]["value"])
        status_label.config(text=f"Found {len(results)} ETDs from year {year}")
    except Exception as e:
        result_list.insert(END, f"Error: {str(e)}")
        status_label.config(text="Error searching ETDs by year")



def open_link():
    if not results_iris:
        return
    index = result_list.curselection()[0]
    iri = results_iris[index]
    link = backend_module.get_etd_link(iri)
    if link:
        webbrowser.open_new_tab(link)


def show_metadata():
    metadata_list.delete(0, END)
    index = result_list.curselection()[0]
    iri = results_iris[index]
    try:
        metadata = backend_module.get_etd_metadata(iri)
        for item in metadata:
            metadata_list.insert(END, item)
    except Exception as e:
        metadata_list.insert(END, f"Error: {str(e)}")


# MAIN UI
main_frame = Frame(root, padx=10, pady=10)
main_frame.pack(fill=BOTH, expand=True)

# Backend Selection
Label(main_frame, text="Select Backend:").pack()
backend_dropdown = ttk.Combobox(main_frame, values=["Virtuoso", "Neo4j"], state="readonly")
backend_dropdown.pack()
backend_dropdown.bind("<<ComboboxSelected>>", lambda e: set_backend(backend_dropdown.get()))

# Top Controls
top_frame = Frame(main_frame)
top_frame.pack(fill=X, pady=5)
Label(top_frame, text="Limit:").grid(row=0, column=0)
limit_entry = Entry(top_frame, width=8)
limit_entry.grid(row=0, column=1)
limit_entry.insert(0, "100")
Button(top_frame, text="Find ETDs", fg="green", command=find_etds).grid(row=0, column=2, padx=10)
Label(top_frame, text="Search:").grid(row=0, column=3)
search_entry = Entry(top_frame, width=20)
search_entry.grid(row=0, column=4)
Button(top_frame, text="Search by Keyword", fg="blue", command=search_by_keyword).grid(row=0, column=5, padx=10)
Label(top_frame, text="Year:").grid(row=0, column=6)
year_entry = Entry(top_frame, width=8)
year_entry.grid(row=0, column=7)
Button(top_frame, text="Search by Year", fg="purple", command=search_by_year).grid(row=0, column=8, padx=10)

# Results list
middle_frame = Frame(main_frame)
middle_frame.pack(fill=BOTH, expand=True, pady=5)
Label(middle_frame, text="ETD Results:").pack(anchor=W)
result_frame = Frame(middle_frame)
result_frame.pack(fill=BOTH, expand=True)
scrollbar = Scrollbar(result_frame)
scrollbar.pack(side=RIGHT, fill=Y)
result_list = Listbox(result_frame, height=15, width=100, selectmode=SINGLE, yscrollcommand=scrollbar.set)
result_list.pack(side=LEFT, fill=BOTH, expand=True)
scrollbar.config(command=result_list.yview)

# Buttons
button_frame = Frame(main_frame)
button_frame.pack(fill=X, pady=5)
Button(button_frame, text="Open ETD Link", fg="red", command=open_link).pack(side=LEFT, padx=10)
Button(button_frame, text="Show Metadata", fg="dark green", command=show_metadata).pack(side=LEFT, padx=10)

# Metadata
bottom_frame = Frame(main_frame)
bottom_frame.pack(fill=BOTH, expand=True, pady=5)
Label(bottom_frame, text="Metadata:").pack(anchor=W)
metadata_frame = Frame(bottom_frame)
metadata_frame.pack(fill=BOTH, expand=True)
metadata_scrollbar = Scrollbar(metadata_frame)
metadata_scrollbar.pack(side=RIGHT, fill=Y)
metadata_list = Listbox(metadata_frame, height=10, width=100, yscrollcommand=metadata_scrollbar.set)
metadata_list.pack(side=LEFT, fill=BOTH, expand=True)
metadata_scrollbar.config(command=metadata_list.yview)

# Status
status_label = Label(root, text="Ready", bd=1, relief=SUNKEN, anchor=W)
status_label.pack(side=BOTTOM, fill=X)

root.mainloop()
