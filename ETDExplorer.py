from tkinter import *
import tkinter.ttk as ttk
import webbrowser
from ETDQueries import get_etd_titles, get_etd_link, get_etd_metadata
from ETDQueries import search_etds_by_keyword, get_etds_by_year, get_etd_count

# Root window
root = Tk()
root.title("ETD Explorer")
root.geometry('950x900')

# Store the IRIs for selected ETDs
results_iris = []

def display_count():
    """Display the count of ETDs in the status bar"""
    count = get_etd_count()
    status_label.config(text=f"Database contains {count} ETDs")

def find_etds():
    """Retrieve and display ETD titles"""
    result_list.delete(0, END)
    metadata_list.delete(0, END)
    results_iris.clear()
    
    try:
        # Get the limit from the entry field
        limit = limit_entry.get()
        limit = int(limit) if limit.strip() else 100
        
        # Get ETD titles
        results = get_etd_titles(limit)
        
        if results:
            for result in results:
                results_iris.append(result["s"]["value"])
                result_list.insert(END, result["o"]["value"])
            status_label.config(text=f"Found {len(results)} ETDs")
        else:
            status_label.config(text="No ETDs found")
    except Exception as e:
        result_list.insert(END, f"Error: {str(e)}")
        status_label.config(text="Error retrieving ETDs")

def search_by_keyword():
    """Search ETDs by keyword"""
    result_list.delete(0, END)
    metadata_list.delete(0, END)
    results_iris.clear()
    
    keyword = search_entry.get().strip()
    if not keyword:
        status_label.config(text="Please enter a keyword to search")
        return
    
    try:
        results = search_etds_by_keyword(keyword)
        
        if results:
            for result in results:
                results_iris.append(result["s"]["value"])
                result_list.insert(END, result["title"]["value"])
            status_label.config(text=f"Found {len(results)} ETDs matching '{keyword}'")
        else:
            status_label.config(text=f"No ETDs found matching '{keyword}'")
    except Exception as e:
        result_list.insert(END, f"Error: {str(e)}")
        status_label.config(text="Error searching ETDs")

def search_by_year():
    """Search ETDs by year"""
    result_list.delete(0, END)
    metadata_list.delete(0, END)
    results_iris.clear()
    
    year = year_entry.get().strip()
    if not year:
        status_label.config(text="Please enter a year to search")
        return
    
    try:
        results = get_etds_by_year(year)
        
        if results:
            for result in results:
                results_iris.append(result["s"]["value"])
                result_list.insert(END, result["title"]["value"])
            status_label.config(text=f"Found {len(results)} ETDs from year {year}")
        else:
            status_label.config(text=f"No ETDs found from year {year}")
    except Exception as e:
        result_list.insert(END, f"Error: {str(e)}")
        status_label.config(text="Error searching ETDs by year")

def open_link():
    """Open the ETD link in a browser"""
    selection = result_list.curselection()
    if not selection:
        status_label.config(text="Please select an ETD first")
        return
    
    index = selection[0]
    if index >= len(results_iris):
        status_label.config(text="Invalid selection")
        return
    
    iri = results_iris[index]
    link = get_etd_link(iri)
    
    if link:
        webbrowser.open_new_tab(link)
        status_label.config(text=f"Opened link: {link}")
    else:
        status_label.config(text="No link available for this ETD")

def show_metadata():
    """Show metadata for the selected ETD"""
    selection = result_list.curselection()
    if not selection:
        status_label.config(text="Please select an ETD first")
        return
    
    index = selection[0]
    if index >= len(results_iris):
        status_label.config(text="Invalid selection")
        return
    
    iri = results_iris[index]
    metadata_list.delete(0, END)
    
    try:
        metadata = get_etd_metadata(iri)
        
        if metadata:
            for item in metadata:
                metadata_list.insert(END, item)
            status_label.config(text=f"Showing metadata for selected ETD")
        else:
            status_label.config(text="No metadata available for this ETD")
    except Exception as e:
        metadata_list.insert(END, f"Error: {str(e)}")
        status_label.config(text="Error retrieving metadata")

# Create main frame
main_frame = Frame(root, padx=10, pady=10)
main_frame.pack(fill=BOTH, expand=True)

# Create top frame for controls
top_frame = Frame(main_frame)
top_frame.pack(fill=X, pady=5)

# Create entries and labels
Label(top_frame, text="Limit:").grid(row=0, column=0, padx=5, pady=5)
limit_entry = Entry(top_frame, width=8)
limit_entry.grid(row=0, column=1, padx=5, pady=5)
limit_entry.insert(0, "100")

Button(top_frame, text="Find ETDs", fg="green", command=find_etds).grid(row=0, column=2, padx=10, pady=5)

Label(top_frame, text="Search:").grid(row=0, column=3, padx=5, pady=5)
search_entry = Entry(top_frame, width=20)
search_entry.grid(row=0, column=4, padx=5, pady=5)

Button(top_frame, text="Search by Keyword", fg="blue", command=search_by_keyword).grid(row=0, column=5, padx=10, pady=5)

Label(top_frame, text="Year:").grid(row=0, column=6, padx=5, pady=5)
year_entry = Entry(top_frame, width=8)
year_entry.grid(row=0, column=7, padx=5, pady=5)

Button(top_frame, text="Search by Year", fg="purple", command=search_by_year).grid(row=0, column=8, padx=10, pady=5)

# Create middle frame for results
middle_frame = Frame(main_frame)
middle_frame.pack(fill=BOTH, expand=True, pady=5)

# Results list with scrollbar
Label(middle_frame, text="ETD Results:").pack(anchor=W)
result_frame = Frame(middle_frame)
result_frame.pack(fill=BOTH, expand=True)

scrollbar = Scrollbar(result_frame)
scrollbar.pack(side=RIGHT, fill=Y)

result_list = Listbox(result_frame, height=15, width=100, selectmode=SINGLE, yscrollcommand=scrollbar.set)
result_list.pack(side=LEFT, fill=BOTH, expand=True)
scrollbar.config(command=result_list.yview)

# Create button frame
button_frame = Frame(main_frame)
button_frame.pack(fill=X, pady=5)

Button(button_frame, text="Open ETD Link", fg="red", command=open_link).pack(side=LEFT, padx=10)
Button(button_frame, text="Show Metadata", fg="dark green", command=show_metadata).pack(side=LEFT, padx=10)

# Metadata frame
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

# Status bar
status_label = Label(root, text="Ready", bd=1, relief=SUNKEN, anchor=W)
status_label.pack(side=BOTTOM, fill=X)

# Initialize by displaying ETD count
display_count()

# Start the GUI
root.mainloop() 