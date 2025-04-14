import streamlit as st
import webbrowser
import sys
import os

# Add the parent directory to sys.path to find virtuoso package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from virtuoso.ETDQueries import (
    get_etd_titles, get_etd_link, get_etd_metadata,
    search_etds_by_keyword, get_etds_by_year, get_etd_count
)

st.set_page_config(page_title="ETD Explorer", layout="wide")

st.title("ETD Explorer")

# Initialize session state
if "results" not in st.session_state:
    st.session_state.results = []
if "iris" not in st.session_state:
    st.session_state.iris = []
if "metadata" not in st.session_state:
    st.session_state.metadata = []

# Display ETD count
count = get_etd_count()
st.info(f"📊 Database contains {count} ETDs")

# Sidebar inputs
st.sidebar.header("Search Options")

limit = st.sidebar.number_input("Limit", min_value=1, max_value=1000, value=100)

if st.sidebar.button("🔍 Find ETDs"):
    st.session_state.results.clear()
    st.session_state.iris.clear()
    try:
        results = get_etd_titles(limit)
        for result in results:
            st.session_state.iris.append(result["s"]["value"])
            st.session_state.results.append(result["o"]["value"])
        st.success(f"Found {len(results)} ETDs")
    except Exception as e:
        st.error(f"Error retrieving ETDs: {e}")

# Keyword Search
keyword = st.sidebar.text_input("Search by Keyword")
if st.sidebar.button("🔎 Search by Keyword"):
    st.session_state.results.clear()
    st.session_state.iris.clear()
    try:
        results = search_etds_by_keyword(keyword)
        for result in results:
            st.session_state.iris.append(result["s"]["value"])
            st.session_state.results.append(result["title"]["value"])
        st.success(f"Found {len(results)} ETDs matching '{keyword}'")
    except Exception as e:
        st.error(f"Error: {e}")

# Year Search
year = st.sidebar.text_input("Search by Year")
if st.sidebar.button("📅 Search by Year"):
    st.session_state.results.clear()
    st.session_state.iris.clear()
    try:
        results = get_etds_by_year(year)
        for result in results:
            st.session_state.iris.append(result["s"]["value"])
            st.session_state.results.append(result["title"]["value"])
        st.success(f"Found {len(results)} ETDs from year {year}")
    except Exception as e:
        st.error(f"Error: {e}")

# Display ETD results
st.subheader("ETD Results")

if st.session_state.results:
    selected_index = st.selectbox("Select an ETD", range(len(st.session_state.results)), 
                                  format_func=lambda i: st.session_state.results[i])
    
    if st.button("🔗 Open ETD Link"):
        iri = st.session_state.iris[selected_index]
        link = get_etd_link(iri)
        if link:
            st.markdown(f"[Open ETD in Browser]({link})", unsafe_allow_html=True)
        else:
            st.warning("No link available.")

    if st.button("📄 Show Metadata"):
        iri = st.session_state.iris[selected_index]
        try:
            metadata = get_etd_metadata(iri)
            if metadata:
                st.session_state.metadata = metadata
            else:
                st.warning("No metadata found.")
        except Exception as e:
            st.error(f"Error retrieving metadata: {e}")

    # Display metadata
    if st.session_state.metadata:
        st.subheader("Metadata")
        for item in st.session_state.metadata:
            st.text(item)
else:
    st.write("No ETD results to display.")
