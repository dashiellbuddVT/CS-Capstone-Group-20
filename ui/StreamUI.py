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

# Add this once at the top
st.markdown("""
    <style>
        .row-widget.stRadio{
            max-height: 200px;
            overflow-y: scroll;
            border: 1px solid #ccc;
            padding-left: 10px;
        }
    </style>
    """, unsafe_allow_html=True)

# ========== 🔍 Clean, Aligned Top Bar ==========
top_bar = st.columns([2,2,2] , vertical_alignment="bottom")

with top_bar[0]:
    with st.form(key="limit_form", clear_on_submit=False):
        limit = st.number_input("Limit", min_value=1, max_value=1000, value=100)
        submitted_limit = st.form_submit_button("📥 Find ETDs")

        if submitted_limit:
            try:
                results = get_etd_titles(limit)
                st.session_state.results = [r["o"]["value"] for r in results]
                st.session_state.iris = [r["s"]["value"] for r in results]
            except Exception as e:
                st.error(f"Error: {e}")

with top_bar[1]:
    with st.form(key="keyword_form", clear_on_submit=False):
        keyword = st.text_input("Keyword")
        submitted_keyword = st.form_submit_button("🔎 Search")

        if submitted_keyword:
            try:
                results = search_etds_by_keyword(keyword)
                st.session_state.results = [r["title"]["value"] for r in results]
                st.session_state.iris = [r["s"]["value"] for r in results]
            except Exception as e:
                st.error(f"Error: {e}")


with top_bar[2]:
    with st.form(key="year_form", clear_on_submit=False):
        year = st.text_input("Year")
        submitted_year = st.form_submit_button("📅 Search")

        if submitted_year:
            try:
                results = get_etds_by_year(year)
                st.session_state.results = [r["title"]["value"] for r in results]
                st.session_state.iris = [r["s"]["value"] for r in results]
            except Exception as e:
                st.error(f"Error: {e}")




# ========== 📄 Results (Simulated Scrollable) ==========
st.markdown(
    "<h3 style='margin-bottom: 0.2rem; margin-top: 0;'>Results</h3>",
    unsafe_allow_html=True
)

if st.session_state.results:

    # Scrollable radio list using custom CSS
    st.markdown("""
        <style>
        /* Scrollable container for the radio buttons */
        div[data-testid="stRadio"] > div {
            max-height: 300px;
            overflow-y: auto;
            overflow-x: hidden;
            border: 1px solid #ddd;
            padding-left: 10px;
            padding-right: 10px;
            display: block !important;
        }

        /* Each radio option in its own row with aligned button + label */
        div[data-testid="stRadio"] label {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            margin-bottom: 0.3rem;
            white-space: normal !important;
            word-break: break-word;
        }
        </style>
    """, unsafe_allow_html=True)


    st.markdown(
        f"<p style='font-size: 0.85rem; color: gray; margin: 0;'>Showing {len(st.session_state.results)} results</p>",
        unsafe_allow_html=True
    )

    selected_title = st.radio(
        "",
        st.session_state.results if isinstance(st.session_state.results,list) else list(st.session_state.results),
        key="etd_radio_select",
        label_visibility="collapsed",
        index=st.session_state.get("selected_index", 0)
    )

    # Store the selected index
    st.session_state.selected_index = st.session_state.results.index(selected_title)
else:
    st.info("No ETDs to display. Use the search controls above to get started.")

# ========== 🧾 Metadata ==========
st.subheader("Metadata")

if st.session_state.results:
    selected_index = st.session_state.get("selected_index", 0)
    iri = st.session_state.iris[selected_index]

    try:
        metadata = get_etd_metadata(iri)
        link = get_etd_link(iri)

        if link:
            st.markdown(f"🔗 [Open ETD in Browser]({link})", unsafe_allow_html=True)
        else:
            st.markdown("🔗 No link available.")

        if metadata:
            mdDict = {}
            for item in metadata:
                key, val = item.split(":", 1) if ":" in item else ("Info", item)
                mdDict[key] = val

            st.markdown(f"Title: {mdDict['hasTitle'].strip()}")
            st.markdown(f"Author: {mdDict['Author'].strip()}")
            st.markdown(f"Year: {mdDict['issuedDate'].strip()}")
            school = os.path.basename(mdDict['publishedBy'].strip()).replace('-','')
            st.markdown(f"Institution: {school}")
            st.markdown(f"Abstract: {mdDict['hasAbstract'].strip()}")
        else:
            st.info("No metadata found for this ETD.")
    except Exception as e:
        st.error(f"Error retrieving metadata: {e}")