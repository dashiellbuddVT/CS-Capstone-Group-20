import streamlit as st
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Backend modules
import virtuoso.ETDQueries as virtuoso_backend
import Neo4jaccess as neo4j_backend

# Backend selector
BACKENDS = {
    "Virtuoso": virtuoso_backend,
    "Neo4j": neo4j_backend
}

# Session state for backend
if "backend_name" not in st.session_state:
    st.session_state.backend_name = "Virtuoso"
if "backend" not in st.session_state:
    st.session_state.backend = BACKENDS[st.session_state.backend_name]

# Page setup
st.set_page_config(page_title="ETD Explorer", layout="wide")
st.title("ETD Explorer")

# 🧠 BACKEND SWITCHER
selected_backend = st.selectbox("Select Backend", list(BACKENDS.keys()), index=list(BACKENDS.keys()).index(st.session_state.backend_name))
if selected_backend != st.session_state.backend_name:
    st.session_state.backend_name = selected_backend
    st.session_state.backend = BACKENDS[selected_backend]
    st.session_state.results = []
    st.session_state.iris = []
    st.session_state.metadata = []
    st.session_state.selected_index = 0
    st.rerun()

backend = st.session_state.backend

# Session state init
for key in ["results", "iris", "metadata", "selected_index"]:
    if key not in st.session_state:
        st.session_state[key] = [] if key != "selected_index" else 0

# ETD Count
try:
    count = backend.get_etd_count()
    st.info(f"📊 {selected_backend} contains {count} ETDs")
except Exception as e:
    st.error(f"⚠️ Failed to load ETD count: {e}")
        

with st.form(key="keyword_form"):
    # Full-width keyword input
    keyword = st.text_input("Enter keyword", key="keyword_input")

    # Row with 3 columns: Field, Limit, Search button
    col1, col2, col3 = st.columns([1, 2, 2])

    with col2:
        metadata_field = st.selectbox(
            "Search in",
            ["title", "author", "advisor", "abstract", "institution", "department", "year"],
            label_visibility="collapsed",
            placeholder="Field"
        )

    with col3:
        search_limit = st.number_input("Limit", min_value=1, max_value=1000, value=100, key="keyword_limit", 
            label_visibility="collapsed")

    with col1:
        search_button = st.form_submit_button("🔍 Search")

    if search_button:
        try:
            results = backend.search_etds_by_keyword(keyword, pred=metadata_field, limit=search_limit)
            st.session_state.results = [r.get("title")["value"] for r in results]
            st.session_state.iris = [r["s"]["value"] for r in results]
            st.session_state.selected_index = 0
        except Exception as e:
            st.error(f"❌ {e}")




# ========== 📄 RESULTS ==========
st.subheader("Results")
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
        label="Select ETD",
        options=st.session_state.results,
        index=st.session_state.selected_index,
        key="etd_radio_select"
    )

    # Store the selected index
    st.session_state.selected_index = st.session_state.results.index(selected_title)
else:
    st.info("No ETDs to display. Use the search controls above to get started.")


# ========== 🧾 METADATA ==========
st.subheader("Metadata")

if st.session_state.results:
    selected_index = st.session_state.get("selected_index", 0)
    iri = st.session_state.iris[st.session_state.selected_index]

    try:
        metadata = backend.get_etd_metadata(iri)
        link = backend.get_etd_link(iri)

        if link:
            st.markdown(f"🔗 [Open ETD in Browser]({link})", unsafe_allow_html=True)
        else:
            st.markdown("🔗 No link available.")

        if metadata:
            mdDict = {}

            for item in metadata:
                key, val = item.split(":", 1) if ":" in item else ("Info", item)
                mdDict[key] = val

            if 'hasTitle' in mdDict:
                st.markdown(f"Title: {mdDict['hasTitle'].strip()}")
            if 'Author' in mdDict:
                st.markdown(f"Author: {mdDict['Author'].strip()}")
            if 'academicAdvisor' in mdDict:
                st.markdown(f"Advisor: {mdDict['academicAdvisor'].strip()}")
            if 'issuedDate' in mdDict:
                st.markdown(f"Year: {mdDict['issuedDate'].strip()}")
            if 'academicDepartment' in mdDict:
                department = os.path.basename(mdDict['academicDepartment'].strip()).replace('-',' ')
                st.markdown(f"Department: {department}")
            if 'publishedBy' in mdDict:
                school = os.path.basename(mdDict['publishedBy'].strip()).replace('-',' ')
                st.markdown(f"Institution: {school}")
            if 'hasAbstract' in mdDict:
                st.markdown(f"Abstract: {mdDict['hasAbstract'].strip()}")
        else:
            st.info("No metadata found for this ETD.")
    except Exception as e:
        st.error(f"Error retrieving metadata: {e}")
