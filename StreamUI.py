import streamlit as st
import sys
import os
import json
import hashlib

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Backend modules
import VirtuosoQueries as virtuoso_backend
import Neo4jQueries as neo4j_backend

# File to store user credentials
USERS_FILE = "users.json"

# Initialize session state variables for login
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "username" not in st.session_state:
    st.session_state.username = ""

# User management functions
def load_users():
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_users(users):
    with open(USERS_FILE, 'w') as f:
        json.dump(users, f)

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def authenticate(username, password):
    users = load_users()
    if username in users and users[username]["password_hash"] == hash_password(password):
        return True
    return False

def register_user(username, password, email):
    users = load_users()
    if username in users:
        return False, "Username already exists"
    
    users[username] = {
        "password_hash": hash_password(password),
        "email": email
    }
    save_users(users)
    return True, "Registration successful"

# Page setup
st.set_page_config(page_title="ETD Explorer", layout="wide")

# Show login/register area in sidebar
with st.sidebar:
    st.title("Account")
    
    if not st.session_state.authenticated:
        tab1, tab2 = st.tabs(["Login", "Register"])
        
        with tab1:
            with st.form("login_form"):
                login_username = st.text_input("Username")
                login_password = st.text_input("Password", type="password")
                login_submit = st.form_submit_button("Login")
                
                if login_submit:
                    if authenticate(login_username, login_password):
                        st.session_state.authenticated = True
                        st.session_state.username = login_username
                        st.success("Login successful!")
                        st.rerun()
                    else:
                        st.error("Invalid username or password")
        
        with tab2:
            with st.form("register_form"):
                reg_username = st.text_input("Username")
                reg_email = st.text_input("Email")
                reg_password = st.text_input("Password", type="password")
                reg_confirm = st.text_input("Confirm Password", type="password")
                reg_submit = st.form_submit_button("Create Account")
                
                if reg_submit:
                    if not reg_username or not reg_email or not reg_password:
                        st.error("All fields are required")
                    elif reg_password != reg_confirm:
                        st.error("Passwords do not match")
                    else:
                        success, message = register_user(reg_username, reg_password, reg_email)
                        if success:
                            st.success(message)
                        else:
                            st.error(message)
    else:
        st.write(f"Logged in as: **{st.session_state.username}**")
        if st.button("Logout"):
            st.session_state.authenticated = False
            st.session_state.username = ""
            st.rerun()

# Main app content
st.title("ETD Explorer")

# Only show the rest of the app if authenticated
if st.session_state.authenticated:
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

    # Backend selector
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
            
    # Search form
    with st.form(key="keyword_form"):
        # Full-width keyword input
        keyword = st.text_input("Enter keyword", key="keyword_input")

        # Row with 3 columns: Field, Limit, Search button
        col1, col2, col3 = st.columns([1, 2, 2])

        with col2:
            metadata_field = st.selectbox(
                "Search in",
                ["title", "author", "advisor", "abstract", "institution", "department"],
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
else:
    st.info("Please log in or create an account to use ETD Explorer")