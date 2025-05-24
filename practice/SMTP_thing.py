import streamlit as st
import os
import sqlite3


# Initialize SQLite database
def init_db():
    conn = sqlite3.connect('source_systems.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS source_systems (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_name TEXT NOT NULL)''')
    conn.commit()
    conn.close()

init_db()


# Fetch company names from the SQLite DB
def fetch_company_names():
    conn = sqlite3.connect('source_systems.db')
    c = conn.cursor()
    c.execute("SELECT company_name FROM source_systems")
    company_names = [row[0] for row in c.fetchall()]
    conn.close()
    return company_names


# Function for login page
def login_page():
    st.title("Login Page")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        if username == "admin" and password == "admin":
            st.session_state.logged_in = True
            st.session_state.is_admin = True  # Admin user
            st.success("Login Successful!")
            st.rerun()  # Reload the app to go to the next page
        elif username != "admin" and password:  # Regular user login
            st.session_state.logged_in = True
            st.session_state.is_admin = False  # Regular user
            st.success("Login Successful!")
            st.rerun()  # Reload the app to go to the next page
        else:
            st.error("Invalid credentials. Please try again.")


# Directory scanning page
def directory_scanning_page():
    st.title("Directory Scanning Page")

    # Dropdown for selecting a source system (company name)
    company_names = fetch_company_names()
    selected_company = st.selectbox("Select a source system", company_names)
    st.write(f"Selected company: {selected_company}")

    # Option for all users to create a new source system (legacy system)
    if st.session_state.is_admin or not st.session_state.is_admin:
        new_company_name = st.text_input("Enter a new source system (legacy system)")

        if st.button("Add Source System"):
            if new_company_name:
                # Insert the new company name into the database
                conn = sqlite3.connect('source_systems.db')
                c = conn.cursor()
                c.execute("INSERT INTO source_systems (company_name) VALUES (?)", (new_company_name,))
                conn.commit()
                conn.close()
                st.success(f"Source system '{new_company_name}' added successfully!")
            else:
                st.error("Please enter a company name.")

    # Allow the user to choose a directory
    folder_path = st.text_input("Enter folder path", os.getcwd())
    if folder_path:
        try:
            # List files in the provided directory
            if os.path.isdir(folder_path):
                files = os.listdir(folder_path)
                st.write("Files in the directory:")
                st.write(files)
            else:
                st.error(f"The path {folder_path} is not a valid directory.")
        except Exception as e:
            st.error(f"Error: {e}")

    # Logout button to go back to the login page
    if st.button("Logout"):
        st.session_state.logged_in = False
        st.session_state.is_admin = False
        st.rerun()


# Main app logic
def main():
    # Initialize session state for login tracking
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
    if "is_admin" not in st.session_state:
        st.session_state.is_admin = False

    if not st.session_state.logged_in:
        login_page()  # Show the login page if not logged in
    else:
        directory_scanning_page()  # Show the directory scanning page after successful login


if __name__ == "__main__":
    main()
