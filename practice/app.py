import streamlit as st
import os


# Simulate a user authentication function (you can replace it with a real one)
def authenticate(username, password):
    # Replace with actual username and password validation
    return username == "admin" and password == "admin"


# Directory scanning function
def scan_directory(directory_path):
    if not os.path.exists(directory_path):
        return "Directory not found."

    # List files and directories in the provided path
    files = os.listdir(directory_path)
    return files


# Streamlit UI
def main():
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
    if not st.session_state.logged_in:
        st.title("Login Page")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.button("Login"):
            if authenticate(username, password):
                st.session_state.logged_in = True
                st.rerun()
            else:
                st.error("Invalid username or password")

    # Directory scanning page
    if st.session_state.logged_in:
        st.title("Directory Scanning App")

        directory_path = st.text_input("Enter directory path", value="")

        if st.button("Scan Directory"):
            if directory_path:
                files = scan_directory(directory_path)
                if isinstance(files, str):
                    st.error(files)
                else:
                    st.write("Files in directory:")
                    for file in files:
                        st.write(file)
            else:
                st.warning("Please enter a directory path.")


if __name__ == "__main__":
    main()
