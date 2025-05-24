def admin_page():
    st.title("Admin Page - Add New Source System")

    new_company_name = st.text_input("Enter new source system (company name)")

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

    if st.button("Back to Scanning Page"):
        st.session_state.page = "scanning"
        st.experimental_rerun()  # Go back to the scanning page
