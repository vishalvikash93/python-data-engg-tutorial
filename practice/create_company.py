import sqlite3


def fetch_company_names():
    conn = sqlite3.connect('source_systems.db')
    c = conn.cursor()
    c.execute("SELECT company_name FROM source_systems")
    company_names = [row[0] for row in c.fetchall()]
    conn.close()
    return company_names

def init_db():
    conn = sqlite3.connect('source_systems.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS source_systems (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_name TEXT NOT NULL)''')
    conn.commit()
    conn.close()

init_db()
