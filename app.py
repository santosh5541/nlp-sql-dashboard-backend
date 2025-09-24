import re
import os
from urllib.parse import quote_plus
from flask import Flask, request, jsonify
from flask_cors import CORS
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_community.utilities import SQLDatabase
from langchain_deepseek import ChatDeepSeek
import mysql.connector
import tempfile

app = Flask(__name__)
CORS(app, origins=["https://nlp-sql-dashboard.vercel.app"])

# DEEPSEEK API key
os.environ['DEEPSEEK_API_KEY'] = os.getenv('DEEPSEEK_API_KEY')

current_db = {
    "conn": None,
    "db_info": None,
    "sql_db": None
}

def parse_sql_query(query_text):
    match = re.search(r"```sql(.*?)```", query_text, re.DOTALL)
    return match.group(1).strip() if match else query_text.strip()

# LLM
llm = ChatDeepSeek(model="deepseek-chat", timeout=60)

sql_prompt_template = """
You are a SQL expert. Using the schema below, write a SQL query to answer the question.
Do not add unnecessary WHERE clauses. Only generate the SQL query.
Schema:
{schema}

Question: {question}
SQL Query:
"""
sql_prompt = ChatPromptTemplate.from_template(sql_prompt_template)
sql_chain = RunnablePassthrough() | sql_prompt | llm.bind(stop=["\nSQL Result:"]) | StrOutputParser()

answer_prompt_template = """
Based on the table schema below, question, sql query, and sql response, write a natural language answer:
{schema}

Question: {question}
SQL Query: {query}
SQL Response: {response}
"""
answer_prompt = ChatPromptTemplate.from_template(answer_prompt_template)

# ---------------- Connect / Disconnect ----------------

AIVEN_CERT = """-----BEGIN CERTIFICATE-----
MIIEUDCCArigAwIBAgIUO5hBU4yU9a9AKV98Hxr7Gg461U0wDQYJKoZIhvcNAQEM
BQAwQDE+MDwGA1UEAww1YTFkMDY5MGItMWEzMi00MmQ0LTg0YmUtYmZmODQwZGY2
ODYxIEdFTiAxIFByb2plY3QgQ0EwHhcNMjUwOTI0MDYzNjQ4WhcNMzUwOTIyMDYz
NjQ4WjBAMT4wPAYDVQQDDDVhMWQwNjkwYi0xYTMyLTQyZDQtODRiZS1iZmY4NDBk
ZjY4NjEgR0VOIDEgUHJvamVjdCBDQTCCAaIwDQYJKoZIhvcNAQEBBQADggGPADCC
AYoCggGBAJUZbtQoOsMN/b9JSuoNayJ4nzlKSf2yeCne4N31OtvdijuubPdMGl8x
a+PCGVYx6mlMt9aGuBqA3fBwOX3O3LaEgR1w26uu9dYID2bgat56T8QDEdVzISxy
zejSYeoywxYjnSNw2pkM9doCtDcuX8P4RROqnGZySyBH6TgHo1WXZdfnzDCfsSqF
hLCf9qi/V9zrfra8Qbw3fgcblKWPrAkY2e23NsvrUa+yYZJ3WpTyEWEZHbcN8YQF
SRt8IeqSwyQLHrZMUfToZCsgzzUMEWobUVQHJrGVEAW14vCEtuxAaX+gtxfLeMZP
FPnMd3vrDJOe455XcIVslhT1q3BC3QDRmEGT9Bjylj80dnVqym2C332fchmDSs+2
Im+aWCF7AaXx9gYFUh3L0hzLXG3el+YGUYsyxf7y0XvOUBJc3Zf83oBxO6+im32y
tgrJCNPCp1u62HTwygFnDzdUVRwxhBmwNF2WWRMJtB1JMlMVNOYWkhv5AG7kfLqD
/OhlFP+IRwIDAQABo0IwQDAdBgNVHQ4EFgQUQKvALWFm72wSn3YDGR7YFtD/EC4w
EgYDVR0TAQH/BAgwBgEB/wIBADALBgNVHQ8EBAMCAQYwDQYJKoZIhvcNAQEMBQAD
ggGBACeN8fMhONCHNKt/WkfsXTq8DMJ/JValNWJ8hb3xzijBZpmrRxRJVnm3rwv7
eeel8U4GB/teDUnc0+aVkHER5xNREM9nYYHQHJFFnTvwFkz5fK6GjwSEBh4bcUvK
ETxNPO5UdU+TCaWsqNY5UnYOvBBcEHwjpPeTM8oRF87E+ZLWCjD+NfrK03soRGfn
zQex0d7PY2QxWBO+Ez6C1OmJ/57pGulnYgKEoCPPmlKCsGI/O/P5z3EWLExfCJw7
289DZ4sNtIP8F2+xC/EyyAZJFY+YBMvjiwPHrCN5bc3RCTHQUknRHstwIbQ7/6x3
g9u8YrD8M5qT+m33a1T0EzzpWEj1cpsMY5em8ufxH+Iw2MFcGpg/f7HYaNKH0MqW
/BdTeZdM737RZpqb2q/lZAcIVLCPdVhqdAIVJS83YHD+dU/gkXa2CHemnjvekRvi
fwhyv1suxp2R+E5kq5LAPTCKxi0EGAFpo1/Yukk6I6Z8TIiq1S34ke2FIvKi28GF
FcUJkg==
-----END CERTIFICATE-----"""

@app.route("/connect", methods=["POST"])
def connect_db():
    data = request.json
    host = data.get("host")
    port = data.get("port", 3306)
    user = data.get("user")
    password = data.get("password")
    database = data.get("database")

    if not all([host, user, password, database]):
        return jsonify({"error": "All fields are required"}), 400

    try:
        # If Aiven, write temp cert file
        ssl_args = None
        if "aivencloud.com" in host:
            port = 26512
            temp_cert = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
            temp_cert.write(AIVEN_CERT.encode())
            temp_cert.flush()
            ssl_args = {"ssl_ca": temp_cert.name}

        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            **(ssl_args or {})
        )

        current_db["conn"] = conn
        current_db["db_info"] = data
        password_encoded = quote_plus(password)
        db_uri = f"mysql+mysqlconnector://{user}:{password_encoded}@{host}:{port}/{database}"
        current_db["sql_db"] = SQLDatabase.from_uri(db_uri)

        return jsonify({"message": f"Connected to database {database}"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/disconnect", methods=["POST"])
def disconnect_db():
    try:
        if current_db["conn"]:
            current_db["conn"].close()
            db_name = current_db["db_info"]["database"]
            current_db["conn"] = None
            current_db["db_info"] = None
            current_db["sql_db"] = None
            return jsonify({"message": f"Disconnected from database {db_name}"})
        return jsonify({"error": "No active database connection"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/ask", methods=["POST"])
def ask():
    data = request.json
    question = data.get("question")

    if not question or not current_db["conn"] or not current_db["sql_db"]:
        return jsonify({"error": "No database connected or question missing"}), 400

    try:
        sql_query = sql_chain.invoke({"question": question, "schema": current_db["sql_db"].get_table_info()})
        parsed_query = parse_sql_query(sql_query)
        cursor = current_db["conn"].cursor(dictionary=True)
        cursor.execute(parsed_query)
        sql_result = cursor.fetchall()
        cursor.close()

        final_chain = (
            RunnablePassthrough.assign(schema=lambda _: current_db["sql_db"].get_table_info())
            .assign(query=lambda _: parsed_query)
            .assign(response=lambda _: sql_result)
            | answer_prompt
            | llm
        )
        final_answer = final_chain.invoke({"question": question})

        return jsonify({"answer": final_answer.content, "sql_result": sql_result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
