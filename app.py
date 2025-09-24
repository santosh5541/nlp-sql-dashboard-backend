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

app = Flask(__name__)
# Enable CORS only for your Vercel frontend
CORS(app, origins=["https://nlp-sql-dashboard.vercel.app"])

# DEEPSEEK API key
os.environ['DEEPSEEK_API_KEY'] = os.getenv('DEEPSEEK_API_KEY')

# Store current database connection and info
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

@app.route("/connect", methods=["POST"])
def connect_db():
    data = request.json
    host = data.get("host")
    port = data.get("port")  # dynamic port
    user = data.get("user")
    password = data.get("password")
    database = data.get("database")

    if not all([host, port, user, password, database]):
        return jsonify({"error": "All fields (host, port, user, password, database) are required"}), 400

    try:
        # For Aiven MySQL, enforce ssl_mode=REQUIRED
        ssl_mode = "REQUIRED" if "aivencloud.com" in host else None

        # MySQL connection
        conn = mysql.connector.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database,
            ssl_mode=ssl_mode
        )
        current_db["conn"] = conn
        current_db["db_info"] = data

        # URL-encode password for LangChain SQLDatabase
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
        else:
            return jsonify({"error": "No active database connection"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------------- Ask Question ----------------

@app.route("/ask", methods=["POST"])
def ask():
    data = request.json
    question = data.get("question")

    if not question:
        return jsonify({"error": "Missing question"}), 400

    if not current_db["conn"] or not current_db["sql_db"]:
        return jsonify({"error": "No database connected"}), 400

    try:
        # Step 1: Generate SQL using LangChain
        sql_query = sql_chain.invoke({"question": question, "schema": current_db["sql_db"].get_table_info()})
        parsed_query = parse_sql_query(sql_query)

        # Step 2: Execute SQL
        cursor = current_db["conn"].cursor(dictionary=True)
        cursor.execute(parsed_query)
        sql_result = cursor.fetchall()
        cursor.close()

        # Step 3: Generate natural language answer
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
    port = int(os.getenv("PORT", 5000))  # Dynamic port for Render
    app.run(host="0.0.0.0", port=port, debug=True)
