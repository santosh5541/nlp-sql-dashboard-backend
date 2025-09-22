import re
import os
from flask import Flask, request, jsonify
from flask_cors import CORS
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_community.utilities import SQLDatabase
from langchain_deepseek import ChatDeepSeek
import mysql.connector

app = Flask(__name__)
CORS(app)  # allow React to call API

# DeepSeek API key
os.environ['DEEPSEEK_API_KEY'] = 'sk-86d752d376a64028929d2511e90dde3b'

# Connect to database
db_url = "mysql+mysqlconnector://root:Iphone5541%40123@localhost:3306/Chinook"
db = None
try:
    db = SQLDatabase.from_uri(db_url)
    print("Database connected")
except Exception as e:
    print(f"DB connection error: {e}")

def get_schema(_):
    return db.get_table_info() if db else "Database connection failed."

def run_query(query):
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Iphone5541@123",
            database="Chinook"
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        return f"Error running query: {e}"

def parse_sql_query(query_text):
    match = re.search(r"```sql(.*?)```", query_text, re.DOTALL)
    return match.group(1).strip() if match else query_text.strip()

# LangChain LLM
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

sql_chain = RunnablePassthrough.assign(schema=get_schema) | sql_prompt | llm.bind(stop=["\nSQL Result:"]) | StrOutputParser()

answer_prompt_template = """
Based on the table schema below, question, sql query, and sql response, write a natural language answer:
{schema}

Question: {question}
SQL Query: {query}
SQL Response: {response}
"""
answer_prompt = ChatPromptTemplate.from_template(answer_prompt_template)

@app.route("/ask", methods=["POST"])
def ask():
    data = request.json
    question = data.get("question")
    if not question:
        return jsonify({"error": "No question provided"}), 400

    try:
        # Step 1: Generate SQL
        sql_query = sql_chain.invoke({"question": question})
        parsed_query = parse_sql_query(sql_query)

        # Step 2: Run SQL
        sql_result = run_query(parsed_query)
        if isinstance(sql_result, str):
            return jsonify({"error": sql_result}), 500

        # Step 3: Generate natural language answer
        final_chain = (
            RunnablePassthrough.assign(schema=get_schema)
            .assign(query=lambda x: parsed_query)
            .assign(response=lambda x: sql_result)
            | answer_prompt
            | llm
        )
        final_answer = final_chain.invoke({"question": question})

        return jsonify({
            "answer": final_answer.content,
            "sql_result": sql_result
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(port=5000, debug=True)
