import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("Spotify App")

messages = st.container()

if prompt := st.chat_input("Ask something"):
    messages.chat_message("user").write(prompt)

    # answer_query = f"""
    # SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER(description,
    #     '{prompt}') as response, description
    # FROM episodes_final
    # """

    prompt_query = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'llama2-70b-chat',
            CONCAT('{prompt}', name)
    ) as response, id, name, description FROM episodes_final LIMIT 10
    """

    run = session.sql(prompt_query)
    
    messages.chat_message("assistant").dataframe(run)