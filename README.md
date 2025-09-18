Configure .env file with "OPENAI_API_KEY ="

Sync dependencies:
> uv sync

Move to the MCP_Server folder then run:
> uv run load_csv_to_sqlite.py

> uv run sql_mcp.py

With this running open new terminal to root and execute:
> uv run --with jupyter jupyter lab
> 
