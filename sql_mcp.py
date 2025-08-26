
import sqlite3
import logging
from contextlib import closing
from typing import Dict, Any, List
from mcp.server.fastmcp import FastMCP

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize the MCP server
mcp_sqlite = FastMCP("SQLite Database Assistant")
mcp_tool = mcp_sqlite.tool

db_path = "data/my.db"

def execute_query(query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """Execute a SQL query and return results as a list of dictionaries."""
    logger.debug(f"Executing query: {query}")
    try:
        with closing(sqlite3.connect(db_path)) as conn:
            conn.row_factory = sqlite3.Row
            with closing(conn.cursor()) as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                results = [dict(row) for row in cursor.fetchall()]
                logger.debug(f"Read query returned {len(results)} rows")
                return results
    except Exception as e:
        logger.error(f"Database error executing query: {e}")
        raise

@mcp_tool()
async def list_tables() -> Dict[str, Any]:
    """
    List all tables in the SQLite database.
    
    Returns:
        Dictionary containing the list of tables and their information.
    """
    try:
        results = execute_query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row['name'] for row in results]
        
        return {
            "status": "success",
            "total_tables": len(tables),
            "tables": tables
        }
    except Exception as e:
        logger.error(f"Error listing tables: {str(e)}")
        return {
            "status": "error",
            "error_message": f"Error listing tables: {str(e)}"
        }

@mcp_tool()
async def describe_table(table_name: str) -> Dict[str, Any]:
    """
    Get the schema information for a specific table.
    
    Args:
        table_name: Name of the table to describe
        
    Returns:
        Dictionary containing table schema information.
    """
    try:
        # Get table info using PRAGMA
        results = execute_query(f"PRAGMA table_info({table_name})")
        
        if not results:
            return {
                "status": "error",
                "error_message": f"Table '{table_name}' does not exist"
            }
        
        columns = []
        for row in results:
            columns.append({
                "column_id": row['cid'],
                "name": row['name'],
                "type": row['type'],
                "not_null": bool(row['notnull']),
                "default_value": row['dflt_value'],
                "primary_key": bool(row['pk'])
            })
        
        return {
            "status": "success",
            "table_name": table_name,
            "columns": columns
        }
    except Exception as e:
        logger.error(f"Error describing table: {str(e)}")
        return {
            "status": "error",
            "error_message": f"Error describing table: {str(e)}"
        }

@mcp_tool()
async def execute_select_query(query: str) -> Dict[str, Any]:
    """
    Execute a SELECT query on the SQLite database.
    
    Args:
        query: SELECT SQL query to execute
        
    Returns:
        Dictionary containing query results.
    """
    try:
        # Validate that it's a SELECT query
        query_upper = query.strip().upper()
        if not query_upper.startswith("SELECT"):
            return {
                "status": "error",
                "error_message": "Only SELECT queries are allowed for this tool"
            }
        
        results = execute_query(query)
        
        return {
            "status": "success",
            "query": query,
            "total_rows": len(results),
            "results": results
        }
    except Exception as e:
        logger.error(f"Error executing SELECT query: {str(e)}")
        return {
            "status": "error",
            "error_message": f"Error executing query: {str(e)}"
        }

# Main function to run the server
if __name__ == "__main__":
    try:
        logger.info("Starting SQLite MCP Server")
        mcp_sqlite.run(transport="streamable-http")
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server error: {e}")
        raise