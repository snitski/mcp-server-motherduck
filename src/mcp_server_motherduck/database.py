import os
import duckdb
from typing import Literal, Optional
import io
from contextlib import redirect_stdout
from tabulate import tabulate
import json
import logging
from .configs import SERVER_VERSION

logger = logging.getLogger("mcp_server_motherduck")


class DatabaseClient:
    def __init__(
        self,
        db_path: str | None = None,
        motherduck_token: str | None = None,
        home_dir: str | None = None,
        saas_mode: bool = False,
        read_only: bool = False,
        json_output: bool = False,
    ):
        self._read_only = read_only
        self._json_output = json_output
        self.db_path, self.db_type = self._resolve_db_path_type(
            db_path, motherduck_token, saas_mode
        )
        logger.info(f"Database client initialized in `{self.db_type}` mode")

        # Set the home directory for DuckDB
        if home_dir:
            os.environ["HOME"] = home_dir

        self.conn = self._initialize_connection()

    def _initialize_connection(self) -> Optional[duckdb.DuckDBPyConnection]:
        """Initialize connection to the MotherDuck or DuckDB database"""

        logger.info(f"🔌 Connecting to {self.db_type} database")

        # S3 and R2 databases don't support read-only mode
        if self.db_type in ("s3", "r2") and self._read_only:
            raise ValueError(f"Read-only mode is not supported for {self.db_type.upper()} databases")

        if self.db_type == "duckdb" and self._read_only:
            # check that we can connect, issue a `select 1` and then close + return None
            try:
                conn = duckdb.connect(
                    self.db_path,
                    config={
                        "custom_user_agent": f"mcp-server-motherduck/{SERVER_VERSION}"
                    },
                    read_only=self._read_only,
                )
                conn.execute("SELECT 1")
                conn.close()
                return None
            except Exception as e:
                logger.error(f"❌ Read-only check failed: {e}")
                raise

        # Check if this is an S3 or R2 path
        if self.db_type in ("s3", "r2"):
            # For S3/R2, we need to create an in-memory connection and attach the database
            conn = duckdb.connect(':memory:')
            
            # Install and load the httpfs extension for S3/R2 support
            import io
            from contextlib import redirect_stdout, redirect_stderr
            
            null_file = io.StringIO()
            with redirect_stdout(null_file), redirect_stderr(null_file):
                try:
                    conn.execute("INSTALL httpfs;")
                except:
                    pass  # Extension might already be installed
                conn.execute("LOAD httpfs;")
            
            if self.db_type == "s3":
                # Configure S3 credentials from environment variables using CREATE SECRET
                aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
                aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
                aws_region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
                
                if aws_access_key and aws_secret_key:
                    # Use CREATE SECRET for better credential management
                    conn.execute(f"""
                        CREATE SECRET IF NOT EXISTS s3_secret (
                            TYPE S3,
                            KEY_ID '{aws_access_key}',
                            SECRET '{aws_secret_key}',
                            REGION '{aws_region}'
                        );
                    """)
            elif self.db_type == "r2":
                # Configure R2 credentials from environment variables using CREATE SECRET
                r2_access_key = os.environ.get('R2_ACCESS_KEY_ID')
                r2_secret_key = os.environ.get('R2_SECRET_ACCESS_KEY')
                r2_account_id = os.environ.get('R2_ACCOUNT_ID')
                
                if r2_access_key and r2_secret_key and r2_account_id:
                    # Use CREATE SECRET for R2 with ACCOUNT_ID instead of REGION
                    conn.execute(f"""
                        CREATE SECRET IF NOT EXISTS r2_secret (
                            TYPE r2,
                            KEY_ID '{r2_access_key}',
                            SECRET '{r2_secret_key}',
                            ACCOUNT_ID '{r2_account_id}'
                        );
                    """)
            
            # Attach the S3/R2 database
            db_alias = "s3db" if self.db_type == "s3" else "r2db"
            try:
                # For S3/R2, we always attach as READ_ONLY since object storage is typically read-only
                # Even when not in read_only mode, we attach as READ_ONLY for S3/R2
                conn.execute(f"ATTACH '{self.db_path}' AS {db_alias};")
                # Use the attached database
                conn.execute(f"USE {db_alias};")
                logger.info(f"✅ Successfully connected to {self.db_type.upper()} database (attached as read-only)")
            except Exception as e:
                logger.error(f"Failed to attach {self.db_type.upper()} database: {e}")
                # If the database doesn't exist and we're not in read-only mode, try to create it
                if "database does not exist" in str(e) and not self._read_only:
                    logger.info(f"{self.db_type.upper()} database doesn't exist, attempting to create it...")
                    try:
                        # Create a new database at the S3/R2 location
                        conn.execute(f"ATTACH '{self.db_path}' AS {db_alias};")
                        conn.execute(f"USE {db_alias};")
                        logger.info(f"✅ Created new {self.db_type.upper()} database at {self.db_path}")
                    except Exception as create_error:
                        logger.error(f"Failed to create {self.db_type.upper()} database: {create_error}")
                        raise
                else:
                    raise
                
            return conn

        conn = duckdb.connect(
            self.db_path,
            config={"custom_user_agent": f"mcp-server-motherduck/{SERVER_VERSION}"},
            read_only=self._read_only,
        )

        logger.info(f"✅ Successfully connected to {self.db_type} database")

        return conn

    def _resolve_db_path_type(
        self, db_path: str, motherduck_token: str | None = None, saas_mode: bool = False
    ) -> tuple[str, Literal["duckdb", "motherduck", "s3", "r2"]]:
        """Resolve and validate the database path"""
        # Handle R2 paths (check before S3 since r2:// is more specific)
        if db_path.startswith("r2://"):
            return db_path, "r2"
        
        # Handle S3 paths
        if db_path.startswith("s3://"):
            return db_path, "s3"
        
        # Handle MotherDuck paths
        if db_path.startswith("md:"):
            if motherduck_token:
                logger.info("Using MotherDuck token to connect to database `md:`")
                if saas_mode:
                    logger.info("Connecting to MotherDuck in SaaS mode")
                    return (
                        f"{db_path}?motherduck_token={motherduck_token}&saas_mode=true",
                        "motherduck",
                    )
                else:
                    return (
                        f"{db_path}?motherduck_token={motherduck_token}",
                        "motherduck",
                    )
            elif os.getenv("motherduck_token"):
                logger.info(
                    "Using MotherDuck token from env to connect to database `md:`"
                )
                return (
                    f"{db_path}?motherduck_token={os.getenv('motherduck_token')}",
                    "motherduck",
                )
            else:
                raise ValueError(
                    "Please set the `motherduck_token` as an environment variable or pass it as an argument with `--motherduck-token` when using `md:` as db_path."
                )

        if db_path == ":memory:":
            return db_path, "duckdb"

        return db_path, "duckdb"

    def _execute(self, query: str) -> str:
        if self.conn is None:
            # open short lived readonly connection for local DuckDB, run query, close connection, return result
            conn = duckdb.connect(
                self.db_path,
                config={"custom_user_agent": f"mcp-server-motherduck/{SERVER_VERSION}"},
                read_only=self._read_only,
            )
            q = conn.execute(query)
        else:
            q = self.conn.execute(query)

        # Fetch rows and get column info before checking output format
        rows = q.fetchall()
        columns = [d[0] for d in q.description]
        column_types = [d[1] for d in q.description]

        if self._json_output:
            # Return JSON output
            result = [dict(zip(columns, row)) for row in rows]
            out = json.dumps(result, indent=2)
        else:
            # Return tabulated output
            headers = [col + "\n" + str(col_type) for col, col_type in zip(columns, column_types)]
            out = tabulate(rows, headers=headers, tablefmt="pretty")

        if self.conn is None:
            conn.close()

        return out

    def query(self, query: str) -> str:
        try:
            return self._execute(query)

        except Exception as e:
            raise ValueError(f"❌ Error executing query: {e}")
