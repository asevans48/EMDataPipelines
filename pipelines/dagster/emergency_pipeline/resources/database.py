"""
Database Resources for Emergency Management Pipeline
StarRocks connection management with federal compliance
"""

import pymysql
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, Any, List, Optional

from dagster import (
    ConfigurableResource,
    get_dagster_logger,
    EnvVar,
)
from pydantic import Field


class StarRocksResource(ConfigurableResource):
    """
    StarRocks database resource for emergency management data
    Handles both public and tenant-isolated data access
    """
    
    host: str = Field(description="StarRocks FE host")
    port: int = Field(default=9030, description="StarRocks FE port")
    user: str = Field(description="Database user")
    password: str = Field(default="", description="Database password")
    database: str = Field(description="Database name")
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = get_dagster_logger()
    
    @contextmanager
    def get_connection(self):
        """Get database connection with proper error handling"""
        connection = None
        try:
            connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4',
                autocommit=False,
                read_timeout=30,
                write_timeout=30,
            )
            self.logger.debug(f"Connected to StarRocks: {self.host}:{self.port}/{self.database}")
            yield connection
            
        except Exception as e:
            self.logger.error(f"Database connection error: {str(e)}")
            raise
        finally:
            if connection:
                connection.close()
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Any]:
        """Execute query and return results"""
        with self.get_connection() as conn:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            try:
                self.logger.debug(f"Executing query: {query[:100]}...")
                cursor.execute(query, params)
                
                if query.strip().upper().startswith('SELECT'):
                    results = cursor.fetchall()
                    self.logger.debug(f"Query returned {len(results)} rows")
                    return results
                else:
                    conn.commit()
                    rows_affected = cursor.rowcount
                    self.logger.debug(f"Query affected {rows_affected} rows")
                    return rows_affected
                    
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Query execution error: {str(e)}")
                raise
            finally:
                cursor.close()
    
    def bulk_insert(self, table: str, data: List[Dict[str, Any]], 
                   batch_size: int = 1000) -> int:
        """Bulk insert data with batching for performance"""
        if not data:
            return 0
        
        total_inserted = 0
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Process in batches
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    
                    # Add audit columns
                    for record in batch:
                        record.update({
                            'ingestion_timestamp': datetime.now(),
                            'data_classification': record.get('data_classification', 'PUBLIC'),
                        })
                    
                    # Generate bulk insert SQL
                    columns = list(batch[0].keys())
                    placeholders = ', '.join(['%s'] * len(columns))
                    
                    insert_sql = f"""
                    INSERT INTO {table} ({', '.join(columns)})
                    VALUES ({placeholders})
                    ON DUPLICATE KEY UPDATE
                    ingestion_timestamp = VALUES(ingestion_timestamp)
                    """
                    
                    # Prepare values
                    values = [[record.get(col) for col in columns] for record in batch]
                    
                    cursor.executemany(insert_sql, values)
                    batch_inserted = cursor.rowcount
                    total_inserted += batch_inserted
                    
                    self.logger.debug(f"Inserted batch {i//batch_size + 1}: {batch_inserted} records")
                
                conn.commit()
                self.logger.info(f"Bulk insert completed: {total_inserted} records into {table}")
                
                return total_inserted
                
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Bulk insert error: {str(e)}")
                raise
            finally:
                cursor.close()
    
    def create_table_if_not_exists(self, table_name: str, schema: Dict[str, str]) -> bool:
        """Create table with specified schema if it doesn't exist"""
        
        # Build CREATE TABLE statement
        column_definitions = []
        for column_name, column_type in schema.items():
            column_definitions.append(f"{column_name} {column_type}")
        
        # Add standard audit columns
        column_definitions.extend([
            "ingestion_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP",
            "data_classification VARCHAR(50) DEFAULT 'PUBLIC'",
            "created_at DATETIME DEFAULT CURRENT_TIMESTAMP",
            "updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
        ])
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(column_definitions)}
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH({list(schema.keys())[0]}) BUCKETS 10
        PROPERTIES (
            "replication_num" = "1",
            "storage_format" = "DEFAULT",
            "compression" = "LZ4"
        )
        """
        
        try:
            self.execute_query(create_sql)
            self.logger.info(f"Table {table_name} created or verified")
            return True
        except Exception as e:
            self.logger.error(f"Error creating table {table_name}: {str(e)}")
            return False
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get table metadata and statistics"""
        try:
            # Get table structure
            structure_query = f"DESCRIBE {table_name}"
            columns = self.execute_query(structure_query)
            
            # Get row count
            count_query = f"SELECT COUNT(*) as row_count FROM {table_name}"
            count_result = self.execute_query(count_query)
            row_count = count_result[0]['row_count'] if count_result else 0
            
            # Get data freshness
            freshness_query = f"""
            SELECT 
                MAX(ingestion_timestamp) as latest_ingestion,
                MIN(ingestion_timestamp) as earliest_ingestion
            FROM {table_name}
            """
            freshness_result = self.execute_query(freshness_query)
            freshness = freshness_result[0] if freshness_result else {}
            
            return {
                "table_name": table_name,
                "columns": columns,
                "row_count": row_count,
                "latest_ingestion": freshness.get('latest_ingestion'),
                "earliest_ingestion": freshness.get('earliest_ingestion'),
                "check_timestamp": datetime.now()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting table info for {table_name}: {str(e)}")
            return {"table_name": table_name, "error": str(e)}
    
    def cleanup_old_data(self, table_name: str, retention_days: int, 
                        date_column: str = 'ingestion_timestamp') -> int:
        """Clean up old data based on retention policy"""
        
        cleanup_query = f"""
        DELETE FROM {table_name}
        WHERE {date_column} < DATE_SUB(NOW(), INTERVAL {retention_days} DAY)
        """
        
        try:
            rows_deleted = self.execute_query(cleanup_query)
            self.logger.info(f"Cleaned up {rows_deleted} old records from {table_name}")
            return rows_deleted
        except Exception as e:
            self.logger.error(f"Error cleaning up {table_name}: {str(e)}")
            return 0
    
    def optimize_table(self, table_name: str) -> bool:
        """Optimize table performance"""
        try:
            # StarRocks table optimization
            optimize_query = f"OPTIMIZE TABLE {table_name}"
            self.execute_query(optimize_query)
            
            self.logger.info(f"Optimized table {table_name}")
            return True
        except Exception as e:
            self.logger.warning(f"Table optimization failed for {table_name}: {str(e)}")
            return False