"""
Connection Pool Manager.
Manages database connection pooling.
"""

from typing import Any, Dict, Optional, List
from threading import Lock
from queue import Queue, Empty
import time


class ConnectionPool:
    """
    Generic connection pool for database adapters.
    
    Usage:
        pool = ConnectionPool(
            create_func=lambda: psycopg2.connect(...),
            max_size=10
        )
        
        conn = pool.acquire()
        try:
            # use connection
        finally:
            pool.release(conn)
    """
    
    def __init__(
        self,
        create_func: callable,
        max_size: int = 5,
        min_size: int = 1,
        timeout: float = 30.0,
        validate_func: Optional[callable] = None,
        close_func: Optional[callable] = None
    ):
        """
        Initialize connection pool.
        
        Args:
            create_func: Function to create new connection
            max_size: Maximum pool size
            min_size: Minimum pool size (pre-created)
            timeout: Acquire timeout in seconds
            validate_func: Function to validate connection
            close_func: Function to close connection
        """
        self._create_func = create_func
        self._max_size = max_size
        self._min_size = min_size
        self._timeout = timeout
        self._validate_func = validate_func or (lambda c: True)
        self._close_func = close_func or (lambda c: c.close())
        
        self._pool: Queue = Queue(maxsize=max_size)
        self._size = 0
        self._lock = Lock()
        self._closed = False
        
        # Pre-create minimum connections
        self._initialize()
    
    def _initialize(self) -> None:
        """Pre-create minimum connections."""
        for _ in range(self._min_size):
            try:
                conn = self._create_func()
                self._pool.put(conn)
                self._size += 1
            except Exception:
                break
    
    def acquire(self, timeout: Optional[float] = None) -> Any:
        """
        Acquire connection from pool.
        
        Args:
            timeout: Override default timeout
            
        Returns:
            Database connection
            
        Raises:
            TimeoutError: If no connection available
            RuntimeError: If pool is closed
        """
        if self._closed:
            raise RuntimeError("Connection pool is closed")
        
        timeout = timeout or self._timeout
        
        # Try to get from pool
        try:
            conn = self._pool.get(timeout=0.1)
            if self._validate_func(conn):
                return conn
            else:
                # Connection invalid, close and create new
                self._safe_close(conn)
                with self._lock:
                    self._size -= 1
        except Empty:
            pass
        
        # Create new if under max
        with self._lock:
            if self._size < self._max_size:
                conn = self._create_func()
                self._size += 1
                return conn
        
        # Wait for available connection
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                conn = self._pool.get(timeout=0.5)
                if self._validate_func(conn):
                    return conn
                else:
                    self._safe_close(conn)
                    with self._lock:
                        self._size -= 1
            except Empty:
                continue
        
        raise TimeoutError(
            f"Could not acquire connection within {timeout} seconds"
        )
    
    def release(self, conn: Any) -> None:
        """
        Release connection back to pool.
        
        Args:
            conn: Connection to release
        """
        if self._closed:
            self._safe_close(conn)
            return
        
        if self._validate_func(conn):
            try:
                self._pool.put_nowait(conn)
            except:
                self._safe_close(conn)
                with self._lock:
                    self._size -= 1
        else:
            self._safe_close(conn)
            with self._lock:
                self._size -= 1
    
    def _safe_close(self, conn: Any) -> None:
        """Safely close connection."""
        try:
            self._close_func(conn)
        except Exception:
            pass
    
    def close(self) -> None:
        """Close all connections in pool."""
        self._closed = True
        
        while True:
            try:
                conn = self._pool.get_nowait()
                self._safe_close(conn)
            except Empty:
                break
        
        self._size = 0
    
    @property
    def size(self) -> int:
        """Current pool size."""
        return self._size
    
    @property
    def available(self) -> int:
        """Available connections in pool."""
        return self._pool.qsize()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()


class ConnectionManager:
    """
    Manages multiple connection pools.
    
    Usage:
        manager = ConnectionManager()
        manager.add_pool("primary", pool1)
        manager.add_pool("replica", pool2)
        
        conn = manager.acquire("primary")
    """
    
    def __init__(self):
        self._pools: Dict[str, ConnectionPool] = {}
        self._default: Optional[str] = None
    
    def add_pool(
        self, 
        name: str, 
        pool: ConnectionPool,
        default: bool = False
    ) -> None:
        """Add connection pool."""
        self._pools[name] = pool
        if default or self._default is None:
            self._default = name
    
    def get_pool(self, name: Optional[str] = None) -> ConnectionPool:
        """Get pool by name."""
        name = name or self._default
        if name not in self._pools:
            raise KeyError(f"Pool '{name}' not found")
        return self._pools[name]
    
    def acquire(self, name: Optional[str] = None) -> Any:
        """Acquire connection from named pool."""
        return self.get_pool(name).acquire()
    
    def release(self, conn: Any, name: Optional[str] = None) -> None:
        """Release connection to named pool."""
        self.get_pool(name).release(conn)
    
    def close_all(self) -> None:
        """Close all pools."""
        for pool in self._pools.values():
            pool.close()
        self._pools.clear()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close_all()
