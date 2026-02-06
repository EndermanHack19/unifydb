from typing import Optional, Any


class OneDBError(Exception):
    
    def __init__(
        self, 
        message: str, 
        code: Optional[str] = None,
        details: Optional[dict] = None
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}
    
    def __str__(self) -> str:
        if self.code:
            return f"[{self.code}] {self.message}"
        return self.message
    
    def to_dict(self) -> dict:
        """Convert exception to dictionary."""
        return {
            "error": self.__class__.__name__,
            "message": self.message,
            "code": self.code,
            "details": self.details,
        }


class ConnectionError(OneDBError):
    """Failed to connect to database."""
    
    def __init__(
        self, 
        message: str, 
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        **kwargs
    ):
        details = {"host": host, "port": port, "database": database}
        super().__init__(message, code="CONN_ERR", details=details, **kwargs)


class QueryError(OneDBError):
    """Error executing query."""
    
    def __init__(
        self, 
        message: str, 
        query: Optional[str] = None,
        params: Optional[Any] = None,
        **kwargs
    ):
        details = {"query": query, "params": str(params) if params else None}
        super().__init__(message, code="QUERY_ERR", details=details, **kwargs)


class AdapterNotFoundError(OneDBError):
    """Requested adapter not found."""
    
    def __init__(self, adapter_name: str, **kwargs):
        message = f"Adapter '{adapter_name}' not found. Available: postgresql, mysql, mongodb, etc."
        super().__init__(message, code="ADAPTER_404", **kwargs)


class DriverNotInstalledError(OneDBError):
    """Database driver not installed."""
    
    def __init__(self, driver_name: str, install_command: str, **kwargs):
        message = (
            f"Driver '{driver_name}' is not installed. "
            f"Install with: {install_command}"
        )
        details = {"driver": driver_name, "install_command": install_command}
        super().__init__(message, code="DRIVER_404", details=details, **kwargs)


class ValidationError(OneDBError):
    """Data validation error."""
    
    def __init__(self, message: str, field: Optional[str] = None, **kwargs):
        details = {"field": field}
        super().__init__(message, code="VALID_ERR", details=details, **kwargs)


class TransactionError(OneDBError):
    """Transaction error."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, code="TX_ERR", **kwargs)
