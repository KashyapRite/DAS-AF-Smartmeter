class ValidationError(Exception):
    """Custom exception for validation errors."""
    def __init__(self, message):
        super().__init__(message)

class DatabaseError(Exception):
    """Custom exception for database errors."""
    def __init__(self, message):
        super().__init__(message)
