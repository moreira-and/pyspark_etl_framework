from __future__ import annotations


class EtlError(Exception):
    """Base error for the ETL framework."""


class ExtractError(EtlError):
    """Raised when the extract stage fails."""


class CheckError(EtlError):
    """Raised when the check stage fails."""


class TransformError(EtlError):
    """Raised when the transform stage fails."""


class ValidateError(EtlError):
    """Raised when the validate stage fails."""


class LoadError(EtlError):
    """Raised when the load stage fails."""


class CertifyError(EtlError):
    """Raised when the certify stage fails."""
