"""Mad3 exceptions."""


class M3FileNotFound(Exception):
    """A file is not found."""

    pass


class M3InputFileNotFound(M3FileNotFound):
    """An input file was not found."""

    pass


class M3FileChanged(Exception):
    """An file has changed."""

    pass


class M3InputFileChanged(M3FileChanged):
    """An file has changed."""

    pass


class M3OutputFileChanged(M3FileChanged):
    """An file has changed."""

    pass
