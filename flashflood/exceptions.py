class FlashFloodException(Exception):
    pass

class FlashFloodJournalingError(FlashFloodException):
    pass

class FlashFloodEventNotFound(FlashFloodException):
    pass

class FlashFloodJournalUploadError(FlashFloodException):
    pass
