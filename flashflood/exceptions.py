class FlashFloodException(Exception):
    pass

class FlashFloodJournalingError(FlashFloodException):
    pass

class FlashFloodEventExistsError(FlashFloodException):
    pass

class FlashFloodEventNotFound(FlashFloodException):
    pass

class FlashFloodJournalUploadError(FlashFloodException):
    pass
