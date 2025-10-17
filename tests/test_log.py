from dao.dao_log import DaoLogger
import unittest




def test_log():
    logger = DaoLogger()
    logger.log("This is a test log message.")
    logger.warning("This is a test warning message.")
    logger.error("This is a test error message.")


