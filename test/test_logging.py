import logging
import multiprocessing

log = multiprocessing.log_to_stderr()
log.setLevel(logging.INFO)


ANSI_TERM_RED = '\x1b[31m'
ANSI_TERM_YELLOW = '\x1b[33m'
ANSI_TERM_GREEN = '\x1b[32m'
ANSI_TERM_PINK = '\x1b[35m'
ANSI_TERM_NORMAL = '\x1b[0m'


def ensure_string(log_fun):
    def inner(msg):
        return log_fun(str(msg))
    return inner

@ensure_string
def log_run_test(msg):
    log.info(ANSI_TERM_YELLOW + msg + ANSI_TERM_NORMAL)

@ensure_string
def log_info(msg):
    log.info(msg)

@ensure_string
def log_error(msg):
    log.info(ANSI_TERM_RED + msg + ANSI_TERM_NORMAL)

@ensure_string
def log_success(msg):
    log.info(ANSI_TERM_GREEN + msg + ANSI_TERM_NORMAL)
