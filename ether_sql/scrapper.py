from datetime import datetime
import logging
from web3.utils.encoding import to_int, to_hex
from ether_sql.models import (
    Blocks,
    Transactions,
    Uncles,
    Receipts,
    Logs,
    Traces,
    StateDiff,
)

logger = logging.getLogger(__name__)


def scrape_blocks(ether_sql_session, start_block_number, end_block_number):
    """
    Main function which starts scrapping data from the node and pushes it into
    the sql database

    :param int start_block_number: starting block number of scraping
    :param int end_block_number: end block number of scraping
    """

    logger.debug("Start block: {}".format(start_block_number))
    logger.debug('End block: {}'.format(end_block_number))

    for block_number in range(start_block_number, end_block_number+1):
        logger.debug('Adding block: {}'.format(block_number))

        ether_sql_session = add_block_number(
                                        block_number=block_number,
                                        ether_sql_session=ether_sql_session)

        logger.info("Commiting block: {} to sql".format(block_number))
        ether_sql_session.db_session.commit()


def add_block_number(block_number, ether_sql_session):
    """
    Adds the block, transactions, uncles, logs and traces of a given block
    number into the db_session

    :param int block_number: The block number to add to the database
    """
    # getting the block_data from the node
    block_data = ether_sql_session.w3.eth.getBlock(
                            block_identifier=block_number,
                            full_transactions=True)
    timestamp = to_int(block_data['timestamp'])
    iso_timestamp = datetime.utcfromtimestamp(timestamp).isoformat()
    block = Blocks.add_block(block_data=block_data,
                             iso_timestamp=iso_timestamp)
    # added the block data in the db session
    ether_sql_session.db_session.add(block)

    if ether_sql_session.settings.PARSE_TRACE and \
       ether_sql_session.settings.PARSE_STATE_DIFF:
        trace_list = ether_sql_session.w3.parity.\
                     traceReplayBlockTransactions(block_number,
                                                  mode=['trace', 'stateDiff'])
    else:
        if ether_sql_session.settings.PARSE_TRACE:
            trace_list = ether_sql_session.w3.parity.\
                         traceReplayBlockTransactions(block_number,
                                                      mode=['trace'])
        elif ether_sql_session.settings.PARSE_STATE_DIFF:
            trace_list = ether_sql_session.w3.parity.\
                         traceReplayBlockTransactions(block_number,
                                                      mode=['stateDiff'])
        else:
            trace_list = []

    transaction_list = block_data['transactions']
    # loop to get the transaction, receipts, logs and traces of the block
    for transaction_data, index in enumerate(transaction_list):
        transaction = Transactions.add_transaction(transaction_data,
                                                   block_number=block_number,
                                                   iso_timestamp=iso_timestamp)
        # added the transaction in the db session
        ether_sql_session.db_session.add(transaction)

        # adding receipt data
        receipt_data = ether_sql_session.w3.eth.getTransactionReceipt(
                                    transaction_data['hash'])
        receipt = Receipts.add_receipt(receipt_data,
                                       block_number=block_number,
                                       timestamp=iso_timestamp)
        ether_sql_session.db_session.add(receipt)

        logs_list = receipt_data['logs']
        for dict_log in logs_list:
            log = Logs.add_log(dict_log, block_number=block_number,
                               iso_timestamp=iso_timestamp)
            # adding the log in db session
            ether_sql_session.db_session.add(log)

        # adding traces
        if ether_sql_session.settings.PARSE_TRACE:
            dict_trace_list = trace_list[index]['trace']
            for dict_trace in dict_trace_list:
                trace = Traces.add_trace(dict_trace,
                                         block_number=block_number,
                                         timestamp=iso_timestamp)
            # added the trace in the db session
            ether_sql_session.db_session.add(trace)

        """
        # adding stateDiff
        if ether_sql_session.settings.PARSE_STATE_DIFF:
            dict_state_list = trace_list[index]['stateDiff']
            for dict_state in dict_state_list:
                stateDiff = StateDiff.add_stateDiff(dict_state,
                                                    transaction['hash'],
                                                    block_number,
                                                    iso_timestamp)
        """
    # getting uncle data
    uncle_list = block_data['uncles']
    for i in range(0, len(uncle_list)):
        # Unfortunately there is no command eth_getUncleByHash
        uncle_data = ether_sql_session.w3.eth.getUncleByBlock(
                                  block_number, i)
        uncle = Uncles.add_uncle(uncle_data=uncle_data,
                                 block_number=block_number,
                                 iso_timestamp=iso_timestamp)
        ether_sql_session.db_session.add(uncle)

    return ether_sql_session
